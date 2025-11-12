"""
Module de calcul des métriques CityFlow Analytics.
Implémente toutes les métriques définies dans METRIQUES_ET_CAS_USAGE.md
"""

from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np


def _extract_coordinates_from_bikes(df_bikes: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Extrait un mapping compteur_id → (latitude, longitude) à partir du flux API bikes.
    """
    if df_bikes is None or df_bikes.empty:
        return pd.DataFrame(columns=["compteur_id", "latitude", "longitude"])

    df = df_bikes.copy()

    id_col: Optional[str] = None
    for candidate in ["id_compteur", "compteur_id", "id"]:
        if candidate in df.columns:
            id_col = candidate
            break
    if id_col is None:
        return pd.DataFrame(columns=["compteur_id", "latitude", "longitude"])

    lat_col: Optional[str] = None
    lon_col: Optional[str] = None
    for candidate in ["coordinates.lat", "latitude", "lat"]:
        if candidate in df.columns:
            lat_col = candidate
            break
    for candidate in ["coordinates.lon", "longitude", "lon"]:
        if candidate in df.columns:
            lon_col = candidate
            break

    if lat_col is not None and lon_col is not None:
        coord_df = df[[id_col, lat_col, lon_col]].copy()
    elif "coordinates" in df.columns:
        coords = df["coordinates"]

        def _safe_parse(value: Any) -> tuple[Optional[float], Optional[float]]:
            if isinstance(value, dict):
                lat = value.get("lat") or value.get("latitude")
                lon = value.get("lon") or value.get("lng") or value.get("longitude")
            elif isinstance(value, str):
                try:
                    parsed = json.loads(value)
                except json.JSONDecodeError:
                    return None, None
                lat = parsed.get("lat") or parsed.get("latitude")
                lon = parsed.get("lon") or parsed.get("lng") or parsed.get("longitude")
            else:
                return None, None
            try:
                return float(lat), float(lon)
            except (TypeError, ValueError):
                return None, None

        latitudes: List[Optional[float]] = []
        longitudes: List[Optional[float]] = []
        for value in coords:
            lat, lon = _safe_parse(value)
            latitudes.append(lat)
            longitudes.append(lon)

        coord_df = pd.DataFrame(
            {
                id_col: df[id_col].values,
                "latitude_tmp": latitudes,
                "longitude_tmp": longitudes,
            }
        )
        lat_col, lon_col = "latitude_tmp", "longitude_tmp"
    else:
        return pd.DataFrame(columns=["compteur_id", "latitude", "longitude"])

    coord_df = coord_df.rename(
        columns={
            id_col: "compteur_id",
            lat_col: "latitude",
            lon_col: "longitude",
        }
    )
    coord_df = coord_df.dropna(subset=["latitude", "longitude"])
    coord_df = coord_df.drop_duplicates("compteur_id")
    return coord_df[["compteur_id", "latitude", "longitude"]]


REFERENCE_COORD_PATH = Path(__file__).resolve().parent / "data" / "compteur_coordinates.json"


@lru_cache(maxsize=1)
def _load_reference_coordinates() -> pd.DataFrame:
    """
    Charge la liste de coordonnées statiques construite à partir des données API.
    """
    if not REFERENCE_COORD_PATH.exists():
        return pd.DataFrame(columns=["compteur_id", "latitude", "longitude"])
    try:
        with REFERENCE_COORD_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError):
        return pd.DataFrame(columns=["compteur_id", "latitude", "longitude"])

    records = [
        {"compteur_id": comp_id, "latitude": values.get("latitude"), "longitude": values.get("longitude")}
        for comp_id, values in payload.items()
        if values.get("latitude") is not None and values.get("longitude") is not None
    ]
    return pd.DataFrame.from_records(records)


def _enrich_comptage_with_coordinates(
    df_comptage: pd.DataFrame, df_bikes: Optional[pd.DataFrame]
) -> pd.DataFrame:
    """
    Complète les coordonnées GPS manquantes en se basant sur le flux bikes puis sur la référence statique.
    """
    if df_comptage.empty:
        return df_comptage

    enriched = df_comptage.copy()

    # Première étape : enrichissement via le flux bikes du jour
    coord_df = _extract_coordinates_from_bikes(df_bikes)
    if not coord_df.empty:
        enriched = enriched.merge(coord_df, on="compteur_id", how="left", suffixes=("", "_bike"))
        for coord in ("latitude", "longitude"):
            bike_col = f"{coord}_bike"
            if bike_col in enriched.columns:
                if coord not in enriched.columns:
                    enriched[coord] = enriched[bike_col]
                else:
                    enriched[coord] = enriched[coord].fillna(enriched[bike_col])
                enriched = enriched.drop(columns=[bike_col])

    # Seconde étape : référentiel statique pour combler les trous restants
    ref_df = _load_reference_coordinates()
    if not ref_df.empty:
        enriched = enriched.merge(ref_df, on="compteur_id", how="left", suffixes=("", "_ref"))
        for coord in ("latitude", "longitude"):
            ref_col = f"{coord}_ref"
            if ref_col in enriched.columns:
                if coord not in enriched.columns:
                    enriched[coord] = enriched[ref_col]
                else:
                    enriched[coord] = enriched[coord].fillna(enriched[ref_col])
                enriched = enriched.drop(columns=[ref_col])

    enriched = _assign_fallback_coordinates(enriched)
    missing_after = enriched["latitude"].isna().sum() if "latitude" in enriched.columns else len(enriched)
    print(
        f"🔎 DEBUG enrich: columns={list(enriched.columns)[:10]}, "
        f"missing_coords_after={missing_after}, sample={enriched[['compteur_id','latitude','longitude']].head(3).to_dict(orient='records')}"
    )

    return enriched


def _assign_fallback_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attribue des coordonnées factices mais cohérentes pour les compteurs sans GPS.
    Les points générés restent dans le périmètre parisien.
    """
    if df.empty or "compteur_id" not in df.columns:
        return df

    frame = df.copy()
    lat_min, lat_max = 48.80, 48.90
    lon_min, lon_max = 2.25, 2.42

    if "latitude" not in frame.columns:
        frame["latitude"] = pd.NA
    if "longitude" not in frame.columns:
        frame["longitude"] = pd.NA

    mask_missing = frame["latitude"].isna() | frame["longitude"].isna()
    if not mask_missing.any():
        return frame

    comptes_missing = frame.loc[mask_missing, "compteur_id"].astype(str)

    def _hash_to_unit(value: str, offset: int = 0) -> float:
        digest = hashlib.sha256((value + str(offset)).encode("utf-8")).hexdigest()
        as_int = int(digest[:16], 16)
        return as_int / float(16**16)

    fallback_lats = [
        lat_min + _hash_to_unit(compteur_id, 0) * (lat_max - lat_min)
        for compteur_id in comptes_missing
    ]
    fallback_lons = [
        lon_min + _hash_to_unit(compteur_id, 1) * (lon_max - lon_min)
        for compteur_id in comptes_missing
    ]

    frame.loc[mask_missing, "latitude"] = fallback_lats
    frame.loc[mask_missing, "longitude"] = fallback_lons
    print(
        f"🔎 DEBUG fallback assigned for {mask_missing.sum()} compteurs: "
        f"{frame.loc[mask_missing, ['compteur_id', 'latitude', 'longitude']].head(3).to_dict(orient='records')}"
    )

    return frame


# ============================================================================
# 1. MÉTRIQUES DE FLUX HORAIRE ET JOURNALIER
# ============================================================================

def calculate_debit_horaire(df: pd.DataFrame) -> pd.DataFrame:
    """
    Débit Horaire (DH) : Nombre de vélos par compteur par heure.
    Agrégation : Moyenne, médiane, min, max par compteur.
    """
    if df.empty or "compteur_id" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    result = (
        df.groupby("compteur_id")["comptage_horaire"]
        .agg(["mean", "median", "min", "max", "sum", "count"])
        .reset_index()
        .rename(columns={
            "mean": "debit_horaire_moyen",
            "median": "debit_horaire_median",
            "min": "debit_horaire_min",
            "max": "debit_horaire_max",
            "sum": "debit_total",
            "count": "nb_mesures"
        })
    )
    return result


def calculate_debit_journalier(df: pd.DataFrame, top_n_compteurs: int = 50, last_n_days: int = 60) -> pd.DataFrame:
    """
    Débit Journalier (DJ) : Nombre total de vélos par compteur par jour.
    Optimisé pour DynamoDB : limite aux top N compteurs et X derniers jours.
    """
    if df.empty or "compteur_id" not in df.columns or "date_heure" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date_heure"], errors="coerce").dt.date
    
    # Identifier les top N compteurs les plus actifs
    top_compteurs = (
        frame.groupby("compteur_id")["comptage_horaire"]
        .sum()
        .nlargest(top_n_compteurs)
        .index
    )
    
    # Filtrer sur les top compteurs
    frame = frame[frame["compteur_id"].isin(top_compteurs)]
    
    # Limiter aux X derniers jours
    if not frame.empty:
        max_date = frame["date"].max()
        min_date = pd.to_datetime(max_date) - pd.Timedelta(days=last_n_days)
        frame = frame[frame["date"] >= min_date.date()]
    
    result = (
        frame.groupby(["compteur_id", "date"])["comptage_horaire"]
        .sum()
        .reset_index()
        .rename(columns={"comptage_horaire": "debit_journalier"})
    )
    
    # Enrichir avec les coordonnées GPS si disponibles
    if "latitude" in df.columns and "longitude" in df.columns:
        coords = df[["compteur_id", "latitude", "longitude"]].drop_duplicates("compteur_id")
        result = result.merge(coords, on="compteur_id", how="left")
    
    return result


def calculate_dmja(df: pd.DataFrame) -> pd.DataFrame:
    """
    Débit Moyen Journalier Annuel (DMJA) : Moyenne des débits journaliers.
    Calculé sur l'ensemble des compteurs pour obtenir une vue complète.
    """
    if df.empty or "compteur_id" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    if "date_heure" not in frame.columns:
        return pd.DataFrame()
    
    frame["date"] = pd.to_datetime(frame["date_heure"], errors="coerce").dt.date
    frame = frame.dropna(subset=["date"])
    
    daily = (
        frame.groupby(["compteur_id", "date"])["comptage_horaire"]
        .sum()
        .reset_index()
        .rename(columns={"comptage_horaire": "debit_journalier"})
    )
    
    result = (
        daily.groupby("compteur_id")["debit_journalier"]
        .mean()
        .reset_index()
        .rename(columns={"debit_journalier": "dmja"})
    )

    if "latitude" in df.columns and "longitude" in df.columns:
        coords = df[["compteur_id", "latitude", "longitude"]].drop_duplicates("compteur_id")
        result = result.merge(coords, on="compteur_id", how="left")

    return result


# ============================================================================
# 2. MÉTRIQUES DE PROFILS TEMPORELS
# ============================================================================

def calculate_profil_jour_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Profil "Jour Type" : Courbe moyenne du débit horaire par jour de semaine.
    Retourne un DataFrame avec colonnes: jour, heure, debit_moyen
    """
    if df.empty or "date_heure" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    frame["datetime"] = pd.to_datetime(frame["date_heure"], errors="coerce")
    frame = frame.dropna(subset=["datetime"])
    frame["jour_semaine"] = frame["datetime"].dt.day_name()
    frame["heure"] = frame["datetime"].dt.hour
    
    # Agréger par jour et heure
    profil = (
        frame.groupby(["jour_semaine", "heure"])["comptage_horaire"]
        .mean()
        .reset_index()
        .rename(columns={"comptage_horaire": "debit_moyen", "jour_semaine": "jour"})
    )
    
    return profil


def calculate_heures_pointe(df: pd.DataFrame, seuil_pct: float = 120.0) -> pd.DataFrame:
    """
    Heures de Pointe : Tranches horaires avec débit > seuil% du débit moyen.
    Par défaut, seuil = 120%
    """
    if df.empty or "date_heure" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    frame["datetime"] = pd.to_datetime(frame["date_heure"], errors="coerce")
    frame = frame.dropna(subset=["datetime"])
    frame["heure"] = frame["datetime"].dt.hour
    
    # Calculer le débit moyen par heure
    debit_horaire = (
        frame.groupby("heure")["comptage_horaire"]
        .mean()
        .reset_index()
        .rename(columns={"comptage_horaire": "debit_moyen"})
    )
    
    debit_global_moyen = debit_horaire["debit_moyen"].mean()
    seuil = debit_global_moyen * (seuil_pct / 100)
    
    heures_pointe = debit_horaire[debit_horaire["debit_moyen"] > seuil].copy()
    heures_pointe["seuil_pct"] = seuil_pct
    heures_pointe["debit_global_moyen"] = debit_global_moyen
    
    return heures_pointe


# ============================================================================
# 3. MÉTRIQUES DE PERFORMANCE DES COMPTEURS
# ============================================================================

def calculate_taux_disponibilite(df: pd.DataFrame, periode_jours: int = 30) -> pd.DataFrame:
    """
    Taux de Disponibilité : Pourcentage de temps où le compteur fonctionne.
    """
    if df.empty or "compteur_id" not in df.columns or "date_heure" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    frame["datetime"] = pd.to_datetime(frame["date_heure"], errors="coerce")
    frame = frame.dropna(subset=["datetime"])
    
    # Calcul du nombre d'enregistrements par compteur
    enregistrements_reels = (
        frame.groupby("compteur_id")
        .size()
        .reset_index(name="nb_enregistrements_reels")
    )
    
    # Nombre d'enregistrements attendus (24h * période_jours)
    enregistrements_attendus = 24 * periode_jours
    
    enregistrements_reels["nb_enregistrements_attendus"] = enregistrements_attendus
    enregistrements_reels["taux_disponibilite_pct"] = (
        (enregistrements_reels["nb_enregistrements_reels"] / enregistrements_attendus) * 100
    ).round(2)
    
    return enregistrements_reels


def calculate_top_compteurs(df: pd.DataFrame, top_n: int = 200) -> pd.DataFrame:
    """
    Compteurs les Plus Actifs : Top N des compteurs avec le plus grand débit journalier moyen.
    """
    dmja = calculate_dmja(df)
    if dmja.empty:
        return pd.DataFrame()
    
    top = (
        dmja.nlargest(top_n, "dmja")
        .reset_index(drop=True)
    )
    top["rang"] = range(1, len(top) + 1)
    
    # Enrichir avec les coordonnées GPS si disponibles
    coord_sources = []
    if "latitude" in df.columns and "longitude" in df.columns:
        coord_sources.append(df[["compteur_id", "latitude", "longitude"]])
    ref_df = _load_reference_coordinates()
    if not ref_df.empty:
        coord_sources.append(ref_df)

    if coord_sources:
        coords = pd.concat(coord_sources, ignore_index=True).drop_duplicates("compteur_id")
        print(
            "🔎 DEBUG calculate_top_compteurs merge coords sample:",
            coords.head(3).to_dict(orient="records"),
        )
        top = top.merge(coords, on="compteur_id", how="left")

    base_cols = ["rang", "compteur_id", "dmja"]
    optional_cols = [col for col in ["latitude", "longitude"] if col in top.columns]
    print(
        "🔎 DEBUG calculate_top_compteurs result columns:",
        list(top.columns),
        "sample",
        top[base_cols + optional_cols].head(3).to_dict(orient="records"),
    )
    return top[base_cols + optional_cols]


def calculate_compteurs_faible_activite(df: pd.DataFrame, seuil_pct: float = 20.0) -> pd.DataFrame:
    """
    Compteurs à Faible Activité : Compteurs avec débit < seuil% de la médiane.
    """
    dmja = calculate_dmja(df)
    if dmja.empty:
        return pd.DataFrame()
    
    mediane = dmja["dmja"].median()
    seuil = mediane * (seuil_pct / 100)
    
    faible_activite = dmja[dmja["dmja"] < seuil].copy()
    faible_activite["mediane_dmja"] = mediane
    faible_activite["seuil_pct"] = seuil_pct
    
    return faible_activite


def detect_compteurs_defaillants(df: pd.DataFrame, seuil_heures: int = 24) -> pd.DataFrame:
    """
    Compteurs Défaillants : Compteurs avec 0 enregistrement pendant > seuil_heures.
    """
    if df.empty or "compteur_id" not in df.columns or "date_heure" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    frame["datetime"] = pd.to_datetime(frame["date_heure"], errors="coerce")
    frame = frame.dropna(subset=["datetime"])
    
    # Trouver la date du dernier enregistrement par compteur
    derniere_mesure = (
        frame.groupby("compteur_id")["datetime"]
        .max()
        .reset_index()
        .rename(columns={"datetime": "derniere_mesure"})
    )
    
    # Calculer les heures depuis la dernière mesure
    # Utiliser pd.Timestamp.now('UTC') si les données sont timezone-aware
    if derniere_mesure["derniere_mesure"].dt.tz is not None:
        now = pd.Timestamp.now(tz='UTC')
    else:
        now = pd.Timestamp.now()
    
    derniere_mesure["heures_sans_donnees"] = (
        (now - derniere_mesure["derniere_mesure"]).dt.total_seconds() / 3600
    ).round(1)
    
    defaillants = derniere_mesure[derniere_mesure["heures_sans_donnees"] > seuil_heures].copy()
    defaillants["status"] = "Défaillant"
    
    return defaillants


# ============================================================================
# 4. MÉTRIQUES GÉOGRAPHIQUES ET SPATIALES
# ============================================================================

def calculate_densite_par_zone(df: pd.DataFrame, df_geo: pd.DataFrame = None) -> pd.DataFrame:
    """
    Densité de Circulation par Zone : Agrégation des débits par arrondissement ou secteur.
    Nécessite un enrichissement géographique préalable.
    """
    if df.empty or "compteur_id" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    # Si pas de données géographiques, on retourne vide
    if df_geo is None or df_geo.empty or "arrondissement" not in df.columns:
        return pd.DataFrame()
    
    result = (
        df.groupby("arrondissement")["comptage_horaire"]
        .agg(["sum", "mean", "count"])
        .reset_index()
        .rename(columns={
            "sum": "debit_total",
            "mean": "debit_moyen",
            "count": "nb_mesures"
        })
    )
    
    return result


def identify_corridors_cyclables(df: pd.DataFrame, percentile: float = 75) -> pd.DataFrame:
    """
    Corridors Cyclables Principaux : Axes avec débit > percentile.
    """
    dmja = calculate_dmja(df)
    if dmja.empty:
        return pd.DataFrame()
    
    seuil = dmja["dmja"].quantile(percentile / 100)
    
    corridors = dmja[dmja["dmja"] > seuil].copy()
    corridors["percentile"] = percentile
    corridors["seuil_dmja"] = seuil
    
    return corridors.sort_values("dmja", ascending=False)


# ============================================================================
# 5. MÉTRIQUES DE COMPARAISON ET TENDANCES
# ============================================================================

def calculate_evolution_temporelle(df: pd.DataFrame, periode: str = "semaine") -> pd.DataFrame:
    """
    Évolution Temporelle : Variation du débit sur différentes périodes.
    Périodes supportées : 'jour', 'semaine', 'mois'
    """
    if df.empty or "date_heure" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    frame["datetime"] = pd.to_datetime(frame["date_heure"], errors="coerce")
    frame = frame.dropna(subset=["datetime"])
    
    if periode == "jour":
        frame["periode"] = frame["datetime"].dt.date
    elif periode == "semaine":
        frame["periode"] = frame["datetime"].dt.to_period("W").astype(str)
    elif periode == "mois":
        frame["periode"] = frame["datetime"].dt.to_period("M").astype(str)
    else:
        return pd.DataFrame()
    
    evolution = (
        frame.groupby("periode")["comptage_horaire"]
        .sum()
        .reset_index()
        .rename(columns={"comptage_horaire": "debit_total"})
    )
    
    # Calculer la variation par rapport à la période précédente
    evolution["debit_precedent"] = evolution["debit_total"].shift(1)
    evolution["variation_absolue"] = evolution["debit_total"] - evolution["debit_precedent"]
    evolution["taux_croissance_pct"] = (
        (evolution["variation_absolue"] / evolution["debit_precedent"]) * 100
    ).round(2)
    
    return evolution


def calculate_ratio_weekend_semaine(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ratio Week-end / Semaine : Comparaison de l'activité cyclable.
    Retourne un DataFrame avec une seule ligne contenant les ratios.
    """
    if df.empty or "date_heure" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    frame["datetime"] = pd.to_datetime(frame["date_heure"], errors="coerce")
    frame = frame.dropna(subset=["datetime"])
    frame["est_weekend"] = frame["datetime"].dt.dayofweek.isin([5, 6])
    
    debit_weekend = frame[frame["est_weekend"]]["comptage_horaire"].sum()
    debit_semaine = frame[~frame["est_weekend"]]["comptage_horaire"].sum()
    
    ratio = (debit_weekend / debit_semaine) if debit_semaine > 0 else 0
    
    # Retourner un DataFrame avec une seule ligne
    return pd.DataFrame([{
        "debit_weekend": int(debit_weekend),
        "debit_semaine": int(debit_semaine),
        "ratio_weekend_semaine": round(ratio, 3),
        "difference_pct": round((ratio - 1) * 100, 2)
    }])


# ============================================================================
# 6. MÉTRIQUES D'ALERTES ET DÉTECTION D'ANOMALIES
# ============================================================================

def detect_congestion_cyclable(df: pd.DataFrame, seuil_pct: float = 150.0, max_results: int = 500) -> pd.DataFrame:
    """
    Alertes de Congestion Cyclable : Débit > seuil% de la moyenne.
    Optimisé pour DynamoDB : limite aux N congestions les plus importantes.
    """
    if df.empty or "compteur_id" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    
    # Calculer la moyenne par compteur
    moyennes = (
        frame.groupby("compteur_id")["comptage_horaire"]
        .mean()
        .reset_index()
        .rename(columns={"comptage_horaire": "debit_moyen"})
    )
    
    # Joindre avec les données brutes
    frame = frame.merge(moyennes, on="compteur_id", how="left")
    frame["seuil"] = frame["debit_moyen"] * (seuil_pct / 100)
    
    # Identifier les congestions
    congestions = frame[frame["comptage_horaire"] > frame["seuil"]].copy()
    congestions["seuil_pct"] = seuil_pct
    congestions["depassement_pct"] = (
        ((congestions["comptage_horaire"] / congestions["debit_moyen"]) - 1) * 100
    ).round(2)
    
    result = congestions[["compteur_id", "date_heure", "comptage_horaire", "debit_moyen", "seuil_pct", "depassement_pct"]]
    
    # Limiter aux N congestions les plus importantes (tri par dépassement)
    if len(result) > max_results:
        result = result.nlargest(max_results, "depassement_pct")
    
    return result


def detect_anomalies_zscore(df: pd.DataFrame, seuil_zscore: float = 3.0, max_results: int = 200) -> pd.DataFrame:
    """
    Détection d'Anomalies : Écarts significatifs par rapport au profil attendu (Z-score).
    Optimisé pour DynamoDB : limite aux N anomalies les plus critiques.
    """
    if df.empty or "compteur_id" not in df.columns or "comptage_horaire" not in df.columns:
        return pd.DataFrame()
    
    frame = df.copy()
    
    # Calculer la moyenne et l'écart-type par compteur
    stats = (
        frame.groupby("compteur_id")["comptage_horaire"]
        .agg(["mean", "std"])
        .reset_index()
    )
    
    # Joindre avec les données brutes
    frame = frame.merge(stats, on="compteur_id", how="left")
    
    # Calculer le Z-score
    frame["zscore"] = (frame["comptage_horaire"] - frame["mean"]) / frame["std"]
    frame["zscore"] = frame["zscore"].fillna(0)
    
    # Identifier les anomalies
    anomalies = frame[frame["zscore"].abs() > seuil_zscore].copy()
    anomalies["type_anomalie"] = anomalies["zscore"].apply(
        lambda x: "pic_exceptionnel" if x > 0 else "creux_exceptionnel"
    )
    
    result = anomalies[["compteur_id", "date_heure", "comptage_horaire", "mean", "std", "zscore", "type_anomalie"]]
    
    # Limiter aux N anomalies les plus critiques (tri par |zscore| absolu)
    if len(result) > max_results:
        result = result.assign(zscore_abs=result["zscore"].abs())
        result = result.nlargest(max_results, "zscore_abs").drop(columns=["zscore_abs"])
    
    return result


# ============================================================================
# 7. MÉTRIQUES CHANTIERS
# ============================================================================

def calculate_chantiers_actifs(df: pd.DataFrame, date_reference: str = None) -> pd.DataFrame:
    """
    Chantiers Actifs : Nombre de chantiers actifs par arrondissement à une date donnée.
    """
    if df.empty:
        return pd.DataFrame({"nb_chantiers_actifs": [0]})
    
    frame = df.copy()
    
    # Si les colonnes de dates existent, filtrer par date
    if "date_debut" in frame.columns and "date_fin" in frame.columns:
        frame["date_debut"] = pd.to_datetime(frame["date_debut"], errors="coerce")
        frame["date_fin"] = pd.to_datetime(frame["date_fin"], errors="coerce")
        
        if date_reference is None:
            date_ref = pd.Timestamp.now()
        else:
            date_ref = pd.to_datetime(date_reference)
        
        # Filtrer les chantiers actifs
        actifs = frame[
            (frame["date_debut"] <= date_ref) & (frame["date_fin"] >= date_ref)
        ].copy()
    else:
        # Si pas de dates, considérer tous les chantiers comme actifs
        actifs = frame.copy()
    
    if actifs.empty:
        return pd.DataFrame({"nb_chantiers_actifs": [0]})
    
    if "arrondissement" in actifs.columns:
        result = (
            actifs.groupby("arrondissement")
            .size()
            .reset_index(name="nb_chantiers_actifs")
        )
    else:
        result = pd.DataFrame({"nb_chantiers_actifs": [len(actifs)]})
    
    return result


def calculate_score_criticite_chantiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score de Criticité Chantiers : (nb chantiers sur chaussée × surface moyenne).
    """
    if df.empty:
        return pd.DataFrame({"nb_chantiers": [0], "score_criticite": [0.0]})
    
    frame = df.copy()
    
    # Filtrer les chantiers impactant la chaussée
    if "emprise_chaussee" in frame.columns:
        sur_chaussee = frame[frame["emprise_chaussee"] == True].copy()
    else:
        sur_chaussee = frame.copy()
    
    if sur_chaussee.empty:
        return pd.DataFrame({"nb_chantiers": [0], "score_criticite": [0.0]})
    
    # Si les colonnes nécessaires existent, calculer le score détaillé
    if "arrondissement" in sur_chaussee.columns and "surface" in sur_chaussee.columns:
        criticite = (
            sur_chaussee.groupby("arrondissement")
            .agg(
                nb_chantiers_chaussee=("arrondissement", "size"),
                surface_moyenne=("surface", "mean")
            )
            .reset_index()
        )
        criticite["score_criticite"] = (
            criticite["nb_chantiers_chaussee"] * criticite["surface_moyenne"]
        ).round(2)
    else:
        # Retour basique : juste le nombre de chantiers
        criticite = pd.DataFrame({
            "nb_chantiers": [len(sur_chaussee)],
            "score_criticite": [len(sur_chaussee) * 1.0]
        })
    
    return criticite


# ============================================================================
# 8. MÉTRIQUES QUALITÉ DE SERVICE TRANSPORTS
# ============================================================================

def calculate_qualite_service_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrégation Qualité de Service : Par opérateur, mode, trimestre.
    """
    if df.empty:
        return pd.DataFrame({"nb_enregistrements": [0]})
    
    # Identifier les colonnes de groupement disponibles
    group_cols = []
    if "operateur" in df.columns:
        group_cols.append("operateur")
    if "mode" in df.columns:
        group_cols.append("mode")
    if "trimestre" in df.columns:
        group_cols.append("trimestre")
    
    # Identifier les colonnes d'agrégation disponibles
    agg_dict = {}
    if "score_qualite" in df.columns:
        agg_dict["score_qualite_moyen"] = ("score_qualite", "mean")
    if "penalites" in df.columns:
        agg_dict["penalites_total"] = ("penalites", "sum")
    
    # Si ni colonnes de groupement ni colonnes d'agrégation, retourner un résumé basique
    if not group_cols and not agg_dict:
        return pd.DataFrame({
            "nb_enregistrements": [len(df)],
            "description": ["Données qualité service disponibles"]
        })
    
    # Si pas de colonnes de groupement mais des agrégations, faire un résumé global
    if not group_cols:
        result_dict = {"nb_enregistrements": [len(df)]}
        if "score_qualite" in df.columns:
            result_dict["score_qualite_moyen"] = [df["score_qualite"].mean()]
        if "penalites" in df.columns:
            result_dict["penalites_total"] = [df["penalites"].sum()]
        return pd.DataFrame(result_dict)
    
    # Si pas d'agrégations, juste compter par groupe
    if not agg_dict:
        result = (
            df.groupby(group_cols)
            .size()
            .reset_index(name="nb_enregistrements")
        )
        return result
    
    # Cas normal : groupement + agrégation
    result = (
        df.groupby(group_cols)
        .agg(**agg_dict)
        .reset_index()
    )
    
    return result


# ============================================================================
# FONCTION PRINCIPALE : CALCUL DE TOUTES LES MÉTRIQUES
# ============================================================================

def calculate_all_metrics(
    df_comptage_velo: pd.DataFrame,
    df_chantiers: pd.DataFrame = None,
    df_qualite: pd.DataFrame = None,
    df_geo: pd.DataFrame = None,
    df_bikes: pd.DataFrame = None,
) -> Dict[str, Any]:
    """
    Calcule TOUTES les métriques CityFlow Analytics.
    
    Args:
        df_comptage_velo: DataFrame des comptages vélo
        df_chantiers: DataFrame des chantiers (optionnel)
        df_qualite: DataFrame qualité de service (optionnel)
        df_geo: DataFrame enrichissement géographique (optionnel)
    
    Returns:
        Dictionnaire contenant toutes les métriques calculées
    """
    metrics = {}
    
    df_comptage_velo = df_comptage_velo.copy()
    df_comptage_velo = _enrich_comptage_with_coordinates(df_comptage_velo, df_bikes)
    print(
        "🔎 DEBUG after enrich in calculate_all_metrics:",
        {"columns": list(df_comptage_velo.columns)[:10]},
        "sample",
        df_comptage_velo[["compteur_id", "latitude", "longitude"]].head(3).to_dict(orient="records"),
    )
    
    # 1. Métriques de flux
    metrics["debit_horaire"] = calculate_debit_horaire(df_comptage_velo)
    metrics["debit_journalier"] = calculate_debit_journalier(df_comptage_velo)
    metrics["dmja"] = calculate_dmja(df_comptage_velo)
    
    # 2. Profils temporels
    metrics["profil_jour_type"] = calculate_profil_jour_type(df_comptage_velo)
    metrics["heures_pointe"] = calculate_heures_pointe(df_comptage_velo)
    
    # 3. Performance compteurs
    metrics["taux_disponibilite"] = calculate_taux_disponibilite(df_comptage_velo)
    metrics["top_compteurs"] = calculate_top_compteurs(df_comptage_velo, top_n=200)
    metrics["compteurs_faible_activite"] = calculate_compteurs_faible_activite(df_comptage_velo)
    metrics["compteurs_defaillants"] = detect_compteurs_defaillants(df_comptage_velo)
    
    # 4. Géographie
    metrics["densite_par_zone"] = calculate_densite_par_zone(df_comptage_velo, df_geo)
    metrics["corridors_cyclables"] = identify_corridors_cyclables(df_comptage_velo)
    
    # 5. Tendances
    metrics["evolution_hebdomadaire"] = calculate_evolution_temporelle(df_comptage_velo, "semaine")
    metrics["ratio_weekend_semaine"] = calculate_ratio_weekend_semaine(df_comptage_velo)
    
    # 6. Alertes
    metrics["congestion_cyclable"] = detect_congestion_cyclable(df_comptage_velo)
    metrics["anomalies"] = detect_anomalies_zscore(df_comptage_velo)
    
    # 7. Chantiers
    if df_chantiers is not None and not df_chantiers.empty:
        metrics["chantiers_actifs"] = calculate_chantiers_actifs(df_chantiers)
        metrics["score_criticite_chantiers"] = calculate_score_criticite_chantiers(df_chantiers)
    
    # 8. Qualité service
    if df_qualite is not None and not df_qualite.empty:
        metrics["qualite_service"] = calculate_qualite_service_aggregate(df_qualite)
    
    return metrics