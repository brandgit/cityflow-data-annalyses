"""
Aggregation helpers combining the processed datasets.
Inclut les enrichissements géographiques et les corrélations entre sources.
"""

from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd
import numpy as np


def aggregate_velib_realtime(df: pd.DataFrame) -> pd.DataFrame:
    """Agrégation des données temps réel Vélib (API)."""
    
    if df.empty:
        return pd.DataFrame()
    
    # Colonnes attendues de l'API bikes
    if "ingestion_date" in df.columns:
        aggregated = (
            df.groupby("ingestion_date")
            .agg(
                nb_stations=("id_compteur", "nunique") if "id_compteur" in df.columns else ("id_site", "nunique"),
                compteur_total_moyen=("compteur_total", "mean") if "compteur_total" in df.columns else ("sum_counts", "mean"),
            )
            .reset_index()
            .rename(columns={"ingestion_date": "jour"})
        )
        return aggregated
    
    return pd.DataFrame()


def aggregate_comptage_velo(df: pd.DataFrame) -> pd.DataFrame:
    """Agrégation des comptages historiques de vélos (batch)."""

    required = {"compteur_id", "date_heure", "comptage_horaire"}
    if not required.issubset(df.columns):
        # Si les colonnes requises ne sont pas présentes, retourner un DataFrame vide
        return pd.DataFrame()

    # Créer une copie avec la colonne date convertie
    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date_heure"], errors="coerce")
    frame = frame.dropna(subset=["date"])
    
    # Extraire juste la date (sans l'heure) pour le groupby
    frame["jour"] = frame["date"].dt.date
    
    aggregated = (
        frame.groupby(["compteur_id", "jour"])
        .agg(
            comptage_total=("comptage_horaire", "sum"),
            comptage_moyen=("comptage_horaire", "mean"),
            comptage_max=("comptage_horaire", "max"),
        )
        .reset_index()
    )
    return aggregated


def aggregate_traffic_incidents(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise disruptions by severity and day."""

    if "updated_at" not in df.columns:
        raise ValueError("Traffic dataframe missing 'updated_at'")

    frame = df.copy()
    frame["jour"] = pd.to_datetime(frame["updated_at"], errors="coerce").dt.date
    
    # Gérer la colonne severity (peut ne pas exister)
    if "severity" in frame.columns:
        frame["gravite"] = frame["severity"].fillna("unknown")
    else:
        frame["gravite"] = "unknown"

    pivot = (
        frame.dropna(subset=["jour"])
        .groupby(["jour", "gravite"])
        .size()
        .unstack(fill_value=0)
        .add_prefix("incidents_")
        .reset_index()
    )
    incident_cols = [col for col in pivot.columns if col.startswith("incidents_")]
    pivot["nb_incidents_total"] = pivot[incident_cols].sum(axis=1)
    return pivot


def aggregate_weather_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Compute daily summaries of meteorological variables."""

    required = {"datetime", "tempmax", "tempmin", "precip"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"Weather dataframe missing columns: {missing}")

    aggregated = (
        df.assign(jour=pd.to_datetime(df["datetime"], errors="coerce"))
        .dropna(subset=["jour"])
        .groupby("jour")
        .agg(
            temperature_max=("tempmax", "mean"),
            temperature_min=("tempmin", "mean"),
            precipitation_mm=("precip", "sum"),
            vent_moyen=("windspeed", "mean"),
        )
        .reset_index()
    )
    return aggregated


def aggregate_comptage_troncon(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate bike counts per tronçon (site)."""

    required = {"site_id", "date", "comptage_horaire"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"Comptage dataframe missing columns: {missing}")

    aggregated = (
        df.groupby(["site_id", "date"])
        .agg(
            comptage_total=("comptage_horaire", "sum"),
            heures_mesurees=("comptage_horaire", "count"),
        )
        .reset_index()
    )
    return aggregated


def aggregate_validations(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate validations per day and per line."""

    required = {"date", "code_ligne", "nb_validations"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"Validations dataframe missing columns: {missing}")

    aggregated = (
        df.groupby(["date", "code_ligne"])
        .agg(nb_validations=("nb_validations", "sum"))
        .reset_index()
    )
    return aggregated


# ============================================================================
# ENRICHISSEMENTS GÉOGRAPHIQUES
# ============================================================================

def enrich_with_arrondissement(df: pd.DataFrame, df_geo: pd.DataFrame = None) -> pd.DataFrame:
    """
    Enrichit les données avec l'arrondissement basé sur les coordonnées géographiques.
    """
    if df.empty or df_geo is None or df_geo.empty:
        return df
    
    # TODO: Implémenter la jointure spatiale si les coordonnées sont disponibles
    # Pour l'instant, retourne les données telles quelles
    return df


def enrich_compteurs_with_troncons(df_comptage: pd.DataFrame, df_troncons: pd.DataFrame) -> pd.DataFrame:
    """
    Enrichit les compteurs avec les informations des tronçons géographiques.
    Attribution compteur → tronçon basée sur la distance minimale.
    """
    if df_comptage.empty or df_troncons.empty:
        return df_comptage
    
    # TODO: Implémenter l'attribution spatiale basée sur les coordonnées
    # Pour l'instant, retourne les données telles quelles
    return df_comptage


# ============================================================================
# CORRÉLATIONS ENTRE SOURCES
# ============================================================================

def correlate_chantiers_velo(
    df_comptage: pd.DataFrame,
    df_chantiers: pd.DataFrame
) -> pd.DataFrame:
    """
    Corrélation chantiers ↔ trafic vélo : impact spatio-temporel des chantiers.
    Analyse l'évolution du trafic vélo avant/pendant les chantiers.
    """
    if df_comptage.empty or df_chantiers.empty:
        return pd.DataFrame()
    
    # Préparer les données de comptage
    frame_comptage = df_comptage.copy()
    if "date_heure" not in frame_comptage.columns or "comptage_horaire" not in frame_comptage.columns:
        return pd.DataFrame()
    
    frame_comptage["date"] = pd.to_datetime(frame_comptage["date_heure"], errors="coerce").dt.date
    frame_comptage = frame_comptage.dropna(subset=["date"])
    
    # Agréger le trafic vélo par jour
    trafic_daily = (
        frame_comptage.groupby("date")["comptage_horaire"]
        .sum()
        .reset_index()
        .rename(columns={"comptage_horaire": "total_velos"})
    )
    
    # Préparer les données de chantiers
    frame_chantiers = df_chantiers.copy()
    if "date_debut" not in frame_chantiers.columns or "date_fin" not in frame_chantiers.columns:
        return pd.DataFrame()
    
    frame_chantiers["date_debut"] = pd.to_datetime(frame_chantiers["date_debut"], errors="coerce").dt.date
    frame_chantiers["date_fin"] = pd.to_datetime(frame_chantiers["date_fin"], errors="coerce").dt.date
    frame_chantiers = frame_chantiers.dropna(subset=["date_debut", "date_fin"])
    
    # Compter le nombre de chantiers actifs par jour
    dates = trafic_daily["date"].unique()
    chantiers_counts = []
    
    for date in dates:
        nb_chantiers = len(frame_chantiers[
            (frame_chantiers["date_debut"] <= date) & 
            (frame_chantiers["date_fin"] >= date)
        ])
        chantiers_counts.append({"date": date, "nb_chantiers_actifs": nb_chantiers})
    
    chantiers_daily = pd.DataFrame(chantiers_counts)
    
    # Joindre trafic vélo et chantiers
    correlation = trafic_daily.merge(chantiers_daily, on="date", how="inner")
    
    if not correlation.empty and len(correlation) > 1:
        # Calculer la corrélation
        corr_value = correlation[["total_velos", "nb_chantiers_actifs"]].corr().iloc[0, 1]
        correlation["correlation_chantiers_velo"] = corr_value
        
        # Calculer des statistiques
        correlation["moyenne_velos"] = correlation["total_velos"].mean()
        correlation["ecart_type_velos"] = correlation["total_velos"].std()
    
    return correlation


def correlate_qualite_validations(
    df_qualite: pd.DataFrame,
    df_validations: pd.DataFrame
) -> pd.DataFrame:
    """
    Corrélation qualité de service ↔ validations : analyse du report modal.
    Étudie si une baisse de qualité de service impacte le nombre de validations.
    """
    if df_qualite.empty or df_validations.empty:
        return pd.DataFrame()
    
    # Vérifier les colonnes nécessaires pour la qualité de service
    if "date" not in df_validations.columns or "nb_validations" not in df_validations.columns:
        return pd.DataFrame()
    
    # Agréger les validations par jour
    validations_daily = (
        df_validations.groupby("date")["nb_validations"]
        .sum()
        .reset_index()
        .rename(columns={"nb_validations": "total_validations"})
    )
    
    # Préparer les données de qualité
    frame_qualite = df_qualite.copy()
    
    # Essayer de convertir la date si elle existe
    date_col = None
    for col in ["date", "trimestre", "periode"]:
        if col in frame_qualite.columns:
            date_col = col
            break
    
    if date_col is None:
        return pd.DataFrame()
    
    # Agréger la qualité de service (si score disponible)
    if "score_qualite" in frame_qualite.columns or "ponctualite" in frame_qualite.columns:
        score_col = "score_qualite" if "score_qualite" in frame_qualite.columns else "ponctualite"
        
        qualite_agg = (
            frame_qualite.groupby(date_col)[score_col]
            .mean()
            .reset_index()
            .rename(columns={score_col: "score_moyen_qualite", date_col: "periode"})
        )
        
        # Pour une corrélation simple, on retourne les données agrégées
        # Une analyse plus poussée nécessiterait des données temporelles alignées
        return qualite_agg
    
    return pd.DataFrame()


def correlate_meteo_velo(
    df_weather: pd.DataFrame,
    df_comptage: pd.DataFrame
) -> pd.DataFrame:
    """
    Corrélation météo ↔ trafic vélo : impact des conditions météorologiques.
    """
    if df_weather.empty or df_comptage.empty:
        return pd.DataFrame()
    
    # Préparer les données météo
    frame_weather = df_weather.copy()
    if "datetime" in frame_weather.columns:
        frame_weather["date"] = pd.to_datetime(frame_weather["datetime"], errors="coerce").dt.date
    
    # Préparer les données de comptage
    frame_comptage = df_comptage.copy()
    if "date_heure" in frame_comptage.columns:
        frame_comptage["date"] = pd.to_datetime(frame_comptage["date_heure"], errors="coerce").dt.date
    
    # Agréger le trafic vélo par jour
    comptage_daily = (
        frame_comptage.groupby("date")["comptage_horaire"]
        .sum()
        .reset_index()
        .rename(columns={"comptage_horaire": "total_velos"})
    )
    
    # Agréger la météo par jour
    if "tempmax" in frame_weather.columns and "precip" in frame_weather.columns:
        weather_daily = (
            frame_weather.groupby("date")
            .agg(
                temperature_max=("tempmax", "mean"),
                precipitation=("precip", "sum")
            )
            .reset_index()
        )
        
        # Joindre les deux datasets
        correlation = comptage_daily.merge(weather_daily, on="date", how="inner")
        
        # Calculer les corrélations
        if not correlation.empty:
            corr_temp = correlation[["total_velos", "temperature_max"]].corr().iloc[0, 1]
            corr_precip = correlation[["total_velos", "precipitation"]].corr().iloc[0, 1]
            
            correlation["correlation_temperature"] = corr_temp
            correlation["correlation_precipitation"] = corr_precip
        
        return correlation
    
    return pd.DataFrame()


# ============================================================================
# BUILD KPIs COMBINÉS
# ============================================================================

def build_kpis(aggregates: dict) -> pd.DataFrame:
    """Combine daily KPIs into a single dataframe for reporting."""
    
    # Normaliser tous les agrégats pour avoir une colonne 'jour' de type date (pas datetime)
    normalized_aggregates = {}
    
    for key, df in aggregates.items():
        if df.empty or key == "daily_kpis":
            continue
        
        df_copy = df.copy()
        
        # Identifier la colonne de date
        date_col = None
        if "jour" in df_copy.columns:
            date_col = "jour"
        elif "date" in df_copy.columns:
            date_col = "date"
            df_copy = df_copy.rename(columns={"date": "jour"})
        
        if date_col:
            # Convertir en date Python simple (sans heure) pour homogénéité
            if pd.api.types.is_datetime64_any_dtype(df_copy["jour"]):
                df_copy["jour"] = pd.to_datetime(df_copy["jour"]).dt.date
            
            normalized_aggregates[key] = df_copy
    
    if not normalized_aggregates:
        return pd.DataFrame()
    
    # Démarrer avec le premier agrégat comme base
    first_key = list(normalized_aggregates.keys())[0]
    base_df = normalized_aggregates[first_key]
    
    # Fusionner tous les autres agrégats
    for key, df in normalized_aggregates.items():
        if key == first_key:
            continue
        
        base_df = base_df.merge(df, how="outer", on="jour", suffixes=("", f"_{key}"))
    
    return base_df
