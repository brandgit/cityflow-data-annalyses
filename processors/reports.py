"""
Module de génération des rapports CityFlow Analytics.
Génère des rapports structurés au format JSON.
"""

from __future__ import annotations

from typing import Dict, List, Any
from datetime import datetime
import pandas as pd
import numpy as np


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convertit un DataFrame en liste de dictionnaires JSON-compatibles."""
    if df.empty:
        return []
    
    # Remplacer NaN/inf par None pour la sérialisation JSON
    df = df.replace([np.inf, -np.inf], np.nan)
    records = df.to_dict(orient="records")
    
    # Convertir les types numpy en types Python natifs
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, (np.integer, np.int64)):
                record[key] = int(value)
            elif isinstance(value, (np.floating, np.float64)):
                record[key] = float(value)
            elif isinstance(value, (pd.Timestamp, datetime)):
                record[key] = value.isoformat()
            elif hasattr(value, 'item'):  # numpy scalars
                record[key] = value.item()
    
    return records


def generate_resume_executif(
    df_comptage: pd.DataFrame,
    metrics: Dict[str, Any],
    date: str
) -> Dict[str, Any]:
    """
    Génère le résumé exécutif quotidien.
    
    Format:
    {
      "date": "2024-10-15",
      "total_passages": 125000,
      "compteurs_actifs": 95,
      "compteurs_defaillants": 4,
      "evolution_vs_hier": "+5.2%",
      "evolution_vs_semaine_derniere": "+12.3%"
    }
    """
    resume = {
        "date": date,
        "total_passages": 0,
        "compteurs_actifs": 0,
        "compteurs_defaillants": 0,
        "evolution_vs_hier": "N/A",
        "evolution_vs_semaine_derniere": "N/A"
    }
    
    # Total de passages
    if not df_comptage.empty and "comptage_horaire" in df_comptage.columns:
        resume["total_passages"] = int(df_comptage["comptage_horaire"].sum())
    
    # Nombre de compteurs actifs
    if not df_comptage.empty and "compteur_id" in df_comptage.columns:
        resume["compteurs_actifs"] = int(df_comptage["compteur_id"].nunique())
    
    # Compteurs défaillants
    if "compteurs_defaillants" in metrics and not metrics["compteurs_defaillants"].empty:
        resume["compteurs_defaillants"] = len(metrics["compteurs_defaillants"])
    
    # Évolution hebdomadaire
    if "evolution_hebdomadaire" in metrics and not metrics["evolution_hebdomadaire"].empty:
        evolution = metrics["evolution_hebdomadaire"]
        if not evolution.empty and "taux_croissance_pct" in evolution.columns:
            dernier_taux = evolution.iloc[-1]["taux_croissance_pct"]
            if pd.notna(dernier_taux):
                resume["evolution_vs_semaine_derniere"] = f"{dernier_taux:+.1f}%"
    
    return resume


def generate_top_compteurs_report(metrics: Dict[str, Any], limit: int = 10) -> Dict[str, Any]:
    """
    Génère le rapport des top compteurs.
    
    Format:
    {
      "titre": "Top 10 Compteurs les Plus Fréquentés",
      "date_generation": "2024-10-15T12:00:00",
      "compteurs": [
        {
          "rang": 1,
          "compteur_id": "100003098-101003098",
          "dmja": 8500,
          "evolution_pct": "+3.2%"
        },
        ...
      ]
    }
    """
    report = {
        "titre": f"Top {limit} Compteurs les Plus Fréquentés",
        "date_generation": datetime.now().isoformat(),
        "compteurs": []
    }
    
    if "top_compteurs" in metrics and not metrics["top_compteurs"].empty:
        top_df = metrics["top_compteurs"].head(limit)
        report["compteurs"] = _df_to_records(top_df)
    
    return report


def generate_zones_congestionnees_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Génère le rapport des zones congestionnées.
    
    Format:
    {
      "titre": "Zones les Plus Congestionnées",
      "date_generation": "2024-10-15T12:00:00",
      "zones": [
        {
          "compteur_id": "100003098-101003098",
          "date_heure": "2024-10-15T08:30:00",
          "debit": 520,
          "debit_moyen": 350,
          "depassement_pct": 150
        },
        ...
      ]
    }
    """
    report = {
        "titre": "Zones les Plus Congestionnées",
        "date_generation": datetime.now().isoformat(),
        "zones": []
    }
    
    if "congestion_cyclable" in metrics and not metrics["congestion_cyclable"].empty:
        congestions = metrics["congestion_cyclable"].nlargest(20, "depassement_pct")
        report["zones"] = _df_to_records(congestions)
    
    return report


def generate_compteurs_defaillants_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Génère le rapport des compteurs défaillants.
    
    Format:
    {
      "titre": "Compteurs Défaillants",
      "date_generation": "2024-10-15T12:00:00",
      "compteurs": [
        {
          "compteur_id": "100005432-101005432",
          "derniere_mesure": "2024-10-14T15:00:00",
          "heures_sans_donnees": 32.5,
          "status": "Défaillant"
        },
        ...
      ]
    }
    """
    report = {
        "titre": "Compteurs Défaillants",
        "date_generation": datetime.now().isoformat(),
        "compteurs": []
    }
    
    if "compteurs_defaillants" in metrics and not metrics["compteurs_defaillants"].empty:
        report["compteurs"] = _df_to_records(metrics["compteurs_defaillants"])
    
    return report


def generate_alertes_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Génère le rapport des alertes de la journée.
    
    Format:
    {
      "titre": "Alertes de la Journée",
      "date_generation": "2024-10-15T12:00:00",
      "alertes": [
        {
          "type": "pic_congestion",
          "compteur_id": "100003098-101003098",
          "date_heure": "2024-10-15T08:30:00",
          "debit": 520,
          "seuil_pct": 150,
          "message": "Pic de circulation détecté: 520 vélos/h (150% de la moyenne)"
        },
        {
          "type": "anomalie",
          "compteur_id": "100005432-101005432",
          "date_heure": "2024-10-15T14:00:00",
          "zscore": 4.2,
          "message": "Anomalie détectée: pic exceptionnel (Z-score: 4.2)"
        }
      ]
    }
    """
    report = {
        "titre": "Alertes de la Journée",
        "date_generation": datetime.now().isoformat(),
        "alertes": []
    }
    
    # Alertes de congestion
    if "congestion_cyclable" in metrics and not metrics["congestion_cyclable"].empty:
        congestions = metrics["congestion_cyclable"].head(10)
        for _, row in congestions.iterrows():
            alerte = {
                "type": "pic_congestion",
                "compteur_id": str(row["compteur_id"]),
                "date_heure": str(row["date_heure"]),
                "debit": float(row["comptage_horaire"]) if pd.notna(row["comptage_horaire"]) else None,
                "seuil_pct": float(row["seuil_pct"]) if pd.notna(row["seuil_pct"]) else None,
                "message": f"Pic de circulation détecté: {row['comptage_horaire']:.0f} vélos/h ({row['depassement_pct']:.1f}% au-dessus de la moyenne)"
            }
            report["alertes"].append(alerte)
    
    # Alertes d'anomalies
    if "anomalies" in metrics and not metrics["anomalies"].empty:
        anomalies = metrics["anomalies"].head(10)
        for _, row in anomalies.iterrows():
            alerte = {
                "type": "anomalie",
                "compteur_id": str(row["compteur_id"]),
                "date_heure": str(row["date_heure"]),
                "zscore": float(row["zscore"]) if pd.notna(row["zscore"]) else None,
                "type_anomalie": str(row["type_anomalie"]),
                "message": f"Anomalie détectée: {row['type_anomalie']} (Z-score: {row['zscore']:.2f})"
            }
            report["alertes"].append(alerte)
    
    # Alertes compteurs défaillants
    if "compteurs_defaillants" in metrics and not metrics["compteurs_defaillants"].empty:
        defaillants = metrics["compteurs_defaillants"]
        for _, row in defaillants.iterrows():
            alerte = {
                "type": "compteur_defaillant",
                "compteur_id": str(row["compteur_id"]),
                "heures_sans_donnees": float(row["heures_sans_donnees"]) if pd.notna(row["heures_sans_donnees"]) else None,
                "message": f"Compteur défaillant: {row['heures_sans_donnees']:.1f}h sans données"
            }
            report["alertes"].append(alerte)
    
    return report


def generate_profil_jour_type_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Génère le rapport des profils "jour type".
    
    Format:
    {
      "titre": "Profils Jour Type",
      "date_generation": "2024-10-15T12:00:00",
      "profils": {
        "Monday": [
          {"heure": 0, "debit_moyen": 15.2},
          {"heure": 1, "debit_moyen": 10.5},
          ...
        ],
        "Tuesday": [...],
        ...
      }
    }
    """
    report = {
        "titre": "Profils Jour Type",
        "date_generation": datetime.now().isoformat(),
        "profils": {}
    }
    
    if "profil_jour_type" in metrics:
        profil_df = metrics["profil_jour_type"]
        if isinstance(profil_df, pd.DataFrame) and not profil_df.empty:
            # Grouper par jour et convertir en dict
            for jour in profil_df["jour"].unique():
                jour_data = profil_df[profil_df["jour"] == jour]
                report["profils"][jour] = _df_to_records(jour_data)
    
    return report


def generate_chantiers_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Génère le rapport des chantiers actifs et zones critiques.
    
    Format:
    {
      "titre": "Analyse des Chantiers",
      "date_generation": "2024-10-15T12:00:00",
      "chantiers_actifs": [...],
      "zones_critiques": [...]
    }
    """
    report = {
        "titre": "Analyse des Chantiers",
        "date_generation": datetime.now().isoformat(),
        "chantiers_actifs": [],
        "zones_critiques": []
    }
    
    if "chantiers_actifs" in metrics and not metrics["chantiers_actifs"].empty:
        report["chantiers_actifs"] = _df_to_records(metrics["chantiers_actifs"])
    
    if "score_criticite_chantiers" in metrics and not metrics["score_criticite_chantiers"].empty:
        report["zones_critiques"] = _df_to_records(metrics["score_criticite_chantiers"])
    
    return report


def generate_tendances_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Génère le rapport des tendances et évolutions.
    
    Format:
    {
      "titre": "Tendances et Évolutions",
      "date_generation": "2024-10-15T12:00:00",
      "evolution_hebdomadaire": [...],
      "ratio_weekend_semaine": {...}
    }
    """
    report = {
        "titre": "Tendances et Évolutions",
        "date_generation": datetime.now().isoformat(),
        "evolution_hebdomadaire": [],
        "ratio_weekend_semaine": {}
    }
    
    if "evolution_hebdomadaire" in metrics and not metrics["evolution_hebdomadaire"].empty:
        report["evolution_hebdomadaire"] = _df_to_records(metrics["evolution_hebdomadaire"])
    
    if "ratio_weekend_semaine" in metrics:
        report["ratio_weekend_semaine"] = metrics["ratio_weekend_semaine"]
    
    return report


def generate_qualite_service_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Génère le rapport de qualité de service des transports.
    
    Format:
    {
      "titre": "Qualité de Service des Transports",
      "date_generation": "2024-10-15T12:00:00",
      "indicateurs": [...]
    }
    """
    report = {
        "titre": "Qualité de Service des Transports",
        "date_generation": datetime.now().isoformat(),
        "indicateurs": []
    }
    
    if "qualite_service" in metrics and not metrics["qualite_service"].empty:
        report["indicateurs"] = _df_to_records(metrics["qualite_service"])
    
    return report


def generate_rapport_complet(
    df_comptage: pd.DataFrame,
    metrics: Dict[str, Any],
    date: str
) -> Dict[str, Any]:
    """
    Génère le rapport quotidien complet au format JSON.
    
    Returns:
        Dictionnaire JSON contenant tous les rapports structurés.
    """
    rapport_complet = {
        "meta": {
            "titre": "Rapport Quotidien CityFlow Analytics",
            "date": date,
            "date_generation": datetime.now().isoformat(),
            "version": "2.0"
        },
        "resume_executif": generate_resume_executif(df_comptage, metrics, date),
        "top_compteurs": generate_top_compteurs_report(metrics),
        "zones_congestionnees": generate_zones_congestionnees_report(metrics),
        "compteurs_defaillants": generate_compteurs_defaillants_report(metrics),
        "alertes": generate_alertes_report(metrics),
        "profil_jour_type": generate_profil_jour_type_report(metrics),
        "chantiers": generate_chantiers_report(metrics),
        "tendances": generate_tendances_report(metrics),
        "qualite_service": generate_qualite_service_report(metrics)
    }
    
    return rapport_complet


def generate_metrics_summary(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Génère un résumé JSON de toutes les métriques calculées.
    
    Returns:
        Dictionnaire JSON avec statistiques clés de chaque métrique.
    """
    summary = {
        "meta": {
            "titre": "Résumé des Métriques CityFlow Analytics",
            "date_generation": datetime.now().isoformat()
        },
        "metriques_disponibles": []
    }
    
    for metric_name, metric_data in metrics.items():
        metric_info = {
            "nom": metric_name,
            "type": type(metric_data).__name__
        }
        
        if isinstance(metric_data, pd.DataFrame):
            metric_info["nb_lignes"] = len(metric_data)
            metric_info["colonnes"] = list(metric_data.columns)
        elif isinstance(metric_data, dict) and not isinstance(metric_data, pd.DataFrame):
            if all(isinstance(v, pd.DataFrame) for v in metric_data.values()):
                metric_info["nb_sous_metriques"] = len(metric_data)
                metric_info["sous_metriques"] = list(metric_data.keys())
            else:
                metric_info["contenu"] = metric_data
        
        summary["metriques_disponibles"].append(metric_info)
    
    return summary

