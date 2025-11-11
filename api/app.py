"""
Application FastAPI pour exposer les métriques CityFlow Analytics.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query

from processors.config import get_config

from .db import (
    fetch_correlations_for_date,
    fetch_metrics_for_date,
    fetch_reports_for_date,
    list_available_dates,
    list_correlation_dates,
    list_report_dates,
)

app = FastAPI(
    title="CityFlow Analytics API",
    description="API pour exposer les métriques CityFlow stockées dans DynamoDB.",
    version="1.0.0",
)


def get_settings() -> Dict[str, Any]:
    """
    Dépendance FastAPI pour exposer la configuration utile.
    """
    cfg = get_config()
    return {
        "environment": cfg.environment,
        "aws_region": cfg.aws_region,
        "metrics_table": cfg.metrics_table,
    }


@app.get("/health", summary="État de l'API")
def health(settings: Dict[str, Any] = Depends(get_settings)) -> Dict[str, Any]:
    """
    Vérifie l'état de santé de l'API.
    """
    return {
        "status": "ok",
        "environment": settings["environment"],
        "metrics_table": settings["metrics_table"],
        "aws_region": settings["aws_region"],
    }


@app.get(
    "/metrics",
    summary="Liste les dates disponibles (métriques)",
    response_description="Dates pour lesquelles des métriques sont disponibles.",
)
def get_metric_dates(limit: int = Query(25, ge=1, le=200)) -> Dict[str, List[str]]:
    """
    Retourne une liste (non exhaustive) de dates disponibles dans DynamoDB.
    """
    dates = list(list_available_dates(limit=limit))
    return {"dates": dates}


@app.get(
    "/metrics/names",
    summary="Liste tous les noms de métriques disponibles",
    response_description="Liste des 18 métriques CityFlow Analytics.",
)
def get_metric_names() -> Dict[str, List[str]]:
    """
    Retourne la liste exhaustive des métriques calculées par CityFlow Analytics.
    """
    metric_names = [
        "debit_horaire",
        "debit_journalier",
        "dmja",
        "profil_jour_type",
        "heures_pointe",
        "taux_disponibilite",
        "top_compteurs",
        "compteurs_faible_activite",
        "compteurs_defaillants",
        "densite_par_zone",
        "corridors_cyclables",
        "evolution_hebdomadaire",
        "ratio_weekend_semaine",
        "congestion_cyclable",
        "anomalies",
        "chantiers_actifs",
        "score_criticite_chantiers",
        "qualite_service",
    ]
    return {"metric_names": metric_names}


@app.get(
    "/metrics/{date}",
    summary="Récupère toutes les métriques pour une date",
    response_description="Liste complète des métriques et leurs données pour la date fournie.",
)
def get_all_metrics_for_date(date: str) -> Dict[str, Any]:
    """
    Retourne TOUTES les métriques stockées pour une date donnée.
    """
    metrics = list(fetch_metrics_for_date(date))

    if not metrics:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune métrique trouvée pour la date {date}.",
        )

    return {
        "date": date,
        "metrics_count": len(metrics),
        "metrics": metrics,
    }


@app.get(
    "/metrics/{date}/{metric_name}",
    summary="Récupère UNE métrique spécifique pour une date",
    response_description="Données de la métrique demandée.",
)
def get_single_metric(date: str, metric_name: str) -> Dict[str, Any]:
    """
    Retourne une métrique précise pour une date donnée.
    
    Exemples:
    - /metrics/2025-11-11/debit_journalier
    - /metrics/2025-11-11/congestion_cyclable
    - /metrics/2025-11-11/anomalies
    """
    metrics = list(fetch_metrics_for_date(date, metric_name=metric_name))

    if not metrics:
        raise HTTPException(
            status_code=404,
            detail=f"Métrique '{metric_name}' introuvable pour la date {date}.",
        )

    return {
        "date": date,
        "metric_name": metric_name,
        "data": metrics[0].get("data", {}),
        "timestamp": metrics[0].get("timestamp"),
    }


@app.get(
    "/correlations",
    summary="Liste les dates disponibles (corrélations)",
    response_description="Dates pour lesquelles des corrélations sont disponibles.",
)
def get_correlation_dates(limit: int = Query(25, ge=1, le=200)) -> Dict[str, List[str]]:
    """
    Retourne une liste (non exhaustive) de dates disponibles pour les corrélations.
    """
    dates = list(list_correlation_dates(limit=limit))
    return {"dates": dates}


@app.get(
    "/correlations/{date}",
    summary="Récupère les corrélations pour une date donnée",
    response_description="Liste des corrélations et leurs données pour la date fournie.",
)
def get_correlations_for_date(date: str) -> Dict[str, Any]:
    """
    Retourne les corrélations stockées pour une date donnée.
    """
    correlations = list(fetch_correlations_for_date(date))

    if not correlations:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune corrélation trouvée pour la date {date}.",
        )

    return {
        "date": date,
        "correlations_count": len(correlations),
        "correlations": correlations,
    }


@app.get(
    "/reports",
    summary="Liste les dates disponibles (rapports)",
    response_description="Dates pour lesquelles des rapports sont disponibles.",
)
def get_report_dates(limit: int = Query(25, ge=1, le=200)) -> Dict[str, List[str]]:
    """
    Retourne une liste (non exhaustive) de dates disponibles pour les rapports.
    """
    dates = list(list_report_dates(limit=limit))
    return {"dates": dates}


@app.get(
    "/reports/{date}",
    summary="Récupère les rapports pour une date donnée",
    response_description="Rapports (processing_report, metrics_summary, rapport_quotidien).",
)
def get_reports_for_date(
    date: str,
    report_type: Optional[str] = Query(
        None, description="Type: processing_report, metrics_summary, rapport_quotidien."
    ),
) -> Dict[str, Any]:
    """
    Retourne les rapports stockés pour une date donnée.
    Possibilité de filtrer par report_type pour un seul rapport.
    """
    reports = list(fetch_reports_for_date(date, report_type=report_type))

    if report_type:
        if not reports:
            raise HTTPException(
                status_code=404,
                detail=f"Rapport '{report_type}' introuvable pour la date {date}.",
            )
        return {
            "date": date,
            "report_type": report_type,
            "payload": reports[0],
        }

    if not reports:
        raise HTTPException(
            status_code=404,
            detail=f"Aucun rapport trouvé pour la date {date}.",
        )

    return {
        "date": date,
        "reports_count": len(reports),
        "reports": reports,
    }


