"""
Gestion de l'accès à DynamoDB pour l'API CityFlow.
"""

from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from typing import Any, Dict, Iterable, Iterator

import boto3
from boto3.dynamodb.conditions import Key

from processors.config import get_config


def _convert_decimal(value: Any) -> Any:
    """
    Convertit récursivement les objets Decimal de DynamoDB
    vers des types Python natifs (int, float).
    """
    if isinstance(value, list):
        return [_convert_decimal(item) for item in value]
    if isinstance(value, dict):
        return {k: _convert_decimal(v) for k, v in value.items()}
    if isinstance(value, Decimal):
        # Si la valeur est entière (ex: Decimal('3')), retourner un int
        if value % 1 == 0:
            return int(value)
        return float(value)
    return value


@lru_cache(maxsize=4)
def get_dynamodb_table(table_name: str):
    """
    Retourne l'objet Table DynamoDB demandé.
    Utilise un cache pour éviter de recréer la ressource.
    """
    cfg = get_config()
    resource = boto3.resource("dynamodb", region_name=cfg.aws_region)
    return resource.Table(table_name)


def fetch_metrics_for_date(date: str, *, metric_name: str | None = None) -> Iterable[Dict[str, Any]]:
    """
    Récupère les métriques pour une date donnée.
    Si metric_name est fourni, retourne uniquement cette métrique.
    """
    cfg = get_config()
    table = get_dynamodb_table(cfg.metrics_table)

    if metric_name:
        response = table.get_item(Key={"date": date, "metric_name": metric_name})
        item = response.get("Item")
        if item:
            yield _convert_decimal(item)
        return

    response = table.query(
        KeyConditionExpression=Key("date").eq(date),
    )
    items = response.get("Items", [])
    for item in items:
        yield _convert_decimal(item)

    # Gestion de la pagination (LastEvaluatedKey)
    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("date").eq(date),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        for item in response.get("Items", []):
            yield _convert_decimal(item)


def list_available_dates(limit: int = 25) -> Iterable[str]:
    """
    Retourne une liste de dates présentes dans la table (scan limité).
    Attention : opération potentiellement coûteuse sur de grosses tables.
    """
    cfg = get_config()
    table = get_dynamodb_table(cfg.metrics_table)
    seen: set[str] = set()
    response = table.scan(
        ProjectionExpression="#d",
        ExpressionAttributeNames={"#d": "date"},
        Limit=limit,
    )
    for item in response.get("Items", []):
        date_value = item.get("date")
        if date_value and date_value not in seen:
            seen.add(date_value)
            yield date_value


def fetch_correlations_for_date(date: str) -> Iterator[Dict[str, Any]]:
    """
    Récupère toutes les corrélations pour une date donnée.
    """
    cfg = get_config()
    table = get_dynamodb_table(cfg.correlations_table)
    response = table.query(KeyConditionExpression=Key("date").eq(date))
    for item in response.get("Items", []):
        yield _convert_decimal(item)

    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("date").eq(date),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        for item in response.get("Items", []):
            yield _convert_decimal(item)


def list_correlation_dates(limit: int = 25) -> Iterable[str]:
    """
    Liste des dates disponibles pour les corrélations.
    """
    cfg = get_config()
    table = get_dynamodb_table(cfg.correlations_table)
    seen: set[str] = set()
    response = table.scan(
        ProjectionExpression="#d",
        ExpressionAttributeNames={"#d": "date"},
        Limit=limit,
    )
    for item in response.get("Items", []):
        date_value = item.get("date")
        if date_value and date_value not in seen:
            seen.add(date_value)
            yield date_value


def fetch_reports_for_date(date: str, *, report_type: str | None = None) -> Iterator[Dict[str, Any]]:
    """
    Récupère les rapports pour une date donnée.
    Si report_type est fourni, retourne uniquement ce rapport.
    """
    cfg = get_config()
    table = get_dynamodb_table(cfg.reports_table)

    if report_type:
        response = table.get_item(Key={"date": date, "report_type": report_type})
        item = response.get("Item")
        if item:
            yield _convert_decimal(item)
        return

    response = table.query(
        KeyConditionExpression=Key("date").eq(date),
    )
    for item in response.get("Items", []):
        yield _convert_decimal(item)

    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("date").eq(date),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        for item in response.get("Items", []):
            yield _convert_decimal(item)


def list_report_dates(limit: int = 25) -> Iterable[str]:
    """
    Liste des dates disponibles pour les rapports.
    """
    cfg = get_config()
    table = get_dynamodb_table(cfg.reports_table)
    seen: set[str] = set()
    response = table.scan(
        ProjectionExpression="#d",
        ExpressionAttributeNames={"#d": "date"},
        Limit=limit,
    )
    for item in response.get("Items", []):
        date_value = item.get("date")
        if date_value and date_value not in seen:
            seen.add(date_value)
            yield date_value


