"""Processing steps for the road segments reference dataset."""

from __future__ import annotations

import json
from functools import partial
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from .base import PipelineResult, ProcessingContext, QualityReport, run_pipeline

COLUMNS_MAP = {
    "Identifiant arc": "troncon_id",
    "Date debut dispo data": "date_debut",
    "Date fin dispo data": "date_fin",
    "Libelle": "libelle",
    "Identifiant noeud aval": "noeud_aval_id",
    "Libelle noeud aval": "noeud_aval_libelle",
    "Identifiant noeud amont": "noeud_amont_id",
    "Libelle noeud amont": "noeud_amont_libelle",
    "geo_point_2d": "geo_point",
    "geo_shape": "geo_shape",
}

REQUIRED_COLUMNS = ["troncon_id", "libelle", "geo_shape"]


def load_raw(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", dtype=str)


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=COLUMNS_MAP)


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    for column in ("date_debut", "date_fin"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce", utc=True)
    if "troncon_id" in frame.columns:
        frame["troncon_id"] = pd.to_numeric(frame["troncon_id"], errors="coerce").astype("Int64")
    if "geo_point" in frame.columns:
        lats, lons = [], []
        for value in frame["geo_point"].fillna(""):
            parts = [p.strip() for p in value.split(",")]
            if len(parts) == 2:
                try:
                    lat_val = float(parts[0])
                    lon_val = float(parts[1])
                except ValueError:
                    lat_val = None
                    lon_val = None
            else:
                lat_val = None
                lon_val = None
            lats.append(lat_val)
            lons.append(lon_val)
        frame["latitude"] = lats
        frame["longitude"] = lons
    if "geo_shape" in frame.columns:
        geometry_type: List[str | None] = []
        approx_length: List[float | None] = []
        for raw in frame["geo_shape"].fillna(""):
            try:
                geojson = json.loads(raw)
            except json.JSONDecodeError:
                geometry_type.append(None)
                approx_length.append(None)
                continue
            geometry_type.append(geojson.get("type"))
            coords = geojson.get("coordinates", [])
            approx_length.append(_approximate_length(coords))
        frame["geometry_type"] = geometry_type
        frame["approx_length_km"] = approx_length
    return frame


def _approximate_length(coords: object) -> float | None:
    try:
        points: List[Tuple[float, float]] = []
        if isinstance(coords, list) and coords and isinstance(coords[0][0], (float, int)):
            points = [(float(x), float(y)) for x, y in coords]
        elif isinstance(coords, list):
            flattened = coords
            while flattened and isinstance(flattened[0], list) and not isinstance(flattened[0][0], (float, int)):
                flattened = flattened[0]
            points = [(float(x), float(y)) for x, y in flattened]
        else:
            return None
        if len(points) < 2:
            return 0.0
        total = 0.0
        for (x1, y1), (x2, y2) in zip(points[:-1], points[1:]):
            total += ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
        # Coordinates are in degrees; multiply by an approximate conversion to kilometres
        return total * 111
    except Exception:
        return None


def _quality_required_columns(df: pd.DataFrame, report: QualityReport) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        report.add(f"error: missing expected columns {missing}")
    else:
        report.add("ok: required columns present")


def _quality_geometry(df: pd.DataFrame, report: QualityReport) -> None:
    if "geometry_type" in df.columns and df["geometry_type"].isna().any():
        report.add("warning: some rows have invalid geometry")


def _enrich_metadata(df: pd.DataFrame, context: ProcessingContext) -> pd.DataFrame:
    frame = df.copy()
    frame["source"] = context.source
    if context.input_path:
        frame["ingestion_date"] = context.input_path.parent.name
        frame["source_file"] = context.input_path.name
    return frame


def process(path: Path) -> PipelineResult:
    raw_df = load_raw(path)
    context = ProcessingContext(source="referentiel_troncons", input_path=path)
    return run_pipeline(
        df=raw_df,
        cleaning=(
            _rename_columns,
            _cast_types,
        ),
        quality_checks=(
            _quality_required_columns,
            _quality_geometry,
        ),
        enrichments=(
            partial(_enrich_metadata, context=context),
        ),
        context=context,
    )
