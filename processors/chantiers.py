"""Processing steps for the chantier (public works) dataset."""

from __future__ import annotations

import json
from functools import partial
from pathlib import Path
from typing import List

import pandas as pd

from .base import PipelineResult, ProcessingContext, QualityReport, run_pipeline

COLUMNS_MAP = {
    "Référence Chantier": "chantier_id",
    "Code postal arrondissement - Commune": "code_commune",
    "Date début du chantier": "date_debut",
    "Date fin du chantier": "date_fin",
    "Surface (m2)": "surface_m2",
    "Synthèse - Nature du chantier": "nature",
    "Encombrement espace public": "encombrement",
    "Impact stationnement": "impact_stationnement",
    "geo_shape": "geo_shape",
    "geo_point_2d": "geo_point",
}

REQUIRED_COLUMNS = [
    "chantier_id",
    "date_debut",
    "date_fin",
    "geo_point",
]


def load_raw(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", dtype=str)


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=COLUMNS_MAP)


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    for column in ("date_debut", "date_fin"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    if "surface_m2" in frame.columns:
        frame["surface_m2"] = pd.to_numeric(frame["surface_m2"], errors="coerce")
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
        geometry_type = []
        for raw in frame["geo_shape"].fillna(""):
            try:
                geojson = json.loads(raw)
                geometry_type.append(geojson.get("type"))
            except json.JSONDecodeError:
                geometry_type.append(None)
        frame["geometry_type"] = geometry_type
    return frame


def _quality_required_columns(df: pd.DataFrame, report: QualityReport) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        report.add(f"error: missing expected columns {missing}")
    else:
        report.add("ok: required columns present")


def _quality_period(df: pd.DataFrame, report: QualityReport) -> None:
    if {"date_debut", "date_fin"}.issubset(df.columns):
        invalid = df.loc[df["date_fin"] < df["date_debut"]]
        if not invalid.empty:
            report.add("warning: some chantiers have date_fin < date_debut")


def _enrich_metadata(df: pd.DataFrame, context: ProcessingContext) -> pd.DataFrame:
    frame = df.copy()
    frame["source"] = context.source
    reference = None
    if context.input_path:
        date_folder = context.input_path.parent.name
        frame["ingestion_date"] = date_folder
        frame["source_file"] = context.input_path.name
        reference = pd.to_datetime(date_folder, errors="coerce")
    if {"date_debut", "date_fin"}.issubset(frame.columns):
        frame["duree_jours"] = (frame["date_fin"] - frame["date_debut"]).dt.days
        if reference is not None and not pd.isna(reference):
            frame["actif"] = (frame["date_debut"] <= reference) & (frame["date_fin"] >= reference)
        else:
            frame["actif"] = False
    return frame


def process(path: Path) -> PipelineResult:
    raw_df = load_raw(path)
    context = ProcessingContext(source="chantiers", input_path=path)
    return run_pipeline(
        df=raw_df,
        cleaning=(
            _rename_columns,
            _cast_types,
        ),
        quality_checks=(
            _quality_required_columns,
            _quality_period,
        ),
        enrichments=(
            partial(_enrich_metadata, context=context),
        ),
        context=context,
    )
