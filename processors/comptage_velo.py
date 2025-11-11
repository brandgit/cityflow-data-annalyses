"""Processing steps for the hourly bike counting CSV."""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import List

import pandas as pd

from .base import PipelineResult, ProcessingContext, QualityReport, run_pipeline

COLUMNS_MAP = {
    "Identifiant du compteur": "compteur_id",
    "Nom du compteur": "compteur_nom",
    "Identifiant du site de comptage": "site_id",
    "Nom du site de comptage": "site_nom",
    "Comptage horaire": "comptage_horaire",
    "Date et heure de comptage": "date_heure",
    "Date d'installation du site de comptage": "date_installation",
    "Coordonnées géographiques": "coordonnees",
    "Identifiant technique compteur": "compteur_tech_id",
    "mois_annee_comptage": "mois_annee",
}

REQUIRED_COLUMNS = list(COLUMNS_MAP.values())


def load_raw(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", dtype=str)


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.rename(columns=COLUMNS_MAP)
    return frame


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "comptage_horaire" in frame.columns:
        frame["comptage_horaire"] = pd.to_numeric(frame["comptage_horaire"], errors="coerce")
    if "date_heure" in frame.columns:
        frame["date_heure"] = pd.to_datetime(frame["date_heure"], errors="coerce", utc=True)
        frame["date"] = frame["date_heure"].dt.tz_convert("UTC").dt.date
        frame["heure"] = frame["date_heure"].dt.tz_convert("UTC").dt.hour
    if "date_installation" in frame.columns:
        frame["date_installation"] = pd.to_datetime(frame["date_installation"], errors="coerce")
    if "coordonnees" in frame.columns:
        lats, lons = [], []
        for value in frame["coordonnees"].fillna(""):
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
    return frame


def _quality_required_columns(df: pd.DataFrame, report: QualityReport) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        report.add(f"error: missing expected columns {missing}")
    else:
        report.add("ok: required columns present")


def _quality_negative_counts(df: pd.DataFrame, report: QualityReport) -> None:
    if "comptage_horaire" in df.columns and (df["comptage_horaire"] < 0).any():
        report.add("warning: negative counts detected")


def _enrich_metadata(df: pd.DataFrame, context: ProcessingContext) -> pd.DataFrame:
    frame = df.copy()
    frame["source"] = context.source
    if context.input_path:
        frame["ingestion_date"] = context.input_path.parent.name
        frame["source_file"] = context.input_path.name
    return frame


def process(path: Path) -> PipelineResult:
    raw_df = load_raw(path)
    context = ProcessingContext(source="comptage_velo", input_path=path)
    return run_pipeline(
        df=raw_df,
        cleaning=(
            _rename_columns,
            _cast_types,
        ),
        quality_checks=(
            _quality_required_columns,
            _quality_negative_counts,
        ),
        enrichments=(
            partial(_enrich_metadata, context=context),
        ),
        context=context,
    )
