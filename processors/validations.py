"""Processing steps for the daily transit validations dataset."""

from __future__ import annotations

from functools import partial
from pathlib import Path

import pandas as pd

from .base import PipelineResult, ProcessingContext, QualityReport, run_pipeline

COLUMNS_MAP = {
    "JOUR": "date",
    "CODE_STIF_TRNS": "code_transport",
    "CODE_STIF_RES": "code_reseau",
    "CODE_STIF_LIGNE": "code_ligne",
    "LIBELLE_LIGNE": "ligne_libelle",
    "ID_GROUPOFLIGNE": "groupe_ligne_id",
    "CATEGORIE_TITRE": "categorie_titre",
    "NB_VALD": "nb_validations",
}

REQUIRED_COLUMNS = ["date", "code_ligne", "nb_validations"]


def load_raw(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", dtype=str)


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=COLUMNS_MAP)


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    if "nb_validations" in frame.columns:
        frame["nb_validations"] = pd.to_numeric(frame["nb_validations"], errors="coerce")
    return frame


def _quality_required_columns(df: pd.DataFrame, report: QualityReport) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        report.add(f"error: missing expected columns {missing}")
    else:
        report.add("ok: required columns present")


def _quality_negative_values(df: pd.DataFrame, report: QualityReport) -> None:
    if "nb_validations" in df.columns and (df["nb_validations"] < 0).any():
        report.add("warning: negative validation counts detected")


def _enrich_metadata(df: pd.DataFrame, context: ProcessingContext) -> pd.DataFrame:
    frame = df.copy()
    frame["source"] = context.source
    if context.input_path:
        frame["ingestion_date"] = context.input_path.parent.name
        frame["source_file"] = context.input_path.name
    if "date" in frame.columns:
        frame["semaine"] = frame["date"].dt.isocalendar().week
    return frame


def process(path: Path) -> PipelineResult:
    raw_df = load_raw(path)
    context = ProcessingContext(source="validations", input_path=path)
    return run_pipeline(
        df=raw_df,
        cleaning=(
            _rename_columns,
            _cast_types,
        ),
        quality_checks=(
            _quality_required_columns,
            _quality_negative_values,
        ),
        enrichments=(
            partial(_enrich_metadata, context=context),
        ),
        context=context,
    )
