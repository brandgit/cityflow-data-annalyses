"""Processing steps for the SNCF/RATP quality of service dataset."""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import List

import pandas as pd

from .base import PipelineResult, ProcessingContext, QualityReport, run_pipeline

COLUMNS_MAP = {
    "OperatorName": "operateur",
    "Theme": "theme",
    "Indicateur": "indicateur",
    "TransportMode": "mode",
    "TransportSubmode": "sous_mode",
    "ID_Line": "id_ligne",
    "Name_Line": "nom_ligne",
    "Trimestre": "trimestre",
    "Annee": "annee",
    "ResultatEnPourcentage": "resultat_pct",
    "ResultatEnOccurrence": "resultat_occ",
    "Objectif référence contrat ": "objectif_pct",
    "Penalite": "penalite",
}

REQUIRED_COLUMNS = [
    "operateur",
    "theme",
    "indicateur",
    "trimestre",
    "annee",
    "resultat_pct",
]


def load_raw(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", dtype=str)


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=COLUMNS_MAP)


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "annee" in frame.columns:
        frame["annee"] = pd.to_numeric(frame["annee"], errors="coerce").astype("Int64")
    if "trimestre" in frame.columns:
        frame["trimestre_num"] = frame["trimestre"].str.extract(r"T(\d)").astype("Int64")
    for column in ("resultat_pct", "objectif_pct", "resultat_occ"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "penalite" in frame.columns:
        frame["penalite"] = frame["penalite"].str.upper().map({"OUI": True, "NON": False})
    return frame


def _quality_required_columns(df: pd.DataFrame, report: QualityReport) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        report.add(f"error: missing expected columns {missing}")
    else:
        report.add("ok: required columns present")


def _quality_scores(df: pd.DataFrame, report: QualityReport) -> None:
    if "resultat_pct" in df.columns:
        invalid = df.loc[(df["resultat_pct"] < 0) | (df["resultat_pct"] > 100)]
        if not invalid.empty:
            report.add("warning: some scores are outside the 0-100 range")


def _enrich_metadata(df: pd.DataFrame, context: ProcessingContext) -> pd.DataFrame:
    frame = df.copy()
    frame["source"] = context.source
    if context.input_path:
        frame["ingestion_date"] = context.input_path.parent.name
        frame["source_file"] = context.input_path.name
    if {"resultat_pct", "objectif_pct"}.issubset(frame.columns):
        frame["ecart_vs_objectif"] = frame["resultat_pct"] - frame["objectif_pct"]
    return frame


def process(path: Path) -> PipelineResult:
    raw_df = load_raw(path)
    context = ProcessingContext(source="qualite_service", input_path=path)
    return run_pipeline(
        df=raw_df,
        cleaning=(
            _rename_columns,
            _cast_types,
        ),
        quality_checks=(
            _quality_required_columns,
            _quality_scores,
        ),
        enrichments=(
            partial(_enrich_metadata, context=context),
        ),
        context=context,
    )
