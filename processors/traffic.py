"""Processing steps dedicated to traffic disruptions feeds (IDFM/Navitia)."""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import List

import pandas as pd

from .base import PipelineResult, ProcessingContext, QualityReport, run_pipeline

REQUIRED_COLUMNS: List[str] = [
    "id",
    "status",
    "updated_at",
    "severity",
]

DATE_COLUMNS = ["updated_at"]


def load_raw(path: Path) -> pd.DataFrame:
    return pd.read_json(path, orient="records", lines=True)


def _flatten_disruptions(df: pd.DataFrame) -> pd.DataFrame:
    if "disruptions" not in df.columns:
        return df

    exploded = df.explode("disruptions", ignore_index=True)
    if exploded.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    records = pd.json_normalize(exploded["disruptions"])
    return records


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    for column in DATE_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def _quality_required_columns(df: pd.DataFrame, report: QualityReport) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        report.add(f"error: missing expected columns {missing}")
    else:
        report.add("ok: required columns present")


def _quality_active_status(df: pd.DataFrame, report: QualityReport) -> None:
    if "status" not in df.columns:
        return
    unknown = df.loc[~df["status"].isin(["active", "ended", "unknown", "ongoing"]), "status"].unique()
    if len(unknown) > 0:
        report.add(f"warning: unexpected statuses detected {unknown.tolist()}")


def _enrich_metadata(df: pd.DataFrame, context: ProcessingContext) -> pd.DataFrame:
    frame = df.copy()
    if context.input_path:
        date_folder = context.input_path.parent.name
        frame["ingestion_date"] = date_folder
        frame["source_file"] = context.input_path.name
    frame["source"] = context.source
    frame["is_active"] = frame.get("status", "").isin(["active", "ongoing"])
    return frame


def process(path: Path) -> PipelineResult:
    raw_df = load_raw(path)
    context = ProcessingContext(source="traffic", input_path=path)
    return run_pipeline(
        df=raw_df,
        cleaning=(
            _flatten_disruptions,
            _cast_types,
        ),
        quality_checks=(
            _quality_required_columns,
            _quality_active_status,
        ),
        enrichments=(
            partial(_enrich_metadata, context=context),
        ),
        context=context,
    )
