"""Processing steps dedicated to the Visual Crossing weather feed."""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import List

import pandas as pd

from .base import PipelineResult, ProcessingContext, QualityReport, run_pipeline

REQUIRED_COLUMNS: List[str] = [
    "datetime",
    "tempmax",
    "tempmin",
    "precip",
]

DATE_COLUMNS = [
    ("datetime", "%Y-%m-%d"),
    ("sunrise", "%H:%M:%S"),
    ("sunset", "%H:%M:%S"),
]


def load_raw(path: Path) -> pd.DataFrame:
    return pd.read_json(path, orient="records", lines=True)


def _flatten_days(df: pd.DataFrame) -> pd.DataFrame:
    if "days" not in df.columns:
        return df

    exploded = df.explode("days", ignore_index=True)
    if exploded.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    days = pd.json_normalize(exploded["days"])
    if "hours" in days.columns:
        days = days.drop(columns=["hours"])  # Hourly level handled in a dedicated pipeline if needed
    return days


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    for column, fmt in DATE_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], format=fmt, errors="coerce")
    numeric_columns = [col for col in frame.columns if col.startswith(("temp", "wind", "precip", "humidity", "pressure"))]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _quality_required_columns(df: pd.DataFrame, report: QualityReport) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        report.add(f"error: missing expected columns {missing}")
    else:
        report.add("ok: required columns present")


def _quality_temperature_range(df: pd.DataFrame, report: QualityReport) -> None:
    if {"tempmax", "tempmin"}.issubset(df.columns):
        invalid = df.loc[df["tempmax"] < df["tempmin"]]
        if not invalid.empty:
            report.add("warning: some rows have tempmax < tempmin")


def _enrich_metadata(df: pd.DataFrame, context: ProcessingContext) -> pd.DataFrame:
    frame = df.copy()
    if context.input_path:
        date_folder = context.input_path.parent.name
        frame["ingestion_date"] = date_folder
        frame["source_file"] = context.input_path.name
    frame["source"] = context.source
    return frame


def process(path: Path) -> PipelineResult:
    raw_df = load_raw(path)
    context = ProcessingContext(source="weather", input_path=path)
    return run_pipeline(
        df=raw_df,
        cleaning=(
            _flatten_days,
            _cast_types,
        ),
        quality_checks=(
            _quality_required_columns,
            _quality_temperature_range,
        ),
        enrichments=(
            partial(_enrich_metadata, context=context),
        ),
        context=context,
    )
