"""Core utilities for data processing pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Protocol

import pandas as pd


class Step(Protocol):
    """Callable protocol for processing steps."""

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        ...


@dataclass(slots=True)
class QualityReport:
    """Represents the outcome of the quality checks for a dataset."""

    passed: bool
    messages: List[str]

    def add(self, message: str) -> None:
        self.messages.append(message)
        if message.lower().startswith("error"):
            self.passed = False


@dataclass(slots=True)
class ProcessingContext:
    """Metadata injected into each processing step."""

    source: str
    input_path: Optional[Path] = None
    extra: Dict[str, object] = None


@dataclass(slots=True)
class PipelineResult:
    """Final artefact produced by a pipeline."""

    dataframe: pd.DataFrame
    quality_report: QualityReport
    metadata: Dict[str, object]


def run_pipeline(
    *,
    df: pd.DataFrame,
    cleaning: Iterable[Step],
    quality_checks: Iterable[Callable[[pd.DataFrame, QualityReport], None]],
    enrichments: Iterable[Step],
    context: ProcessingContext,
) -> PipelineResult:
    """Execute processing steps in the canonical order.

    Parameters
    ----------
    df:
        Raw dataframe loaded from the raw landing zone.
    cleaning:
        Collection of functions dedicated to data cleaning (type casting,
        renaming, deduplication, etc.).
    quality_checks:
        Collection of functions performing validation. Each function receives
        the intermediate dataframe and the shared :class:`QualityReport` so it
        can append warnings or errors.
    enrichments:
        Collection of functions adding derived features or joining reference
        data.
    context:
        Metadata describing the current run (source name, input path, etc.).
    """

    frame = df.copy()

    for step in cleaning:
        frame = step(frame)

    quality = QualityReport(passed=True, messages=[])
    for check in quality_checks:
        check(frame, quality)

    for step in enrichments:
        frame = step(frame)

    metadata = {
        "source": context.source,
        "input_path": str(context.input_path) if context.input_path else None,
    }
    if context.extra:
        metadata.update(context.extra)

    return PipelineResult(frame, quality, metadata)
