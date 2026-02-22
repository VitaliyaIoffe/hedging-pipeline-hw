"""
Event classification: assign a label that captures event type and action.
"""

from __future__ import annotations

from typing import Final

import pandas as pd

from hedging_pipeline.config import (
    ACTION_ADD,
    ACTION_DEL,
    COL_ACTION,
    COL_EVENT_TYPE,
    EVENT_TYPE_ADHOC,
    EVENT_TYPE_ANNUAL,
)
from hedging_pipeline.logging_config import logger

CLASS_LABEL_COL: Final[str] = "classification"


def _normalize_event_type(et: object) -> str:
    """Map raw event_type to canonical (adhoc | annual)."""
    if pd.isna(et):
        return ""
    s = str(et).strip().lower()
    if s == "annual":
        return EVENT_TYPE_ANNUAL
    if s == "adhoc":
        return EVENT_TYPE_ADHOC
    return s


def _normalize_action(a: object) -> str:
    """Map raw action to canonical add | del."""
    if pd.isna(a):
        return ""
    s = str(a).strip().lower()
    if s in (ACTION_ADD, "addition"):
        return ACTION_ADD
    if s in (ACTION_DEL, "delete", "deletion", "drop"):
        return ACTION_DEL
    return s


class EventClassifier:
    """Classifies events with a label combining event_type and action (e.g. adhoc_add, annual_del)."""

    def classify(self, events: pd.DataFrame) -> pd.DataFrame:
        """
        Add a classification column: '{event_type}_{action}'.
        Expects columns: event_type, action.
        """
        df = events.copy()
        if COL_EVENT_TYPE not in df.columns:
            raise ValueError(f"Events must have column '{COL_EVENT_TYPE}'")
        if COL_ACTION not in df.columns:
            raise ValueError(f"Events must have column '{COL_ACTION}'")

        et = df[COL_EVENT_TYPE].apply(_normalize_event_type)
        ac = df[COL_ACTION].apply(_normalize_action)
        df[CLASS_LABEL_COL] = et + "_" + ac

        unknown_et = ~et.isin([EVENT_TYPE_ADHOC, EVENT_TYPE_ANNUAL]) & df[COL_EVENT_TYPE].notna()
        unknown_ac = ~ac.isin([ACTION_ADD, ACTION_DEL]) & df[COL_ACTION].notna()
        if unknown_et.any() or unknown_ac.any():
            logger.warning(
                "Some events have unknown type or action: type %s, action %s",
                df.loc[unknown_et, COL_EVENT_TYPE].unique().tolist() if unknown_et.any() else [],
                df.loc[unknown_ac, COL_ACTION].unique().tolist() if unknown_ac.any() else [],
            )
        return df
