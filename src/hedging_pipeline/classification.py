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

        et = df[COL_EVENT_TYPE]
        ac = df[COL_ACTION]
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
