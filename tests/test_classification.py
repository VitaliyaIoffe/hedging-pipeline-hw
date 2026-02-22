"""Unit tests for EventClassifier: parametrized and edge cases."""

import pandas as pd
import pytest

from hedging_pipeline.classification import CLASS_LABEL_COL, EventClassifier
from hedging_pipeline.config import COL_ACTION, COL_EVENT_TYPE

# ---------- Parametrized: event_type + action -> classification ----------


@pytest.mark.parametrize(
    "event_type,action,expected_label",
    [
        ("adhoc", "add", "adhoc_add"),
        ("adhoc", "del", "adhoc_del"),
        ("ANNUAL", "add", "annual_add"),
        ("ANNUAL", "del", "annual_del"),
        ("annual", "add", "annual_add"),
        ("Adhoc", "del", "adhoc_del"),
    ],
)
def test_classify_events_label_combinations(
    classifier: EventClassifier,
    event_type: str,
    action: str,
    expected_label: str,
) -> None:
    df = pd.DataFrame(
        {
            COL_EVENT_TYPE: [event_type],
            COL_ACTION: [action],
        }
    )
    out = classifier.classify(df)
    assert out[CLASS_LABEL_COL].iloc[0] == expected_label


@pytest.mark.parametrize("action_alias", ["addition", "delete", "deletion", "drop"])
def test_classify_events_action_aliases(classifier: EventClassifier, action_alias: str) -> None:
    # addition -> add, delete/deletion/drop -> del
    df = pd.DataFrame(
        {
            COL_EVENT_TYPE: ["adhoc"],
            COL_ACTION: [action_alias],
        }
    )
    out = classifier.classify(df)
    if action_alias == "addition":
        assert out[CLASS_LABEL_COL].iloc[0] == "adhoc_add"
    else:
        assert out[CLASS_LABEL_COL].iloc[0] == "adhoc_del"


# ---------- Edge cases ----------


def test_classify_events_adds_label(classifier: EventClassifier) -> None:
    df = pd.DataFrame(
        {
            COL_EVENT_TYPE: ["adhoc", "ANNUAL", "adhoc"],
            COL_ACTION: ["add", "del", "del"],
        }
    )
    out = classifier.classify(df)
    assert CLASS_LABEL_COL in out.columns
    assert list(out[CLASS_LABEL_COL]) == ["adhoc_add", "annual_del", "adhoc_del"]


def test_classify_events_requires_event_type_column(classifier: EventClassifier) -> None:
    with pytest.raises(ValueError) as exc_info:
        classifier.classify(pd.DataFrame({COL_ACTION: ["add"]}))
    assert COL_EVENT_TYPE in str(exc_info.value)


def test_classify_events_requires_action_column(classifier: EventClassifier) -> None:
    with pytest.raises(ValueError) as exc_info:
        classifier.classify(pd.DataFrame({COL_EVENT_TYPE: ["adhoc"]}))
    assert COL_ACTION in str(exc_info.value)


def test_classify_events_empty_dataframe(classifier: EventClassifier) -> None:
    df = pd.DataFrame(columns=[COL_EVENT_TYPE, COL_ACTION])
    out = classifier.classify(df)
    assert len(out) == 0
    assert CLASS_LABEL_COL in out.columns


def test_classify_events_preserves_other_columns(classifier: EventClassifier) -> None:
    df = pd.DataFrame(
        {
            COL_EVENT_TYPE: ["adhoc"],
            COL_ACTION: ["add"],
            "ticker": ["AAPL"],
        }
    )
    out = classifier.classify(df)
    assert "ticker" in out.columns
    assert out["ticker"].iloc[0] == "AAPL"
    assert out[CLASS_LABEL_COL].iloc[0] == "adhoc_add"


def test_classify_events_unknown_type_produces_label_still(classifier: EventClassifier) -> None:
    """Unknown event_type is lowercased and concatenated; no exception."""
    df = pd.DataFrame(
        {
            COL_EVENT_TYPE: ["unknown_type"],
            COL_ACTION: ["add"],
        }
    )
    out = classifier.classify(df)
    assert out[CLASS_LABEL_COL].iloc[0] == "unknown_type_add"


def test_classify_events_nan_type_produces_empty_prefix(classifier: EventClassifier) -> None:
    df = pd.DataFrame(
        {
            COL_EVENT_TYPE: [None],
            COL_ACTION: ["add"],
        }
    )
    out = classifier.classify(df)
    assert out[CLASS_LABEL_COL].iloc[0] == "_add"
