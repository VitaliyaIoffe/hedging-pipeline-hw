"""Unit tests for logging configuration."""

from pathlib import Path

from hedging_pipeline.logging_config import setup_logging


def test_setup_logging_with_nonexistent_path() -> None:
    """Should not raise; falls back to basicConfig."""
    setup_logging(Path("/nonexistent/logging.ini"))


def test_setup_logging_with_ini(tmp_path: Path) -> None:
    ini = tmp_path / "logging.ini"
    ini.write_text("""
[loggers]
keys = root
[handlers]
keys = h
[formatters]
keys = f
[logger_root]
level = INFO
handlers = h
[handler_h]
class = logging.StreamHandler
level = INFO
formatter = f
args = ()
[formatter_f]
format = %(message)s
""")
    setup_logging(ini)
    # No exception means fileConfig succeeded
    import logging

    assert logging.getLogger().level == logging.INFO
