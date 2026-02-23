"""
Configure logging from a config file (e.g. logging.ini or logging.yaml).
Provides a single package logger for all hedging_pipeline modules.
"""

from __future__ import annotations

import logging
import logging.config
import sys
from pathlib import Path
from typing import Final

# One general logger for the whole package (used by loaders, classification, enrichment, etc.)
PACKAGE_LOGGER_NAME: Final[str] = "hedging_pipeline"
logger: Final[logging.Logger] = logging.getLogger(PACKAGE_LOGGER_NAME)


def setup_logging(
    config_path: Path | str | None = None,
    *,
    default_level: int = logging.INFO,
) -> None:
    """
    Load logging configuration from a file. If config_path is None, looks for
    logging.ini in the project root (parent of parent of this package when in src layout).
    Falls back to default level and console handler if file is missing or invalid.
    """
    if config_path is None:
        pkg_dir: Path = Path(__file__).resolve().parent
        candidate: Path = pkg_dir.parent.parent / "logging.ini"
        if not candidate.exists():
            candidate = pkg_dir.parent / "logging.ini"
        if not candidate.exists():
            candidate = Path.cwd() / "logging.ini"
        config_path = candidate

    path = Path(config_path)
    if not path.exists():
        logging.basicConfig(
            level=default_level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stdout,
        )
        return

    if path.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml

            with open(path) as config_file:
                config = yaml.safe_load(config_file)
            logging.config.dictConfig(config)
        except Exception:
            logging.basicConfig(
                level=default_level,
                format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                stream=sys.stdout,
            )
        return

    try:
        logging.config.fileConfig(
            path,
            disable_existing_loggers=False,
        )
    except Exception:
        logging.basicConfig(
            level=default_level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stdout,
        )
