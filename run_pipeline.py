#!/usr/bin/env python3
"""
Entry point: run the event processing pipeline (uses package and logging.ini).
"""

from hedging_pipeline.cli import main

if __name__ == "__main__":
    import sys

    sys.exit(main())
