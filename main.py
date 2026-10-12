#!/usr/bin/env python3
"""Entry point for the heatwave data pipeline.

Examples (from the project root)::

    python main.py init
    python main.py ingest-imd data/sample_imd_2025_05.csv
    python main.py simulate-aws --steps 48 --seed 7
    python main.py report --json-out logs/quality_report.json
    python main.py query --region "NORTHWEST INDIA" --start 2025-05-20 --end 2025-05-21
"""

import sys

from heatwave_pipeline.cli import main

if __name__ == "__main__":
    sys.exit(main())
