#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Entrypoint dedicado ao Pipeline 1.0 (download e consolidação bruta)."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.anvisa_base.workflows.stage1_download_consolidacao import (
    main as run_stage1_download,
)


def run() -> None:
    """Executa apenas a etapa de download/consolidação."""
    run_stage1_download()


if __name__ == "__main__":
    run()
