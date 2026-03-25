#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compat: executa processamento ANVISA sem download (1.5 + 2B)."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.anvisa_base.main import run


def main() -> None:
    run(skip_download=True, skip_stage15=False, skip_advanced=False, force_refresh=False)


if __name__ == "__main__":
    main()

