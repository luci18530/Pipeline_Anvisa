#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Entrypoint principal do pipeline ANVISA.

Fluxo:
1. Pipeline 1.0 - Download e consolidação bruta
2. Pipeline 1.5 - Processamento e engenharia
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.anvisa_base.workflows.stage1_download_consolidacao import (
    main as run_stage1_download,
)
from pipelines.anvisa_base.workflows.stage15_processamento_engenharia import (
    main as run_stage15_processing,
)


def run(skip_download: bool = False) -> None:
    """Executa o pipeline ANVISA completo ou parcial."""
    if not skip_download:
        print("=" * 80)
        print("EXECUTANDO PIPELINE 1.0: DOWNLOAD E CONSOLIDAÇÃO")
        print("=" * 80)
        print()
        run_stage1_download()
        print()

    print("=" * 80)
    print("EXECUTANDO PIPELINE 1.5: PROCESSAMENTO E ENGENHARIA")
    print("=" * 80)
    print()
    run_stage15_processing()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline de processamento ANVISA")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Pular download e executar apenas processamento/engenharia",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(skip_download=args.skip_download)
