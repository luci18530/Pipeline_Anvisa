#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Entrypoint principal do pipeline ANVISA.

Execução padrão (fluida):
1. Pipeline 1.0 - Download e consolidação bruta
2. Pipeline 1.5 - Processamento e engenharia
3. Pipeline 2B - Processamento avançado
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
from pipelines.anvisa_base.src.processar_dados import (
    main as run_stage2b_processing,
)


def run(
    skip_download: bool = False,
    skip_stage15: bool = False,
    skip_advanced: bool = False,
    force_refresh: bool = False,
) -> None:
    """Executa o pipeline ANVISA completo ou parcial."""
    if not skip_download:
        print("=" * 80)
        print("EXECUTANDO PIPELINE 1.0: DOWNLOAD E CONSOLIDACAO")
        print("=" * 80)
        print()
        run_stage1_download(force_refresh=force_refresh)
        print()

    if not skip_stage15:
        print("=" * 80)
        print("EXECUTANDO PIPELINE 1.5: PROCESSAMENTO E ENGENHARIA")
        print("=" * 80)
        print()
        run_stage15_processing()
        print()

    if not skip_advanced:
        print("=" * 80)
        print("EXECUTANDO PIPELINE 2B: PROCESSAMENTO AVANCADO")
        print("=" * 80)
        print()
        run_stage2b_processing()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline ANVISA completo")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Pular etapa 1.0 (download/consolidacao)",
    )
    parser.add_argument(
        "--skip-stage15",
        action="store_true",
        help="Pular etapa 1.5 (processamento/engenharia)",
    )
    parser.add_argument(
        "--skip-advanced",
        action="store_true",
        help="Pular etapa 2B (processamento avancado)",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Limpar pasta raw antes de baixar novamente",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(
        skip_download=args.skip_download,
        skip_stage15=args.skip_stage15,
        skip_advanced=args.skip_advanced,
        force_refresh=args.force_refresh,
    )

