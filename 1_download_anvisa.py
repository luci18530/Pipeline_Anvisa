#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIPELINE ANVISA - EXECUCAO UNICA

Este comando executa tudo que a base ANVISA precisa:
1) download e consolidacao bruta
2) processamento e engenharia
3) processamento avancado (gera baseANVISA.csv + dtypes)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.anvisa_base.main import run


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline ANVISA completo (execucao unica)")
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Limpar data/raw antes de baixar novamente",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Pular download e reutilizar base local",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    print("=" * 80)
    print("PIPELINE ANVISA - EXECUCAO UNICA")
    print("=" * 80)
    print("[INFO] Etapas: 1.0 + 1.5 + 2B")
    print()

    try:
        run(
            skip_download=args.skip_download,
            skip_stage15=False,
            skip_advanced=False,
            force_refresh=args.force_refresh,
        )
        print()
        print("=" * 80)
        print("[OK] Pipeline ANVISA concluido (base pronta para NFe)!")
        print("Proximo passo: python 3_pipeline_nfe.py")
        print("=" * 80)
    except Exception as exc:
        print(f"[ERRO] Falha no pipeline ANVISA: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()

