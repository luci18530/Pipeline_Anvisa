#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Compatibilidade: executa a parte de processamento da base ANVISA sem novo download.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.anvisa_base.main import run


print("=" * 80)
print("PIPELINE ANVISA - PROCESSAMENTO (SEM DOWNLOAD)")
print("=" * 80)
print()

try:
    run(skip_download=True, skip_stage15=False, skip_advanced=False, force_refresh=False)
    print()
    print("=" * 80)
    print("[OK] Processamento ANVISA concluido!")
    print("Proximo passo: python scripts/run_nfe_pipeline_completo.py")
    print("=" * 80)
except Exception as e:
    print(f"[ERRO] Falha no processamento: {e}")
    sys.exit(1)
