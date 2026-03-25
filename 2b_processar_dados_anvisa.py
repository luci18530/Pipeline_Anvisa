#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Compatibilidade: executa apenas a etapa 2B (processamento avançado) da base ANVISA.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.anvisa_base.main import run


print("=" * 80)
print("PIPELINE ANVISA - ETAPA 2B (AVANCADO)")
print("=" * 80)
print()

try:
    run(skip_download=True, skip_stage15=True, skip_advanced=False, force_refresh=False)
    print()
    print("=" * 80)
    print("[OK] Etapa 2B concluida!")
    print("Proximo passo: python 3_pipeline_nfe.py")
    print("=" * 80)
except Exception as e:
    print(f"[ERRO] Falha no processamento avancado: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

