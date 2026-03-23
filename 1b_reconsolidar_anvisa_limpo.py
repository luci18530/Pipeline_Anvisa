#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SCRIPT DE COMPATIBILIDADE - RE-CONSOLIDAR ARQUIVOS ANVISA_LIMPO

Este wrapper foi mantido para compatibilidade.
O fluxo recomendado agora está unificado em:
  python 1_download_anvisa.py --modo reconsolidar

Próximo passo: python 2_processar_base_anvisa.py
"""
from __future__ import annotations

import sys
import os
import subprocess


PROJECT_ROOT = os.path.dirname(__file__)
PYTHON_EXE = sys.executable
SCRIPT_FASE1 = os.path.join(PROJECT_ROOT, "1_download_anvisa.py")


print("=" * 80)
print("RE-CONSOLIDAÇÃO (COMPATIBILIDADE)")
print("=" * 80)
print("[INFO] Redirecionando para: python 1_download_anvisa.py --modo reconsolidar")
print()

try:
    result = subprocess.run(
        [PYTHON_EXE, SCRIPT_FASE1, "--modo", "reconsolidar"],
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Execução retornou código {result.returncode}")

    print()
    print("=" * 80)
    print("[OK] Re-consolidação concluída (via fase 1 unificada)!")
    print("Execute agora: python 2_processar_base_anvisa.py")
    print("=" * 80)
except Exception as e:
    print(f"[ERRO] Falha na re-consolidação: {e}")
    sys.exit(1)
