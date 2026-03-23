#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIPELINE 3 - PROCESSAMENTO DE NOTAS FISCAIS (NFe)

Processa notas fiscais eletrônicas com matching ANVISA e aplicação da
regra temporal de ICMS 18%/20% para o estado da Paraíba.
"""

import sys
from pathlib import Path

# Root do projeto
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

print("=" * 80)
print("PIPELINE 3 - PROCESSAMENTO DE NOTAS FISCAIS (NFe)")
print("=" * 80)
print()
print("[INFO] Verificando base ANVISA...")

# Verificar se base ANVISA e dtypes existem
BASE_ANVISA = PROJECT_ROOT / "output" / "anvisa" / "baseANVISA.csv"
DTYPES_ANVISA = PROJECT_ROOT / "output" / "anvisa" / "baseANVISA_dtypes.json"
if not BASE_ANVISA.exists() or not DTYPES_ANVISA.exists():
    print()
    print("=" * 80)
    print("[ERRO] Artefatos da base ANVISA não encontrados!")
    print(f"  - baseANVISA.csv: {'OK' if BASE_ANVISA.exists() else 'AUSENTE'}")
    print(f"  - baseANVISA_dtypes.json: {'OK' if DTYPES_ANVISA.exists() else 'AUSENTE'}")
    print()
    print("Execute primeiro:")
    print("  python 2_processar_base_anvisa.py")
    print("  python 2b_processar_dados_anvisa.py")
    print("=" * 80)
    sys.exit(1)

print("[OK] Base ANVISA e dtypes encontrados")
print()

try:
    from pipelines.nfe.main import run

    run()

    print()
    print("=" * 80)
    print("[OK] Pipeline NFe concluído com sucesso!")
    print()
    print("Outputs gerados:")
    print("  - data/processed/nfe_etapa07_matched.csv (com ICMS_ALIQUOTA_APLICADA)")
    print("  - QlikView/df_central.csv (output final)")
    print("=" * 80)
    sys.exit(0)
except Exception as exc:
    print()
    print("=" * 80)
    print(f"[ERRO] Falha no pipeline NFe: {exc}")
    print("=" * 80)
    import traceback

    traceback.print_exc()
    sys.exit(1)
