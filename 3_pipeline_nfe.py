#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIPELINE 3 - PROCESSAMENTO DE NOTAS FISCAIS (NFe)

Processa notas fiscais eletrônicas com matching ANVISA e aplicação da
regra temporal de ICMS 18%/20% para o estado da Paraíba.

REGRA TEMPORAL:
- Data emissão < 2024-01-01 → usa PF/PMVG 18%
- Data emissão >= 2024-01-01 → usa PF/PMVG 20%

IMPORTANTE: Certifique-se de que baseANVISA.csv existe em output/anvisa/
Execute ANTES: python 2_processar_base_anvisa.py
"""
import sys
import os
from pathlib import Path

# Root do projeto
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

print("=" * 80)
print("PIPELINE 3 - PROCESSAMENTO DE NOTAS FISCAIS (NFe)")
print("=" * 80)
print()
print("[INFO] Verificando base ANVISA...")

# Verificar se base ANVISA existe
BASE_ANVISA = PROJECT_ROOT / "output" / "anvisa" / "baseANVISA.csv"
if not BASE_ANVISA.exists():
    print()
    print("=" * 80)
    print("[ERRO] Base ANVISA não encontrada!")
    print()
    print("Execute primeiro:")
    print("  python 2_processar_base_anvisa.py")
    print("=" * 80)
    sys.exit(1)

print("[OK] Base ANVISA encontrada")
print()

try:
    # Importar e executar o pipeline NFe correto
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
except Exception as e:
    print()
    print("=" * 80)
    print(f"[ERRO] Falha no pipeline NFe: {e}")
    print("=" * 80)
    import traceback
    traceback.print_exc()
    sys.exit(1)
