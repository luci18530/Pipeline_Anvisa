#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIPELINE 2- PROCESSAMENTO E ENGENHARIA DA BASE ANVISA

Processa arquivos ANVISA consolidados e aplica engenharias:
- Processamento de vigências
- Cálculos de preços (PF 18%, PMVG 18%)
- Padronização de atributos
- Gera baseANVISA.csv final

IMPORTANTE: Execute APÓS ter rodado 1_download_anvisa.py
"""
import sys
import os

# Adicionar scripts ao path
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "pipelines", "anvisa_base", "scripts")
sys.path.insert(0, SCRIPTS_DIR)

print("=" * 80)
print("PIPELINE 1.5 - PROCESSAMENTO E ENGENHARIA DA BASE ANVISA")
print("=" * 80)
print()

try:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "processamento_engenharia",
        os.path.join(SCRIPTS_DIR, "2_processamento_engenharia.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    print()
    print("=" * 80)
    print("[OK] Base ANVISA processada com sucesso!")
    print("Arquivo gerado: output/anvisa/baseANVISA.csv")
    print()
    print("Execute agora: python 2b_processar_dados_anvisa.py")
    print("=" * 80)
except Exception as e:
    print(f"[ERRO] Falha no processamento: {e}")
    sys.exit(1)
