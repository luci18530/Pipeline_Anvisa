#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIPELINE 2B - PROCESSAMENTO E ENGENHARIA AVANÇADA DA BASE ANVISA

Executa o processamento detalhado dos dados ANVISA após a consolidação:
- Limpeza e padronização
- Unificação de vigências consecutivas
- Classificação terapêutica
- Processamento de princípios ativos
- Processamento de produtos
- Processamento de apresentações
- Processamento de dosagem
- Processamento de laboratório
- Processamento de grupo terapêutico
- Finalizações

IMPORTANTE: Execute APÓS ter rodado 2_processar_base_anvisa.py
"""
import sys
import os

# Adicionar scripts ao path
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "pipelines", "anvisa_base", "src")
sys.path.insert(0, SCRIPTS_DIR)

print("=" * 80)
print("PIPELINE 2B - PROCESSAMENTO E ENGENHARIA AVANÇADA DA BASE ANVISA")
print("=" * 80)
print()

try:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "processar_dados",
        os.path.join(SCRIPTS_DIR, "processar_dados.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    print()
    print("=" * 80)
    print("[OK] Base ANVISA processada com sucesso!")
    print("Arquivo gerado: output/anvisa/baseANVISA.csv")
    print()
    print("Execute agora: python 3_pipeline_nfe.py")
    print("=" * 80)
except Exception as e:
    print(f"[ERRO] Falha no processamento: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
