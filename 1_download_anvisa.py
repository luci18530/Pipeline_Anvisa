#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIPELINE 1.0 - DOWNLOAD E CONSOLIDAÇÃO BRUTA DA ANVISA

Faz download dos arquivos da ANVISA e gera um arquivo consolidado bruto.
Execute este script APENAS quando precisar baixar novos dados do site da ANVISA.

Para re-processar dados já baixados, use: 2_processar_base_anvisa.py
"""
import sys
import os

# Adicionar scripts ao path
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "pipelines", "anvisa_base", "scripts")
sys.path.insert(0, SCRIPTS_DIR)

print("=" * 80)
print("PIPELINE 1.0 - DOWNLOAD E CONSOLIDAÇÃO BRUTA DA ANVISA")
print("=" * 80)
print()

try:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "download_consolidacao",
        os.path.join(SCRIPTS_DIR, "1_download_consolidacao.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    print()
    print("=" * 80)
    print("[OK] Download concluído!")
    print("Execute agora: python 2_processar_base_anvisa.py")
    print("=" * 80)
except Exception as e:
    print(f"[ERRO] Falha no download: {e}")
    sys.exit(1)
