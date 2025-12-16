#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SCRIPT RÁPIDO - RE-CONSOLIDAR ARQUIVOS ANVISA_LIMPO

Re-consolida arquivos ANVISA_LIMPO_*.csv já existentes sem re-baixar.
Use quando quiser atualizar a base consolidada preservando PF 18% / PMVG 18%.

QUANDO USAR:
- Já tem os arquivos ANVISA_LIMPO em data/processed/
- Quer incluir PF 18% e PMVG 18% sem re-baixar tudo
- Quer ajustar seleção de colunas

PRÓXIMO PASSO: python 2_processar_base_anvisa.py
"""
import sys
import os

# Adicionar scripts ao path
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "pipelines", "anvisa_base", "scripts")
sys.path.insert(0, SCRIPTS_DIR)

print("=" * 80)
print("RE-CONSOLIDAÇÃO RÁPIDA - ARQUIVOS ANVISA_LIMPO")
print("=" * 80)
print()

try:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "reconsolidar",
        os.path.join(SCRIPTS_DIR, "reconsolidar_anvisa_limpo.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    print()
    print("=" * 80)
    print("[OK] Re-consolidação concluída!")
    print("Execute agora: python 2_processar_base_anvisa.py")
    print("=" * 80)
except Exception as e:
    print(f"[ERRO] Falha na re-consolidação: {e}")
    sys.exit(1)
