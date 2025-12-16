#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script principal para executar o pipeline de processamento ANVISA.

Pipeline dividido em 2 etapas:
1. Download e Consolidação Bruta (1_download_consolidacao.py)
2. Processamento e Engenharia (2_processamento_engenharia.py)

Para re-executar apenas o processamento (sem re-baixar):
    python main.py --skip-download

Para executar completo (download + processamento):
    python main.py
"""

import sys
import os
import argparse

# Adicionar scripts ao path
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)


def run(skip_download=False) -> None:
    """Executa o pipeline de processamento da base ANVISA."""
    
    if not skip_download:
        print("=" * 80)
        print("EXECUTANDO PIPELINE 1.0: DOWNLOAD E CONSOLIDAÇÃO")
        print("=" * 80)
        print()
        
        # Importar e executar download
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "download_consolidacao",
                os.path.join(SCRIPTS_DIR, "1_download_consolidacao.py")
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.main()
        except Exception as e:
            print(f"[ERRO] Falha no Pipeline 1.0: {e}")
            return
        
        print()
        print("=" * 80)
        print()
    
    print("=" * 80)
    print("EXECUTANDO PIPELINE 1.5: PROCESSAMENTO E ENGENHARIA")
    print("=" * 80)
    print()
    
    # Importar e executar processamento
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "processamento_engenharia",
            os.path.join(SCRIPTS_DIR, "2_processamento_engenharia.py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.main()
    except Exception as e:
        print(f"[ERRO] Falha no Pipeline 1.5: {e}")
        return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de processamento ANVISA")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Pular download e executar apenas processamento/engenharia"
    )
    
    args = parser.parse_args()
    run(skip_download=args.skip_download)
