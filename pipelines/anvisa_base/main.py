#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script principal para executar o pipeline de processamento ANVISA.

Este script deve ser executado da raiz do projeto:
    python main.py
"""

import sys
import os

# Adicionar src ao path para importar módulos da pipeline
SRC_DIR = os.path.join(os.path.dirname(__file__), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from processar_dados import main as _run_processamento


def run() -> None:
    """Executa o pipeline de processamento da base ANVISA."""
    _run_processamento()


if __name__ == "__main__":
    run()
