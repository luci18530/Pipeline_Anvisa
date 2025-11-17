#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para baixar e processar dados da ANVISA.

Este script deve ser executado da raiz do projeto:
    python scripts/baixar.py
    
ou simplesmente:
    python download.py
"""

import sys
import os

# Assegura acesso aos módulos da pipeline
SRC_DIR = os.path.join(os.path.dirname(__file__), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from scripts.baixar import main as _baixar_main


def run() -> None:
    """Executa o downloader oficial da base ANVISA."""
    _baixar_main()


if __name__ == "__main__":
    run()
