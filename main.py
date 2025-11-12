#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script principal para executar o pipeline de processamento ANVISA.

Este script deve ser executado da raiz do projeto:
    python main.py
"""

import sys
import os

# Adicionar src ao path para importar módulos
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from processar_dados import main

if __name__ == "__main__":
    main()
