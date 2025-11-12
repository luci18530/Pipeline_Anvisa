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

# Adicionar src ao path para importar config se necessário
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from scripts.baixar import main

if __name__ == "__main__":
    main()
