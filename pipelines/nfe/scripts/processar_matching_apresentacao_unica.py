"""
Script de execução standalone para Etapa 13: Matching de Apresentação Única
"""

import sys
from pathlib import Path

# Adiciona o diretório src ao path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / 'src'))

from nfe_etapa13_matching_apresentacao_unica import processar_matching_apresentacao_unica

if __name__ == '__main__':
    processar_matching_apresentacao_unica()
