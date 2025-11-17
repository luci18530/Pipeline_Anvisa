"""
Script de execução standalone para Etapa 12: Unificação e Matching Final
"""

import sys
from pathlib import Path

# Adiciona o diretório src ao path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / 'src'))

from nfe_etapa12_unificacao_matching import processar_unificacao_matching

if __name__ == '__main__':
    processar_unificacao_matching()
