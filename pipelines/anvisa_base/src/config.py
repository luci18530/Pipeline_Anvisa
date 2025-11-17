# -*- coding: utf-8 -*-
"""
Configurações gerais para o pipeline de processamento da Anvisa.
"""
import pandas as pd
import numpy as np

# ==============================================================================
#      CONFIGURAÇÕES DO PANDAS E NUMPY
# ==============================================================================

def configurar_pandas():
    """Configura as opções de exibição do pandas."""
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    np.set_printoptions(suppress=True, precision=0)

# ==============================================================================
#      CONSTANTES DO PIPELINE
# ==============================================================================

# Arquivos de entrada e saída
ARQUIVO_ENTRADA = 'data/processed/anvisa/base_anvisa_precos_vigencias.csv'
ARQUIVO_SAIDA = 'output/anvisa/baseANVISA.csv'

# Colunas para verificação de mudanças na unificação de vigências
COLUNAS_VERIFICACAO_MUDANCAS = [
    'PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%', 'ICMS 0%', 'CAP',
    'PRINCÍPIO ATIVO', 'LABORATÓRIO', 'PRODUTO', 'APRESENTAÇÃO',
    'CLASSE TERAPÊUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO'
]

# Colunas EAN para limpeza
COLUNAS_EAN = ['EAN 1', 'EAN 2', 'EAN 3']

# Mapeamento de grupos anatômicos
GRUPOS_ANATOMICOS = {
    'L': 'ANTINEOPLÁSICOS E IMUNOMODULADORES',
    'J': 'ANTI-INFECCIOSOS DE USO SISTÊMICO',
    'A': 'TRATO ALIMENTAR E METABOLISMO',
    'B': 'SANGUE E ÓRGÃOS HEMATOPOÉTICOS',
    'K': 'SOLUÇÕES INTRAVENOSAS',
    'C': 'SISTEMA CARDIOVASCULAR',
    'M': 'SISTEMA MÚSCULO-ESQUELÉTICO',
    'H': 'HORMÔNIOS SISTÊMICOS, EXCETO SEXUAIS E INSULINAS',
    'R': 'SISTEMA RESPIRATÓRIO',
    'D': 'DERMATOLÓGICOS',
    'G': 'SISTEMA GENITURINÁRIO E HORMÔNIOS SEXUAIS',
    'S': 'ÓRGÃOS SENSORIAIS',
    'P': 'ANTIPARASITÁRIOS'
}

# Códigos ATC específicos para sistema nervoso
CODIGOS_PSICO_NEUROLOGICOS = ['N03', 'N04', 'N05', 'N06', 'N07']
CODIGOS_ANESTESICOS_ANALGESICOS = ['N01', 'N02']