# -*- coding: utf-8 -*-
"""
CONFIGURAÇÃO DA BASE ANVISA (CMED)
===================================

Arquivo central de configuração para download e processamento da base de preços
de medicamentos da ANVISA.

IMPORTANTE: Edite este arquivo para alterar períodos de coleta e parâmetros.
"""

from datetime import datetime

# ==============================================================================
# PERÍODO DE COLETA DOS DADOS
# ==============================================================================

# Toggle: usar apenas o mês anterior ou coletar histórico completo
USAR_MES_ANTERIOR = False  # True = apenas mês anterior | False = desde ANO/MES_INICIO

# Data INICIAL do período (quando USAR_MES_ANTERIOR = False)
ANO_INICIO = 2020
MES_INICIO = 1

# Data FINAL do período (calculada dinamicamente como mês/ano atual)
# Não é necessário editar - será sempre o mês corrente
hoje = datetime.now()
ANO_FIM = hoje.year
MES_FIM = hoje.month

# ==============================================================================
# CONFIGURAÇÕES DE DOWNLOAD
# ==============================================================================

# URL base do site da ANVISA para download dos arquivos
URL_ANVISA = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/anos-anteriores/anos-anteriores"

# Número máximo de downloads simultâneos
MAX_DOWNLOAD_WORKERS = 6

# Número máximo de threads para limpeza de arquivos
MAX_CLEANING_THREADS = 8

# ==============================================================================
# CAMINHOS DOS ARQUIVOS
# ==============================================================================

# Pasta onde serão salvos os arquivos .zip baixados da ANVISA
PASTA_DOWNLOADS_BRUTOS = "data/raw"

# Pasta onde serão salvos os arquivos processados
PASTA_ARQUIVOS_LIMPOS = "data/processed"

# Arquivo consolidado temporário (durante o processamento)
ARQUIVO_CONSOLIDADO_TEMP = "data/processed/anvisa/anvisa_pmvg_consolidado_temp.csv"

# Arquivo final com vigências processadas
ARQUIVO_FINAL_VIGENCIAS = "data/processed/anvisa/base_anvisa_precos_vigencias.csv"

# ==============================================================================
# NOTAS DE USO
# ==============================================================================
"""
EXEMPLOS DE CONFIGURAÇÃO:

1. Coletar histórico completo desde 2020:
   USAR_MES_ANTERIOR = False
   ANO_INICIO = 2020
   MES_INICIO = 1

2. Coletar apenas últimos 2 anos:
   USAR_MES_ANTERIOR = False
   ANO_INICIO = 2023
   MES_INICIO = 1

3. Atualização incremental (apenas mês anterior):
   USAR_MES_ANTERIOR = True
   (ANO_INICIO e MES_INICIO serão ignorados)

4. Período específico (ex: 2022 até hoje):
   USAR_MES_ANTERIOR = False
   ANO_INICIO = 2022
   MES_INICIO = 1
"""
