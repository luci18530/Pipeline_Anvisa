# -*- coding: utf-8 -*-
"""
CONFIGURAÇÃO DA BASE ANVISA (CMED)
==================================

Arquivo central de configuração para download e processamento da base de preços
de medicamentos da ANVISA.
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta

try:
    from pipeline_config import get_toggle
except Exception:  # pragma: no cover
    def get_toggle(*keys, default=None):  # type: ignore
        return default


# ==============================================================================
# PERÍODO DE COLETA DOS DADOS
# ==============================================================================

# Toggle: usar apenas o mês anterior ou coletar histórico completo.
# Prioriza o toggle central quando disponível; fallback para valor local.
USAR_MES_ANTERIOR = bool(get_toggle("anvisa", "usar_mes_anterior", default=False))

hoje = datetime.now()
if USAR_MES_ANTERIOR:
    # Janela curta: apenas mês anterior.
    referencia = hoje.replace(day=1) - relativedelta(months=1)
    ANO_INICIO = referencia.year
    MES_INICIO = referencia.month
    ANO_FIM = referencia.year
    MES_FIM = referencia.month
else:
    # Histórico completo até mês atual.
    ANO_INICIO = 2020
    MES_INICIO = 1
    ANO_FIM = hoje.year
    MES_FIM = hoje.month

# ==============================================================================
# CONFIGURAÇÕES DE DOWNLOAD
# ==============================================================================

URL_ANVISA = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/anos-anteriores/anos-anteriores"
MAX_DOWNLOAD_WORKERS = 12
MAX_CLEANING_THREADS = 8
DOWNLOAD_THREAD_MULTIPLIER = 2
DOWNLOAD_MAX_THREADS = 24
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1 MB
DOWNLOAD_RETRIES = 4
DOWNLOAD_BACKOFF_SECONDS = 1.0

# ==============================================================================
# CAMINHOS DOS ARQUIVOS
# ==============================================================================

PASTA_DOWNLOADS_BRUTOS = "data/raw"
PASTA_ARQUIVOS_LIMPOS = "data/processed"
ARQUIVO_CONSOLIDADO_TEMP = "data/processed/anvisa/anvisa_pmvg_consolidado_temp.csv"
ARQUIVO_FINAL_VIGENCIAS = "data/processed/anvisa/base_anvisa_precos_vigencias.csv"
