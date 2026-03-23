# -*- coding: utf-8 -*-
"""
CONFIGURAÇÃO DA BASE ANVISA (CMED)
==================================

Arquivo central de configuração para download e processamento da base de preços
de medicamentos da ANVISA.
"""

from datetime import datetime

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

# Data INICIAL do período (quando USAR_MES_ANTERIOR = False)
ANO_INICIO = 2020
MES_INICIO = 1

# Data FINAL do período (calculada dinamicamente como mês/ano atual)
hoje = datetime.now()
ANO_FIM = hoje.year
MES_FIM = hoje.month

# ==============================================================================
# CONFIGURAÇÕES DE DOWNLOAD
# ==============================================================================

URL_ANVISA = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/anos-anteriores/anos-anteriores"
MAX_DOWNLOAD_WORKERS = 6
MAX_CLEANING_THREADS = 8

# ==============================================================================
# CAMINHOS DOS ARQUIVOS
# ==============================================================================

PASTA_DOWNLOADS_BRUTOS = "data/raw"
PASTA_ARQUIVOS_LIMPOS = "data/processed"
ARQUIVO_CONSOLIDADO_TEMP = "data/processed/anvisa/anvisa_pmvg_consolidado_temp.csv"
ARQUIVO_FINAL_VIGENCIAS = "data/processed/anvisa/base_anvisa_precos_vigencias.csv"
