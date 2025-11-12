# -*- coding: utf-8 -*-
"""
Pipeline de Processamento de Dados ANVISA

Módulo principal para processamento de dados de medicamentos da ANVISA.
"""

__version__ = "2.0.0"
__author__ = "Data Processing Team"

from .config import configurar_pandas, ARQUIVO_ENTRADA, ARQUIVO_SAIDA

__all__ = ["configurar_pandas", "ARQUIVO_ENTRADA", "ARQUIVO_SAIDA"]
