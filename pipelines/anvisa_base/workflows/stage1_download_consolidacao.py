# -*- coding: utf-8 -*-
"""
Pipeline 1.0 - Download e Consolidação Bruta
Faz download dos arquivos da ANVISA e gera um arquivo consolidado bruto.
NÃO aplica engenharias ou seleção de colunas.
"""
import os
import shutil
from datetime import datetime
import logging

from pipelines.anvisa_base.config_anvisa import (
    ANO_INICIO,
    MES_INICIO,
    ANO_FIM,
    MES_FIM,
    PASTA_DOWNLOADS_BRUTOS,
    PASTA_ARQUIVOS_LIMPOS,
    ARQUIVO_CONSOLIDADO_TEMP
)

from pipelines.anvisa_base.workflows.baixar import (
    scrape_anvisa_links,
    download_files,
    clean_downloaded_files,
    consolidate_cleaned_files,
)

def main(force_refresh: bool = False):
    """Pipeline 1.0: Download e Consolidação Bruta"""
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
    logging.info(f"Período de coleta: {MES_INICIO:02d}/{ANO_INICIO} até {MES_FIM:02d}/{ANO_FIM}")
    logging.info("=" * 80)
    logging.info("PIPELINE 1.0 - DOWNLOAD E CONSOLIDAÇÃO BRUTA DA ANVISA")
    logging.info("=" * 80)
    
    # 1. Limpeza inicial opcional (refresh completo)
    if force_refresh and os.path.exists(PASTA_DOWNLOADS_BRUTOS):
        shutil.rmtree(PASTA_DOWNLOADS_BRUTOS)
        logging.info(f"Pasta antiga '{PASTA_DOWNLOADS_BRUTOS}' removida (force_refresh).")

    os.makedirs(PASTA_DOWNLOADS_BRUTOS, exist_ok=True)
    os.makedirs(PASTA_ARQUIVOS_LIMPOS, exist_ok=True)
    logging.info("Estrutura de pastas criada.")

    # 2. Raspagem de Links
    try:
        df_links = scrape_anvisa_links()
    except Exception as e:
        logging.error(f"Falha ao raspar os links da Anvisa: {e}")
        return

    # 3. Filtragem e Download
    data_inicio = datetime(ANO_INICIO, MES_INICIO, 1)
    data_fim = datetime(ANO_FIM, MES_FIM, 1)
    df_to_download = df_links[df_links.apply(
        lambda row: data_inicio <= datetime(row['ano'], row['mes'], 1) <= data_fim, 
        axis=1
    )]

    if df_to_download.empty:
        logging.warning("Nenhum link encontrado no período selecionado.")
    else:
        download_files(df_to_download)

    # 4. Limpeza e Consolidação BRUTA (sem processar vigências ainda)
    clean_downloaded_files(PASTA_DOWNLOADS_BRUTOS, PASTA_ARQUIVOS_LIMPOS)
    df_consolidado = consolidate_cleaned_files(PASTA_ARQUIVOS_LIMPOS, ARQUIVO_CONSOLIDADO_TEMP)
    
    if df_consolidado is None:
        logging.error("A consolidação falhou.")
        return

    # 5. Salvar Consolidado Bruto
    logging.info(f"[OK] Pipeline 1.0 concluído!")
    logging.info(f"Arquivo consolidado bruto: {os.path.abspath(ARQUIVO_CONSOLIDADO_TEMP)}")
    logging.info(f"Tamanho: {len(df_consolidado):,} linhas.")
    logging.info("")
    logging.info("Para fluxo completo, execute: python scripts/run_anvisa_completo.py")

if __name__ == "__main__":
    main()
