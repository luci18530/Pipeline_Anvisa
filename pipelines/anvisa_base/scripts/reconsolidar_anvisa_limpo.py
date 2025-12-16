# -*- coding: utf-8 -*-
"""
Script para RE-CONSOLIDAR arquivos ANVISA_LIMPO_*.csv
Lê todos os arquivos ANVISA_LIMPO em data/processed/ e gera um novo consolidado_temp.csv
com TODAS as colunas, incluindo PF 18% e PMVG 18%.
"""
import pandas as pd
import glob
import os
import sys
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
PASTA_ARQUIVOS_LIMPOS = PROJECT_ROOT / "data" / "processed"
ARQUIVO_CONSOLIDADO_TEMP = PROJECT_ROOT / "data" / "processed" / "anvisa" / "anvisa_pmvg_consolidado_temp.csv"

def consolidar_arquivos_limpos():
    """
    Lê todos os arquivos ANVISA_LIMPO_*.csv e consolida em um único CSV.
    """
    logging.info("=" * 80)
    logging.info("RE-CONSOLIDANDO ARQUIVOS ANVISA_LIMPO")
    logging.info("=" * 80)
    
    # Buscar todos os arquivos ANVISA_LIMPO
    pattern = str(PASTA_ARQUIVOS_LIMPOS / "ANVISA_LIMPO_*.csv")
    arquivos = sorted(glob.glob(pattern))
    
    if not arquivos:
        logging.error(f"Nenhum arquivo ANVISA_LIMPO encontrado em {PASTA_ARQUIVOS_LIMPOS}")
        return None
    
    logging.info(f"Encontrados {len(arquivos)} arquivos ANVISA_LIMPO")
    
    # Ler e consolidar todos os arquivos
    dfs = []
    for arquivo in arquivos:
        nome = os.path.basename(arquivo)
        logging.info(f"  Lendo: {nome}")
        try:
            df = pd.read_csv(
                arquivo, 
                sep=';', 
                encoding='utf-8',
                low_memory=False,
                on_bad_lines='skip'
            )
            dfs.append(df)
            logging.info(f"    ✓ {len(df):,} linhas carregadas")
        except Exception as e:
            logging.warning(f"    ✗ Erro ao ler {nome}: {e}")
            continue
    
    if not dfs:
        logging.error("Nenhum dataframe válido foi carregado")
        return None
    
    # Concatenar todos os dataframes
    logging.info("Consolidando todos os arquivos...")
    df_consolidado = pd.concat(dfs, ignore_index=True)
    logging.info(f"Total consolidado: {len(df_consolidado):,} linhas")
    
    # Verificar se PF 18% e PMVG 18% estão presentes
    colunas_icms18 = []
    if 'PF 18%' in df_consolidado.columns:
        colunas_icms18.append('PF 18%')
    if 'PMVG 18%' in df_consolidado.columns:
        colunas_icms18.append('PMVG 18%')
    
    if colunas_icms18:
        logging.info(f"✓ Colunas ICMS 18% encontradas: {', '.join(colunas_icms18)}")
    else:
        logging.warning("⚠ Colunas PF 18% e PMVG 18% NÃO encontradas!")
    
    # Salvar arquivo consolidado
    os.makedirs(ARQUIVO_CONSOLIDADO_TEMP.parent, exist_ok=True)
    df_consolidado.to_csv(ARQUIVO_CONSOLIDADO_TEMP, sep=';', index=False, encoding='utf-8')
    
    logging.info(f"[OK] Arquivo consolidado salvo em: {ARQUIVO_CONSOLIDADO_TEMP}")
    logging.info(f"Colunas totais: {len(df_consolidado.columns)}")
    logging.info("")
    logging.info("Execute agora: python pipelines/anvisa_base/scripts/2_processamento_engenharia.py")
    
    return df_consolidado

def main():
    consolidar_arquivos_limpos()

if __name__ == "__main__":
    main()
