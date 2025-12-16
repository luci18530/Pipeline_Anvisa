# -*- coding: utf-8 -*-
"""
Pipeline 1.5 - Processamento e Engenharia da Base ANVISA
Lê o arquivo consolidado bruto e aplica:
- Processamento de vigências
- Seleção de colunas (COLUNAS_PARA_MANTER)
- Cálculos de preços (PF 18%, PMVG 18%, fallbacks)
- Padronização de atributos
"""
import pandas as pd
import os
import sys
import shutil
import re
import logging
import unicodedata
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ANVISA_BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ANVISA_BASE_DIR))

from config_anvisa import ARQUIVO_CONSOLIDADO_TEMP, ARQUIVO_FINAL_VIGENCIAS

# ==============================================================================
#      CONFIGURAÇÕES
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Colunas que queremos manter na base final
COLUNAS_PARA_MANTER = [
    'REGISTRO',
    'CÓDIGO GGREM',
    'EAN 1',
    'EAN 2',
    'EAN 3',
    'PRINCÍPIO ATIVO',
    'LABORATÓRIO',
    'PRODUTO',
    'APRESENTAÇÃO',
    'CLASSE TERAPÊUTICA',
    'TIPO DE PRODUTO (STATUS DO PRODUTO)',
    'REGIME DE PREÇO',
    'PF 0%',
    'PF 18%',
    'PF 20%',
    'PMVG 0%',
    'PMVG 18%',
    'PMVG 20%',
    'ICMS 0%',
    'CAP',
    'ANO_REF',
    'MES_REF'
]

def process_vigencias(df_consolidado):
    """
    Processa o dataframe consolidado bruto para criar a tabela final de vigências.
    """
    logging.info("Iniciando processamento de vigências...")
    df = df_consolidado.copy()

    # PASSO 1: Preparação
    linhas_antes = len(df)
    df = df.dropna(subset=['ANO_REF', 'MES_REF'])
    df = df[(df['ANO_REF'] != '') & (df['MES_REF'] != '')]
    linhas_removidas = linhas_antes - len(df)
    if linhas_removidas > 0:
        logging.warning(f"Removidas {linhas_removidas} linhas com ANO_REF ou MES_REF inválidos")
    
    cols_to_check = ['PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%', 'ICMS 0%', 'CAP']
    
    # Limpar campos numéricos que não devem ter .0 (remover casas decimais de inteiros)
    # Converter para string e remover .0 no final
    for col in ['REGISTRO', 'CÓDIGO GGREM', 'EAN 1', 'EAN 2', 'EAN 3']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # Adicionar PF 18% e PMVG 18% se existirem no dataframe
    if 'PF 18%' in df.columns:
        cols_to_check.append('PF 18%')
    if 'PMVG 18%' in df.columns:
        cols_to_check.append('PMVG 18%')
    
    df['id_produto'] = df['REGISTRO'].astype(str).str.strip() + '-' + df['CÓDIGO GGREM'].astype(str).str.strip()
    
    # Converter ANO_REF e MES_REF para int antes de criar DATA_REF
    df['ANO_REF'] = pd.to_numeric(df['ANO_REF'], errors='coerce').fillna(0).astype(int)
    df['MES_REF'] = pd.to_numeric(df['MES_REF'], errors='coerce').fillna(0).astype(int)
    df['DATA_REF'] = pd.to_datetime(
        df['ANO_REF'].astype(str) + '-' + df['MES_REF'].astype(str) + '-01',
        format='%Y-%m-%d',
        errors='coerce'
    )
    
    # Conversão de colunas de preço
    for col in ['PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%']:
        if col in df.columns:
            s = df[col].astype(str).str.replace(',', '.', regex=False).str.replace(r'\.(?=.*\.)', '', regex=True)
            df[col] = pd.to_numeric(s, errors='coerce')
    
    df.sort_values(['id_produto', 'DATA_REF'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # PASSO 2: Detecção de Mudanças
    logging.info("Detectando mudanças de preços...")
    mudanca_valores = df[cols_to_check].ne(df[cols_to_check].shift(1)).any(axis=1)
    mudanca_produto = df['id_produto'] != df['id_produto'].shift(1)
    inicio_vigencia = mudanca_produto | mudanca_valores

    # PASSO 3: Construção de Vigências
    logging.info("Construindo tabela de vigências...")
    df_vigencias = df[inicio_vigencia].copy()
    df_vigencias['VIG_INICIO'] = df_vigencias['DATA_REF']
    df_vigencias['VIG_FIM'] = df_vigencias.groupby('id_produto')['VIG_INICIO'].shift(-1) - pd.Timedelta(days=1)

    def calcular_vig_fim_final(vig_inicio_date):
        if pd.isna(vig_inicio_date): 
            return None
        return pd.Timestamp(
            year=vig_inicio_date.year if vig_inicio_date.month <= 3 else vig_inicio_date.year + 1,
            month=4,
            day=15
        )
    
    last_vigencia_mask = df_vigencias['VIG_FIM'].isnull()
    df_vigencias.loc[last_vigencia_mask, 'VIG_FIM'] = df_vigencias.loc[last_vigencia_mask, 'VIG_INICIO'].apply(calcular_vig_fim_final)

    # PASSO 4: Criar ID de preço e selecionar colunas finais
    df_vigencias['id_preco'] = df_vigencias['id_produto'] + '_' + df_vigencias['VIG_INICIO'].dt.strftime('%Y%m%d')
    
    colunas_finais = [
        'id_preco', 'id_produto', 'VIG_INICIO', 'VIG_FIM',
        'PRINCÍPIO ATIVO', 'LABORATÓRIO', 'CÓDIGO GGREM', 'REGISTRO',
        'EAN 1', 'EAN 2', 'EAN 3',
        'PRODUTO', 'APRESENTAÇÃO', 'CLASSE TERAPÊUTICA',
        'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO',
        'PF 0%', 'PF 18%', 'PF 20%',
        'PMVG 0%', 'PMVG 18%', 'PMVG 20%',
        'ICMS 0%', 'CAP'
    ]
    
    df_vigencias_final = df_vigencias[[col for col in colunas_finais if col in df_vigencias.columns]].copy()
    
    # PASSO 5: Limpeza numérica final e preenchimento de preços
    def parse_num_seguro(x):
        if pd.isna(x): 
            return np.nan
        s = re.sub(r"[^\d,.\-]", "", unicodedata.normalize("NFKC", str(x)))
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            return float(s)
        except (ValueError, TypeError):
            return np.nan
        
    for c in ['PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%']:
        if c in df_vigencias_final.columns:
            df_vigencias_final[c] = df_vigencias_final[c].apply(parse_num_seguro)
    
    # Fallback para PF 20% e PMVG 20%
    mask_pf = df_vigencias_final['PF 20%'].isnull() & df_vigencias_final['PF 0%'].notnull()
    df_vigencias_final.loc[mask_pf, 'PF 20%'] = (df_vigencias_final.loc[mask_pf, 'PF 0%'] * 1.25).round(2)
    
    mask_pmvg = df_vigencias_final['PMVG 20%'].isnull() & df_vigencias_final['PMVG 0%'].notnull()
    df_vigencias_final.loc[mask_pmvg, 'PMVG 20%'] = (df_vigencias_final.loc[mask_pmvg, 'PMVG 0%'] * 1.25).round(2)

    # PASSO 5.1: Fallback para preços com ICMS 18%
    # Fórmula CMED: Preço com ICMS = Preço 0% / (1 - alíquota)
    # Para 18%: PF 18% = PF 0% / 0.82 ≈ PF 0% × 1.2195122
    FATOR_ICMS_18 = 1 / (1 - 0.18)
    
    if 'PF 18%' not in df_vigencias_final.columns:
        df_vigencias_final['PF 18%'] = np.nan
    mask_pf18 = df_vigencias_final['PF 18%'].isnull() & df_vigencias_final['PF 0%'].notnull()
    df_vigencias_final.loc[mask_pf18, 'PF 18%'] = (df_vigencias_final.loc[mask_pf18, 'PF 0%'] * FATOR_ICMS_18).round(2)
    
    if 'PMVG 18%' not in df_vigencias_final.columns:
        df_vigencias_final['PMVG 18%'] = np.nan
    mask_pmvg18 = df_vigencias_final['PMVG 18%'].isnull() & df_vigencias_final['PMVG 0%'].notnull()
    df_vigencias_final.loc[mask_pmvg18, 'PMVG 18%'] = (df_vigencias_final.loc[mask_pmvg18, 'PMVG 0%'] * FATOR_ICMS_18).round(2)

    # PASSO 6: Padronização de atributos
    logging.info("Padronizando atributos de texto pela última vigência...")
    cols_to_standardize = [
        'PRINCÍPIO ATIVO', 'LABORATÓRIO', 'PRODUTO', 'APRESENTAÇÃO',
        'CLASSE TERAPÊUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO'
    ]
    
    latest_data = df_vigencias_final.sort_values('VIG_INICIO').drop_duplicates(
        subset='id_produto', 
        keep='last'
    ).set_index('id_produto')
    
    for col in [c for c in cols_to_standardize if c in df_vigencias_final.columns]:
        df_vigencias_final[col] = df_vigencias_final['id_produto'].map(latest_data[col])
    
    # Uppercase em colunas de texto
    for col in df_vigencias_final.select_dtypes(include=['object']).columns:
        df_vigencias_final[col] = df_vigencias_final[col].str.upper()

    # PASSO 7: Remoção de duplicatas
    logging.info("Removendo duplicatas...")
    df_vigencias_final['quality_score'] = df_vigencias_final.notna().sum(axis=1)
    df_vigencias_final.sort_values(
        by=['id_produto', 'VIG_INICIO', 'quality_score'],
        ascending=[True, True, False],
        inplace=True
    )
    df_vigencias_final.drop_duplicates(subset=['id_produto', 'VIG_INICIO'], keep='first', inplace=True)
    df_vigencias_final.drop(columns=['quality_score'], inplace=True)
    
    return df_vigencias_final


def main():
    """Pipeline 1.5: Processamento e Engenharia"""
    logging.info("=" * 80)
    logging.info("PIPELINE 1.5 - PROCESSAMENTO E ENGENHARIA DA BASE ANVISA")
    logging.info("=" * 80)
    
    # Verificar se o arquivo consolidado bruto existe
    if not os.path.exists(ARQUIVO_CONSOLIDADO_TEMP):
        logging.error(f"Arquivo consolidado não encontrado: {ARQUIVO_CONSOLIDADO_TEMP}")
        logging.error("Execute primeiro o Pipeline 1.0 (1_download_consolidacao.py)")
        return
    
    # Carregar arquivo consolidado bruto
    logging.info(f"Carregando arquivo consolidado: {ARQUIVO_CONSOLIDADO_TEMP}")
    df_consolidado = pd.read_csv(ARQUIVO_CONSOLIDADO_TEMP, sep=';', encoding='utf-8', low_memory=False)
    logging.info(f"Carregado: {len(df_consolidado):,} linhas")
    
    # Processar vigências e aplicar engenharias
    df_vigencias_final = process_vigencias(df_consolidado)
    
    # Salvar resultado final
    df_vigencias_final.to_csv(ARQUIVO_FINAL_VIGENCIAS, sep=';', index=False, encoding='utf-8')
    logging.info(f"[OK] Base processada salva em: {os.path.abspath(ARQUIVO_FINAL_VIGENCIAS)}")
    logging.info(f"Tamanho final: {len(df_vigencias_final):,} linhas")
    
    # Garantir compatibilidade: copiar para output/anvisa/
    output_anvisa_dir = PROJECT_ROOT / 'output' / 'anvisa'
    os.makedirs(output_anvisa_dir, exist_ok=True)
    output_copy_path = output_anvisa_dir / 'baseANVISA.csv'
    
    shutil.copy2(ARQUIVO_FINAL_VIGENCIAS, output_copy_path)
    logging.info(f"[INFO] Base copiada para: {output_copy_path}")
    
    logging.info("")
    logging.info("=" * 80)
    logging.info("Pipeline 1.5 concluído! Base ANVISA pronta para uso no Pipeline NFe.")
    logging.info("=" * 80)

if __name__ == "__main__":
    main()
