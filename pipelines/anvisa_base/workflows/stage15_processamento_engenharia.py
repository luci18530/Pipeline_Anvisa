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
import shutil
import re
import logging
import unicodedata
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]

from pipelines.anvisa_base.config_anvisa import ARQUIVO_CONSOLIDADO_TEMP, ARQUIVO_FINAL_VIGENCIAS


def _normalizar_nome_coluna(col: str) -> str:
    texto = unicodedata.normalize("NFKD", str(col).upper())
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = re.sub(r"[^A-Z0-9% ]+", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def _padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: dict[str, str] = {}
    for col in df.columns:
        key = _normalizar_nome_coluna(col)
        if "GGREM" in key:
            rename_map[col] = "CÓDIGO GGREM"
        elif key == "CODIGO GGREM":
            rename_map[col] = "CÓDIGO GGREM"
        elif key == "PRINCIPIO ATIVO":
            rename_map[col] = "PRINCÍPIO ATIVO"
        elif key == "SUBSTANCIA":
            rename_map[col] = "PRINCÍPIO ATIVO"
        elif key == "LABORATORIO":
            rename_map[col] = "LABORATÓRIO"
        elif key == "APRESENTACAO":
            rename_map[col] = "APRESENTAÇÃO"
        elif key == "CLASSE TERAPEUTICA":
            rename_map[col] = "CLASSE TERAPÊUTICA"
        elif key == "REGIME DE PRECO":
            rename_map[col] = "REGIME DE PREÇO"

    if rename_map:
        df = df.rename(columns=rename_map)
    return df

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
    Processa vigências em chunks para evitar picos de memória.
    """
    import gc
    import tempfile
    
    logging.info("Iniciando processamento de vigências (processamento em chunks)...")
    df_consolidado = _padronizar_colunas(df_consolidado)
    colunas_criticas = ["REGISTRO", "CÓDIGO GGREM", "ANO_REF", "MES_REF"]
    faltantes = [c for c in colunas_criticas if c not in df_consolidado.columns]
    if faltantes:
        raise KeyError(
            f"Colunas críticas ausentes no consolidado: {faltantes}. "
            f"Colunas disponíveis: {list(df_consolidado.columns)}"
        )
    
    # Salvar em arquivo temporário para processar em chunks
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    temp_path = temp_file.name
    temp_file.close()
    
    # Etapa 1: Limpeza e preparação básica (ler e resalvar só o necessário)
    logging.info("Etapa 1: Limpeza de dados inválidos...")
    
    # Manter apenas colunas essenciais para economizar memória
    colunas_essenciais = [
        'REGISTRO', 'CÓDIGO GGREM', 'EAN 1', 'EAN 2', 'EAN 3',
        'PRINCÍPIO ATIVO', 'LABORATÓRIO', 'PRODUTO', 'APRESENTAÇÃO',
        'CLASSE TERAPÊUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO',
        'PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%',
        'ICMS 0%', 'CAP', 'ANO_REF', 'MES_REF'
    ]
    
    # Filtrar apenas colunas que existem
    cols_presentes = [c for c in colunas_essenciais if c in df_consolidado.columns]
    df_consolidado = df_consolidado[cols_presentes]
    
    # Remover linhas inválidas
    df_consolidado = df_consolidado.dropna(subset=['ANO_REF', 'MES_REF'], how='any')
    df_consolidado = df_consolidado[(df_consolidado['ANO_REF'] != '') & (df_consolidado['MES_REF'] != '')]
    
    logging.info(f"Mantidas {len(df_consolidado):,} linhas após limpeza")
    
    # Etapa 2: Preparação de tipos
    logging.info("Etapa 2: Otimização de tipos de dados...")
    
    for col in ['LABORATÓRIO', 'CLASSE TERAPÊUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO']:
        if col in df_consolidado.columns:
            df_consolidado[col] = df_consolidado[col].astype('category')
    
    for col in ['REGISTRO', 'CÓDIGO GGREM', 'EAN 1', 'EAN 2', 'EAN 3']:
        if col in df_consolidado.columns:
            df_consolidado[col] = df_consolidado[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    df_consolidado['ANO_REF'] = pd.to_numeric(df_consolidado['ANO_REF'], errors='coerce').fillna(0).astype('int16')
    df_consolidado['MES_REF'] = pd.to_numeric(df_consolidado['MES_REF'], errors='coerce').fillna(0).astype('int8')
    
    for col in ['PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%']:
        if col in df_consolidado.columns:
            s = df_consolidado[col].astype(str).str.replace(',', '.', regex=False).str.replace(r'\.(?=.*\.)', '', regex=True)
            df_consolidado[col] = pd.to_numeric(s, errors='coerce').astype('float32')
    
    df_consolidado['DATA_REF'] = pd.to_datetime(
        df_consolidado['ANO_REF'].astype(str) + '-' + df_consolidado['MES_REF'].astype(str) + '-01',
        format='%Y-%m-%d',
        errors='coerce'
    )
    
    df_consolidado['id_produto'] = df_consolidado['REGISTRO'].astype(str).str.strip() + '-' + df_consolidado['CÓDIGO GGREM'].astype(str).str.strip()
    
    # Etapa 3: Sort e identificar vigências
    logging.info("Etapa 3: Identificando mudanças de preço...")
    df_consolidado = df_consolidado.sort_values(['id_produto', 'DATA_REF']).reset_index(drop=True)
    
    cols_to_check = ['PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%', 'ICMS 0%', 'CAP']
    if 'PF 18%' in df_consolidado.columns:
        cols_to_check.append('PF 18%')
    if 'PMVG 18%' in df_consolidado.columns:
        cols_to_check.append('PMVG 18%')
    
    # Identificar início de vigências (mudança de preço ou produto)
    mudanca_valores = df_consolidado[cols_to_check].ne(df_consolidado[cols_to_check].shift(1)).any(axis=1)
    mudanca_produto = df_consolidado['id_produto'] != df_consolidado['id_produto'].shift(1)
    inicio_vigencia = mudanca_produto | mudanca_valores
    
    df_vigencias = df_consolidado[inicio_vigencia].copy()
    del df_consolidado, mudanca_valores, mudanca_produto, inicio_vigencia
    gc.collect()
    
    logging.info(f"Identificadas {len(df_vigencias):,} mudanças de vigência")
    
    # Etapa 4: Construir vigências
    logging.info("Etapa 4: Construindo tabela de vigências...")
    
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
    del df_vigencias
    gc.collect()
    
    # PASSO 5: Limpeza numérica final e preenchimento de preços
    logging.info("Etapa 5: Limpeza e preenchimento de preços...")
    
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
            df_vigencias_final[c] = df_vigencias_final[c].apply(parse_num_seguro).astype('float32')
    
    # Fallback para preços faltantes
    mask_pf = df_vigencias_final['PF 20%'].isnull() & df_vigencias_final['PF 0%'].notnull()
    df_vigencias_final.loc[mask_pf, 'PF 20%'] = (df_vigencias_final.loc[mask_pf, 'PF 0%'] * 1.25).round(2)
    
    mask_pmvg = df_vigencias_final['PMVG 20%'].isnull() & df_vigencias_final['PMVG 0%'].notnull()
    df_vigencias_final.loc[mask_pmvg, 'PMVG 20%'] = (df_vigencias_final.loc[mask_pmvg, 'PMVG 0%'] * 1.25).round(2)

    FATOR_ICMS_18 = 1 / (1 - 0.18)
    
    if 'PF 18%' not in df_vigencias_final.columns:
        df_vigencias_final['PF 18%'] = np.nan
    mask_pf18 = df_vigencias_final['PF 18%'].isnull() & df_vigencias_final['PF 0%'].notnull()
    df_vigencias_final.loc[mask_pf18, 'PF 18%'] = (df_vigencias_final.loc[mask_pf18, 'PF 0%'] * FATOR_ICMS_18).round(2).astype('float32')
    
    if 'PMVG 18%' not in df_vigencias_final.columns:
        df_vigencias_final['PMVG 18%'] = np.nan
    mask_pmvg18 = df_vigencias_final['PMVG 18%'].isnull() & df_vigencias_final['PMVG 0%'].notnull()
    df_vigencias_final.loc[mask_pmvg18, 'PMVG 18%'] = (df_vigencias_final.loc[mask_pmvg18, 'PMVG 0%'] * FATOR_ICMS_18).round(2).astype('float32')

    # PASSO 6: Padronização de atributos
    logging.info("Etapa 6: Padronizando atributos de texto...")
    cols_to_standardize = [
        'PRINCÍPIO ATIVO', 'LABORATÓRIO', 'PRODUTO', 'APRESENTAÇÃO',
        'CLASSE TERAPÊUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO'
    ]
    
    df_vigencias_final = df_vigencias_final.sort_values(['id_produto', 'VIG_INICIO'])
    
    for col in [c for c in cols_to_standardize if c in df_vigencias_final.columns]:
        df_vigencias_final[col] = df_vigencias_final.groupby('id_produto')[col].transform(
            lambda x: x.bfill().ffill()
        )
        nulos_apos = df_vigencias_final[col].isna().sum()
        if nulos_apos > 0:
            logging.info(f"  {col}: {nulos_apos:,} registros ainda nulos")

    # Uppercase em colunas de texto
    for col in df_vigencias_final.select_dtypes(include=['object']).columns:
        if col not in ['id_preco', 'id_produto']:
            df_vigencias_final[col] = df_vigencias_final[col].str.upper()

    # PASSO 7: Remoção de duplicatas
    logging.info("Etapa 7: Removendo duplicatas...")
    df_vigencias_final['quality_score'] = df_vigencias_final.notna().sum(axis=1)
    df_vigencias_final.sort_values(
        by=['id_produto', 'VIG_INICIO', 'quality_score'],
        ascending=[True, True, False],
        inplace=True
    )
    df_vigencias_final.drop_duplicates(subset=['id_produto', 'VIG_INICIO'], keep='first', inplace=True)
    df_vigencias_final.drop(columns=['quality_score'], inplace=True)
    
    logging.info(f"Finalizadas {len(df_vigencias_final):,} vigências únicas")
    gc.collect()
    return df_vigencias_final


def main():
    """Pipeline 1.5: Processamento e Engenharia"""
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    logging.info("=" * 80)
    logging.info("PIPELINE 1.5 - PROCESSAMENTO E ENGENHARIA DA BASE ANVISA")
    logging.info("=" * 80)
    
    # Verificar se o arquivo consolidado bruto existe
    if not os.path.exists(ARQUIVO_CONSOLIDADO_TEMP):
        logging.error(f"Arquivo consolidado não encontrado: {ARQUIVO_CONSOLIDADO_TEMP}")
        logging.error("Execute primeiro o Pipeline 1.0 (1_download_consolidacao.py)")
        return
    
    # Carregar arquivo consolidado bruto com chunked loading para evitar OOM
    logging.info(f"Carregando arquivo consolidado: {ARQUIVO_CONSOLIDADO_TEMP}")
    
    # Usar chunked loading para processar arquivo grande
    chunks = []
    chunk_size = 100000
    for i, chunk in enumerate(pd.read_csv(ARQUIVO_CONSOLIDADO_TEMP, sep=';', encoding='utf-8', low_memory=False, chunksize=chunk_size)):
        chunks.append(chunk)
        if (i + 1) % 5 == 0:
            logging.info(f"  Carregado {(i+1)*chunk_size:,} linhas...")
    
    df_consolidado = pd.concat(chunks, ignore_index=True)
    df_consolidado = _padronizar_colunas(df_consolidado)
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
