# -*- coding: utf-8 -*-
"""
Módulo para limpeza e padronização de dados da Anvisa.
Responsável por padronizar as colunas GGREM e EAN.
"""
import pandas as pd
import sys
import os

# Adicionar src ao path para importar config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import COLUNAS_EAN

def padronizar_codigo_ggrem(df):
    """
    Padroniza a coluna 'CÓDIGO GGREM' removendo caracteres não numéricos.
    
    Args:
        df (pandas.DataFrame): DataFrame com a coluna 'CÓDIGO GGREM'
        
    Returns:
        pandas.DataFrame: DataFrame com a coluna 'CÓDIGO GGREM' padronizada
    """
    print("Padronizando 'CÓDIGO GGREM'...")
    
    if 'CÓDIGO GGREM' in df.columns:
        df['CÓDIGO GGREM'] = (
            df['CÓDIGO GGREM']
            .astype(str)
            .str.strip()
            .replace({'nan': None, 'None': None, '': None})
            .str.replace(r'\.0$', '', regex=True)
            .str.replace(r'[^0-9]', '', regex=True)
        )
        print("[OK] 'CODIGO GGREM' padronizado com sucesso.")
    else:
        print("[AVISO] Coluna 'CODIGO GGREM' nao encontrada.")
    
    return df

def padronizar_colunas_ean(df):
    """
    Padroniza as colunas EAN (EAN 1, EAN 2, EAN 3) removendo caracteres não numéricos.
    
    Args:
        df (pandas.DataFrame): DataFrame com as colunas EAN
        
    Returns:
        pandas.DataFrame: DataFrame com as colunas EAN padronizadas
    """
    print("Padronizando colunas EAN...")
    
    for col in COLUNAS_EAN:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace({'nan': '', 'None': '', '<NA>': '', '-': ''})
                .str.replace(r'\.0$', '', regex=True)
                .str.replace(r'[^0-9]', '', regex=True)
            )
        else:
            print(f"[AVISO] Coluna '{col}' nao encontrada.")
    
    print("[OK] Colunas EAN padronizadas com sucesso.")
    return df

def limpar_padronizar_dados(df):
    """
    Executa todas as etapas de limpeza e padronização dos dados.
    
    Args:
        df (pandas.DataFrame): DataFrame original
        
    Returns:
        pandas.DataFrame: DataFrame limpo e padronizado
    """
    print("=" * 80)
    print("INICIANDO LIMPEZA E PADRONIZAÇÃO DOS DADOS")
    print("=" * 80)
    
    # Fazer uma cópia para não modificar o original
    df_limpo = df.copy()
    
    # Padronizar GGREM
    df_limpo = padronizar_codigo_ggrem(df_limpo)
    
    # Padronizar EAN
    df_limpo = padronizar_colunas_ean(df_limpo)
    
    print("\n[OK] Limpeza e padronizacao concluida!")
    print("Amostra das colunas apos a limpeza:")
    
    # Mostrar amostra das colunas limpas
    colunas_para_mostrar = ['CÓDIGO GGREM'] + [col for col in COLUNAS_EAN if col in df_limpo.columns]
    if colunas_para_mostrar:
        print(df_limpo[colunas_para_mostrar].head())
    
    return df_limpo

if __name__ == "__main__":
    # Exemplo de uso (para testes)
    print("Este módulo deve ser importado e usado em conjunto com outros módulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")