# -*- coding: utf-8 -*-
"""
Módulo para unificação de vigências consecutivas e idênticas.
Consolida registros que têm os mesmos valores mas vigências consecutivas.
"""
import pandas as pd
import sys
import os

# Adicionar src ao path para importar config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import COLUNAS_VERIFICACAO_MUDANCAS

def preparar_dados_para_unificacao(df):
    """
    Prepara os dados para o processo de unificação, ordenando e convertendo datas.
    
    Args:
        df (pandas.DataFrame): DataFrame original
        
    Returns:
        pandas.DataFrame: DataFrame preparado para unificação
    """
    print("Preparando dados para unificação...")
    
    df_prep = df.copy()
    
    # Converter datas
    df_prep['VIG_INICIO'] = pd.to_datetime(df_prep['VIG_INICIO'])
    df_prep['VIG_FIM'] = pd.to_datetime(df_prep['VIG_FIM'])
    
    # Ordenar por produto e data de início
    df_prep.sort_values(['id_produto', 'VIG_INICIO'], inplace=True)
    df_prep.reset_index(drop=True, inplace=True)
    
    print("[OK] Dados preparados para unificacao.")
    return df_prep

def identificar_blocos_identicos(df):
    """
    Identifica blocos de registros consecutivos com valores idênticos.
    
    Args:
        df (pandas.DataFrame): DataFrame preparado
        
    Returns:
        pandas.DataFrame: DataFrame com coluna 'bloco_id' identificando blocos
    """
    print("Identificando blocos de registros idênticos...")
    
    # Verificar quais colunas existem no DataFrame
    cols_to_check = [col for col in COLUNAS_VERIFICACAO_MUDANCAS if col in df.columns]
    
    if not cols_to_check:
        print("[AVISO] Nenhuma coluna de verificacao encontrada. Usando todas as colunas exceto datas.")
        cols_to_check = [col for col in df.columns 
                        if col not in ['VIG_INICIO', 'VIG_FIM', 'id_produto', 'id_preco']]
    
    print(f"Colunas sendo verificadas para mudanças: {cols_to_check}")
    
    # Identificar mudanças
    mudanca_produto = df['id_produto'] != df['id_produto'].shift(1)
    mudanca_valores = (df[cols_to_check].ne(df[cols_to_check].shift(1))).any(axis=1)
    inicio_bloco = mudanca_produto | mudanca_valores
    df['bloco_id'] = inicio_bloco.cumsum()
    
    print("[OK] Blocos identificados.")
    return df

def agregar_blocos(df):
    """
    Agrega os blocos identificados, mantendo o primeiro registro e a última data fim.
    
    Args:
        df (pandas.DataFrame): DataFrame com blocos identificados
        
    Returns:
        pandas.DataFrame: DataFrame com registros agregados
    """
    print("Agregando blocos...")
    
    # Preparar dicionário de agregação
    agg_dict = {col: 'first' for col in df.columns if col not in ['bloco_id', 'VIG_FIM']}
    agg_dict['VIG_FIM'] = 'last'
    
    # Agregar
    df_unificado = df.groupby('bloco_id').agg(agg_dict).reset_index(drop=True)
    
    print("[OK] Blocos agregados.")
    return df_unificado

def finalizar_unificacao(df):
    """
    Finaliza o processo de unificação, removendo colunas auxiliares e recriando id_preco.
    
    Args:
        df (pandas.DataFrame): DataFrame agregado
        
    Returns:
        pandas.DataFrame: DataFrame finalizado
    """
    print("Finalizando unificação...")
    
    # Recriar id_preco com base na nova data de início
    df['id_preco'] = (
        df['id_produto'] + '_' +
        df['VIG_INICIO'].dt.strftime('%Y%m%d')
    )
    
    # Remover coluna auxiliar se existir
    if 'bloco_id' in df.columns:
        df = df.drop('bloco_id', axis=1)
    
    print("[OK] Unificacao finalizada.")
    return df

def unificar_vigencias_consecutivas(df):
    """
    Executa o processo completo de unificação de vigências consecutivas.
    
    Args:
        df (pandas.DataFrame): DataFrame original
        
    Returns:
        pandas.DataFrame: DataFrame com vigências unificadas
    """
    print("=" * 80)
    print("INICIANDO UNIFICAÇÃO DE VIGÊNCIAS CONSECUTIVAS")
    print("=" * 80)
    
    linhas_antes = len(df)
    print(f"Número de linhas antes da unificação: {linhas_antes:,}")
    
    # PASSO 1: Preparação
    df_prep = preparar_dados_para_unificacao(df)
    
    # PASSO 2: Identificar blocos idênticos
    df_com_blocos = identificar_blocos_identicos(df_prep)
    
    # PASSO 3: Agregar os blocos
    df_unificado = agregar_blocos(df_com_blocos)
    
    # PASSO 4: Finalização
    df_final = finalizar_unificacao(df_unificado)
    
    # Estatísticas finais
    linhas_depois = len(df_final)
    economia = linhas_antes - linhas_depois
    
    print("\n[OK] Processo de unificacao concluido!")
    print(f"Numero de linhas apos a unificacao: {linhas_depois:,}")
    print("=" * 40)
    print(f"  LINHAS ECONOMIZADAS: {economia:,}  ")
    print("=" * 40)
    
    return df_final

if __name__ == "__main__":
    # Exemplo de uso (para testes)
    print("Este módulo deve ser importado e usado em conjunto com outros módulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")