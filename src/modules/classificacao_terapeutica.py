# -*- coding: utf-8 -*-
"""
Módulo para padronização da classificação terapêutica e criação do grupo anatômico.
Processa códigos ATC e cria categorias anatômicas.
"""
import pandas as pd
import re
import unicodedata
import sys
import os

# Adicionar src ao path para importar config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import GRUPOS_ANATOMICOS, CODIGOS_PSICO_NEUROLOGICOS, CODIGOS_ANESTESICOS_ANALGESICOS

def criar_backup_classe_original(df):
    """
    Cria um backup da coluna original para permitir re-execuções.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'CLASSE TERAPÊUTICA'
        
    Returns:
        pandas.DataFrame: DataFrame com backup criado
    """
    if 'CLASSE_TERAPEUTICA_ORIGINAL' not in df.columns:
        print("Criando backup 'CLASSE_TERAPEUTICA_ORIGINAL' para permitir re-execuções.")
        df['CLASSE_TERAPEUTICA_ORIGINAL'] = df['CLASSE TERAPÊUTICA']
    else:
        print("Utilizando backup 'CLASSE_TERAPEUTICA_ORIGINAL' como fonte para garantir consistência.")
    
    return df

def padronizar_classe_terapeutica_completa(texto):
    """
    Extrai, padroniza e recombina o código ATC e a descrição.
    
    Args:
        texto (str): Texto da classe terapêutica original
        
    Returns:
        str: Classe terapêutica padronizada ou None se inválida
    """
    if pd.isna(texto):
        return None

    original = str(texto).strip()

    # --- Extração ---
    codigo_bruto = original.split(' - ', 1)[0].strip()
    descricao_bruta = original.split(' - ', 1)[1] if ' - ' in original else ''

    # --- a) Padronização do Código ATC ---
    padrao1 = r'([A-Z])(\d)([A-Z]?\s*-\s*)'
    padrao2 = r'([A-Z])(\d)([A-Z])'
    padrao3 = r'^([A-Z])(\d)(?=\s|[A-Z]|$)'

    def corrigir_grupo(match):
        letra_grupo = match.group(1)
        numero_grupo = match.group(2).zfill(2)
        resto = match.group(3) if len(match.groups()) > 2 else ''
        return f"{letra_grupo}{numero_grupo}{resto}"

    codigo_corrigido = re.sub(padrao1, corrigir_grupo, codigo_bruto)
    codigo_corrigido = re.sub(padrao2, corrigir_grupo, codigo_corrigido)
    codigo_corrigido = re.sub(padrao3, corrigir_grupo, codigo_corrigido)
    codigo_padronizado = re.sub(r'00(\s|$)', r'\1', codigo_corrigido).strip()

    # --- b) Limpeza da Descrição ---
    desc = descricao_bruta.upper()
    desc = ''.join(c for c in unicodedata.normalize('NFD', desc) if unicodedata.category(c) != 'Mn')
    desc = re.sub(r'[^A-Z0-9\s]', '', desc)
    descricao_limpa = re.sub(r'\s+', ' ', desc).strip()

    # --- c) Montagem Final ---
    if descricao_limpa:
        return f"{codigo_padronizado} - {descricao_limpa}"
    else:
        return codigo_padronizado  # Retorna só o código se não houver descrição

def get_grupo_anatomico(classe_completa):
    """
    Extrai o código ATC da classe terapêutica e retorna o grupo anatômico correspondente.
    
    Args:
        classe_completa (str): Classe terapêutica no formato 'CODIGO - DESCRICAO'
        
    Returns:
        str: Grupo anatômico correspondente
    """
    # Etapa de extração e validação
    if not isinstance(classe_completa, str) or ' - ' not in classe_completa:
        return 'VÁRIOS'

    codigo_atc = classe_completa.split(' - ')[0].strip()
    if not codigo_atc:
        return 'VÁRIOS'

    # Lógica de negócio para categorização
    primeira_letra = codigo_atc[0]
    primeiros_tres = codigo_atc[:3]

    # Regras específicas (devem vir antes das gerais)
    if primeiros_tres in CODIGOS_PSICO_NEUROLOGICOS:
        return 'SISTEMA NERVOSO-PSICONEUROLÓGICOS'

    if primeiros_tres in CODIGOS_ANESTESICOS_ANALGESICOS:
        return 'SISTEMA NERVOSO-ANESTÉSICOS E ANALGÉSICOS'

    # Mapeamento de regras gerais
    return GRUPOS_ANATOMICOS.get(primeira_letra, 'VÁRIOS')

def padronizar_classe_terapeutica(df):
    """
    Padroniza a coluna 'CLASSE TERAPÊUTICA' do DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'CLASSE TERAPÊUTICA'
        
    Returns:
        pandas.DataFrame: DataFrame com classe terapêutica padronizada
    """
    print("=" * 80)
    print("PADRONIZAÇÃO DA CLASSE TERAPÊUTICA")
    print("=" * 80)
    
    # Criar backup para re-execução
    df = criar_backup_classe_original(df)
    
    print("\nIniciando a padronização da coluna 'CLASSE TERAPÊUTICA'...")
    
    # Aplicar padronização usando o backup como fonte
    df['CLASSE TERAPÊUTICA'] = df['CLASSE_TERAPEUTICA_ORIGINAL'].apply(
        padronizar_classe_terapeutica_completa
    )
    
    print("\n[OK] Padronizacao da Classe Terapeutica concluida.")
    
    # Verificação - mostrar amostra
    print("\nAmostra do resultado:")
    if len(df) >= 10:
        print(df[['CLASSE TERAPÊUTICA']].sample(10))
    else:
        print(df[['CLASSE TERAPÊUTICA']].head())
    
    return df

def criar_grupo_anatomico(df):
    """
    Cria a coluna 'GRUPO ANATOMICO' baseada na classe terapêutica.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'CLASSE TERAPÊUTICA' padronizada
        
    Returns:
        pandas.DataFrame: DataFrame com coluna 'GRUPO ANATOMICO' criada
    """
    print("=" * 80)
    print("CRIAÇÃO DA COLUNA 'GRUPO ANATÔMICO'")
    print("=" * 80)
    
    print("Criando a coluna 'GRUPO ANATOMICO' a partir da 'CLASSE TERAPÊUTICA'...")
    
    df['GRUPO ANATOMICO'] = df['CLASSE TERAPÊUTICA'].apply(get_grupo_anatomico)
    
    print("\n[OK] Coluna 'GRUPO ANATOMICO' criada/atualizada com sucesso.")
    
    # Verificação - mostrar amostra
    print("\nAmostra do resultado:")
    if len(df) >= 10:
        print(df[['CLASSE TERAPÊUTICA', 'GRUPO ANATOMICO']].sample(10))
    else:
        print(df[['CLASSE TERAPÊUTICA', 'GRUPO ANATOMICO']].head())
    
    print("\nDistribuição dos grupos anatômicos:")
    print(df['GRUPO ANATOMICO'].value_counts())
    
    return df

def processar_classificacao_terapeutica(df):
    """
    Executa o processo completo de padronização da classificação terapêutica.
    
    Args:
        df (pandas.DataFrame): DataFrame original
        
    Returns:
        pandas.DataFrame: DataFrame com classificação processada
    """
    print("=" * 80)
    print("PROCESSAMENTO DA CLASSIFICAÇÃO TERAPÊUTICA")
    print("=" * 80)
    
    # Fazer uma cópia para não modificar o original
    df_processado = df.copy()
    
    # Padronizar classe terapêutica
    df_processado = padronizar_classe_terapeutica(df_processado)
    
    # Criar grupo anatômico
    df_processado = criar_grupo_anatomico(df_processado)
    
    print("\n[OK] Processamento da classificacao terapeutica concluido!")
    
    return df_processado

if __name__ == "__main__":
    # Exemplo de uso (para testes)
    print("Este módulo deve ser importado e usado em conjunto com outros módulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")