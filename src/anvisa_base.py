"""
Módulo de carregamento e preparação da base ANVISA (CMED)
Carrega dados de preços de medicamentos e otimiza uso de memória
"""

import pandas as pd
import json
import os
from datetime import datetime


# ============================================================
# CONFIGURAÇÕES
# ============================================================

OUTPUT_DIR = "output"
ANVISA_CSV_FILE = os.path.join(OUTPUT_DIR, "baseANVISA.csv")
ANVISA_DTYPES_FILE = os.path.join(OUTPUT_DIR, "baseANVISA_dtypes.json")


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def verificar_arquivos_anvisa():
    """Verifica se os arquivos da base ANVISA existem"""
    arquivos_faltantes = []
    
    if not os.path.exists(ANVISA_CSV_FILE):
        arquivos_faltantes.append(ANVISA_CSV_FILE)
    
    if not os.path.exists(ANVISA_DTYPES_FILE):
        arquivos_faltantes.append(ANVISA_DTYPES_FILE)
    
    if arquivos_faltantes:
        msg = "[ERRO] Arquivos da base ANVISA não encontrados!\n"
        msg += "[INFO] Coloque os seguintes arquivos na pasta 'output/':\n"
        for arq in arquivos_faltantes:
            msg += f"  - {os.path.basename(arq)}\n"
        raise FileNotFoundError(msg)
    
    return True


def carregar_dtypes_anvisa():
    """Carrega o JSON com os tipos de dados da base ANVISA"""
    print(f"[INFO] Carregando tipos de dados de: {ANVISA_DTYPES_FILE}")
    
    with open(ANVISA_DTYPES_FILE, 'r', encoding='utf-8') as f:
        dtypes = json.load(f)
    
    # Garantir que colunas sensíveis sejam string
    colunas_sensiveis = ["EAN", "CÓDIGO GGREM", "REGISTRO", "CNPJ", "CODIGO", "GTIN"]
    
    for col in dtypes.keys():
        if any(p in col.upper() for p in colunas_sensiveis):
            dtypes[col] = "string"
    
    print(f"[OK] {len(dtypes)} tipos de dados carregados")
    return dtypes


def carregar_base_anvisa(dtypes):
    """
    Carrega a base ANVISA (CMED) preservando tipos e datas
    
    Parâmetros:
        dtypes (dict): Dicionário com tipos de dados
        
    Retorna:
        DataFrame: Base ANVISA carregada
    """
    print("="*60)
    print("[INICIO] Carregamento da Base ANVISA (CMED)")
    print("="*60 + "\n")
    
    # Separar colunas de data e demais tipos
    print("[INFO] Processando definições de tipos...")
    parse_dates_cols = [col for col, tipo in dtypes.items() if 'datetime' in tipo.lower()]
    dtype_cols = {col: tipo for col, tipo in dtypes.items() if 'datetime' not in tipo.lower()}
    
    # Forçar string em colunas sensíveis
    colunas_sensiveis = ["EAN", "CÓDIGO GGREM", "REGISTRO", "CNPJ", "CODIGO", "GTIN"]
    for col in dtype_cols.keys():
        if any(p in col.upper() for p in colunas_sensiveis):
            dtype_cols[col] = "string"
    
    print(f"[INFO] Colunas de data: {len(parse_dates_cols)}")
    print(f"[INFO] Colunas com tipo definido: {len(dtype_cols)}")
    
    # Carregar CSV
    print(f"\n[INFO] Carregando CSV de: {ANVISA_CSV_FILE}")
    print("[INFO] Aguarde, este processo pode demorar...")
    
    dfpre = pd.read_csv(
        ANVISA_CSV_FILE,
        sep='\t',
        dtype=dtype_cols,
        parse_dates=parse_dates_cols,
        na_values=['', ' ', 'nan', 'NaN']
    )
    
    print(f"[OK] Base ANVISA carregada: {len(dfpre):,} registros, {len(dfpre.columns)} colunas")
    
    return dfpre


def limpar_colunas_anvisa(dfpre):
    """
    Limpa e padroniza colunas da base ANVISA
    
    Parâmetros:
        dfpre (DataFrame): Base ANVISA carregada
        
    Retorna:
        DataFrame: Base limpa
    """
    print("\n" + "="*60)
    print("[INICIO] Limpeza de Colunas")
    print("="*60 + "\n")
    
    cols_antes = set(dfpre.columns)
    
    # 1. Remover colunas que terminam com "_ORIGINAL"
    print("[INFO] Removendo colunas '_ORIGINAL'...")
    dfpre = dfpre.loc[:, ~dfpre.columns.str.endswith('_ORIGINAL')]
    
    # 2. Remover coluna "SUBSTANCIA_COMPOSTA" se existir
    if 'SUBSTANCIA_COMPOSTA' in dfpre.columns:
        print("[INFO] Removendo coluna 'SUBSTANCIA_COMPOSTA'...")
        dfpre = dfpre.drop(columns=['SUBSTANCIA_COMPOSTA'])
    
    # Verificar mudanças
    cols_depois = set(dfpre.columns)
    removidas = cols_antes - cols_depois
    
    print(f"\n[OK] {len(removidas)} colunas removidas:")
    for c in sorted(removidas):
        print(f"  - {c}")
    
    # Verificar coluna padronizada
    if 'CLASSE TERAPEUTICA' in dfpre.columns:
        print("\n[OK] Coluna final padronizada: CLASSE TERAPEUTICA")
    else:
        print("\n[AVISO] Coluna 'CLASSE TERAPEUTICA' não encontrada")
    
    print("="*60)
    print("[SUCESSO] Limpeza concluída")
    print("="*60)
    
    return dfpre


def otimizar_memoria_nfe(df):
    """
    Otimiza uso de memória do DataFrame de NFe
    
    Parâmetros:
        df (DataFrame): DataFrame de NFe
        
    Retorna:
        DataFrame: DataFrame otimizado
    """
    print("\n" + "="*60)
    print("[INICIO] Otimização de Memória - NFe")
    print("="*60 + "\n")
    
    # Remover colunas desnecessárias
    cols_to_drop = [
        'id_data_fabricacao',
        'id_data_validade',
        'id_medicamento',
        'data_emissao_original',
        'id_venc',
        'municipio_bruto'
    ]
    
    print("[INFO] Removendo colunas desnecessárias...")
    cols_removidas = [c for c in cols_to_drop if c in df.columns]
    if cols_removidas:
        df = df.drop(columns=cols_removidas)
        print(f"[OK] {len(cols_removidas)} colunas removidas: {', '.join(cols_removidas)}")
    else:
        print("[INFO] Nenhuma coluna desnecessária encontrada")
    
    # Medir memória inicial
    print("\n--- ANÁLISE INICIAL ---")
    initial_mem = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Uso de memória inicial: {initial_mem:.2f} MB")
    print(f"Registros: {len(df):,}")
    print(f"Colunas: {len(df.columns)}")
    
    # Aplicar otimizações
    print("\n--- APLICANDO OTIMIZAÇÕES ---")
    
    # Converter data_emissao para datetime (se ainda não for)
    if 'data_emissao' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['data_emissao']):
        print("[INFO] Convertendo 'data_emissao' para datetime...")
        df['data_emissao'] = pd.to_datetime(df['data_emissao'])
    
    # Converter colunas object para category
    print("\n[INFO] Convertendo colunas de texto para 'category'...")
    converted_cols = []
    for col in df.select_dtypes(include=['object']).columns:
        # Converter se número de valores únicos < 50% do total
        if df[col].nunique() / len(df) < 0.5:
            df[col] = df[col].astype('category')
            converted_cols.append(col)
    
    if converted_cols:
        print(f"[OK] {len(converted_cols)} colunas convertidas para 'category'")
        for col in converted_cols[:5]:  # Mostrar apenas as primeiras 5
            print(f"  - {col}")
        if len(converted_cols) > 5:
            print(f"  ... e mais {len(converted_cols) - 5}")
    
    # Downcast de inteiros
    print("\n[INFO] Otimizando colunas numéricas (downcast)...")
    int_cols = df.select_dtypes(include=['int64']).columns
    if len(int_cols) > 0:
        df[int_cols] = df[int_cols].apply(pd.to_numeric, downcast='integer')
        print(f"[OK] {len(int_cols)} colunas inteiras otimizadas")
    
    # Downcast de floats
    float_cols = df.select_dtypes(include=['float64']).columns
    if len(float_cols) > 0:
        df[float_cols] = df[float_cols].apply(pd.to_numeric, downcast='float')
        print(f"[OK] {len(float_cols)} colunas decimais otimizadas")
    
    # Medir memória final
    print("\n--- ANÁLISE FINAL ---")
    optimized_mem = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Uso de memória otimizado: {optimized_mem:.2f} MB")
    
    # Calcular economia
    reduction = ((initial_mem - optimized_mem) / initial_mem) * 100
    print("\n--- RESULTADO ---")
    print(f"Redução de memória: {reduction:.2f}%")
    print(f"Memória economizada: {(initial_mem - optimized_mem):.2f} MB")
    
    print("\n" + "="*60)
    print("[SUCESSO] Otimização concluída")
    print("="*60)
    
    return df


def processar_base_anvisa():
    """
    Processa a base ANVISA completa
    
    Retorna:
        DataFrame: Base ANVISA processada
    """
    print("="*60)
    print("Pipeline de Processamento da Base ANVISA")
    print("="*60 + "\n")
    
    # Verificar arquivos
    print("[VALIDANDO] Arquivos da base ANVISA...")
    verificar_arquivos_anvisa()
    print("[OK] Todos os arquivos encontrados!\n")
    
    # Carregar tipos
    dtypes = carregar_dtypes_anvisa()
    print()
    
    # Carregar base
    dfpre = carregar_base_anvisa(dtypes)
    
    # Limpar colunas
    dfpre = limpar_colunas_anvisa(dfpre)
    
    # Exibir amostra
    print("\n" + "="*60)
    print("Amostra de Dados (3 registros aleatórios)")
    print("="*60)
    print(dfpre.sample(min(3, len(dfpre))).to_string())
    
    # Exibir tipos
    print("\n" + "="*60)
    print("Resumo de Tipos de Dados")
    print("="*60)
    print(dfpre.dtypes.value_counts())
    
    print("\n" + "="*60)
    print("[SUCESSO] Base ANVISA processada com sucesso!")
    print("="*60)
    print(f"\nEstatísticas finais:")
    print(f"  - Registros: {len(dfpre):,}")
    print(f"  - Colunas: {len(dfpre.columns)}")
    print(f"  - Memória: {dfpre.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    return dfpre


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    # Processar base ANVISA
    dfpre = processar_base_anvisa()
    
    # Exibir colunas disponíveis
    print("\n" + "="*60)
    print("Colunas Disponíveis na Base ANVISA")
    print("="*60)
    for i, col in enumerate(sorted(dfpre.columns), 1):
        print(f"{i:2d}. {col}")
