"""
Módulo de otimização de memória para DataFrames
Reduz uso de memória através de conversão de tipos e remoção de colunas
"""

import pandas as pd


def otimizar_memoria_dataframe(df, nome="DataFrame"):
    """
    Otimiza uso de memória de um DataFrame
    
    Parâmetros:
        df (DataFrame): DataFrame a ser otimizado
        nome (str): Nome do DataFrame para logging
        
    Retorna:
        DataFrame: DataFrame otimizado
    """
    print("\n" + "="*60)
    print(f"[INICIO] Otimização de Memória - {nome}")
    print("="*60 + "\n")
    
    # Medir memória inicial
    print("--- ANÁLISE INICIAL ---")
    initial_mem = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Uso de memória inicial: {initial_mem:.2f} MB")
    print(f"Registros: {len(df):,}")
    print(f"Colunas: {len(df.columns)}")
    
    # Aplicar otimizações
    print("\n--- APLICANDO OTIMIZAÇÕES ---")
    
    # Converter colunas object para category
    print("\n[INFO] Convertendo colunas de texto para 'category'...")
    converted_cols = []
    for col in df.select_dtypes(include=['object']).columns:
        # Converter se número de valores únicos < 50% do total
        unique_ratio = df[col].nunique() / len(df)
        if unique_ratio < 0.5:
            df[col] = df[col].astype('category')
            converted_cols.append((col, unique_ratio))
    
    if converted_cols:
        print(f"[OK] {len(converted_cols)} colunas convertidas para 'category'")
        for col, ratio in converted_cols[:5]:  # Mostrar apenas as primeiras 5
            print(f"  - {col} (valores únicos: {ratio*100:.1f}%)")
        if len(converted_cols) > 5:
            print(f"  ... e mais {len(converted_cols) - 5}")
    else:
        print("[INFO] Nenhuma coluna elegível para conversão")
    
    # Downcast de inteiros
    print("\n[INFO] Otimizando colunas numéricas (downcast)...")
    int_cols = df.select_dtypes(include=['int64']).columns
    if len(int_cols) > 0:
        df[int_cols] = df[int_cols].apply(pd.to_numeric, downcast='integer')
        print(f"[OK] {len(int_cols)} colunas inteiras otimizadas")
    else:
        print("[INFO] Nenhuma coluna int64 encontrada")
    
    # Downcast de floats
    float_cols = df.select_dtypes(include=['float64']).columns
    if len(float_cols) > 0:
        df[float_cols] = df[float_cols].apply(pd.to_numeric, downcast='float')
        print(f"[OK] {len(float_cols)} colunas decimais otimizadas")
    else:
        print("[INFO] Nenhuma coluna float64 encontrada")
    
    # Medir memória final
    print("\n--- ANÁLISE FINAL ---")
    optimized_mem = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Uso de memória otimizado: {optimized_mem:.2f} MB")
    
    # Calcular economia
    reduction = ((initial_mem - optimized_mem) / initial_mem) * 100
    economy = initial_mem - optimized_mem
    
    print("\n--- RESULTADO ---")
    print(f"Redução de memória: {reduction:.2f}%")
    print(f"Memória economizada: {economy:.2f} MB")
    
    print("\n" + "="*60)
    print("[SUCESSO] Otimização concluída")
    print("="*60)
    
    return df


def remover_colunas_desnecessarias(df, colunas_para_remover=None):
    """
    Remove colunas desnecessárias do DataFrame
    
    Parâmetros:
        df (DataFrame): DataFrame de origem
        colunas_para_remover (list): Lista de colunas a remover (opcional)
        
    Retorna:
        DataFrame: DataFrame com colunas removidas
    """
    # Colunas padrão para remover de NFe
    if colunas_para_remover is None:
        colunas_para_remover = [
            'id_data_fabricacao',
            'id_data_validade',
            'id_medicamento',
            'data_emissao_original',
            'id_venc',
            'municipio_bruto'
        ]
    
    print("[INFO] Removendo colunas desnecessárias...")
    cols_removidas = [c for c in colunas_para_remover if c in df.columns]
    
    if cols_removidas:
        df = df.drop(columns=cols_removidas)
        print(f"[OK] {len(cols_removidas)} colunas removidas:")
        for col in cols_removidas:
            print(f"  - {col}")
    else:
        print("[INFO] Nenhuma coluna desnecessária encontrada")
    
    return df


def preparar_nfe_para_matching(df):
    """
    Prepara DataFrame de NFe para matching com ANVISA
    Remove colunas desnecessárias e otimiza memória
    
    Parâmetros:
        df (DataFrame): DataFrame de NFe enriquecido
        
    Retorna:
        DataFrame: DataFrame preparado
    """
    print("\n" + "="*60)
    print("Preparação de NFe para Matching")
    print("="*60 + "\n")
    
    # Remover colunas desnecessárias
    df = remover_colunas_desnecessarias(df)
    
    # Converter data_emissao para datetime (se necessário)
    if 'data_emissao' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['data_emissao']):
            print("\n[INFO] Convertendo 'data_emissao' para datetime...")
            df['data_emissao'] = pd.to_datetime(df['data_emissao'])
            print("[OK] Conversão concluída")
    
    # Otimizar memória
    df = otimizar_memoria_dataframe(df, nome="NFe")
    
    return df


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    import glob
    import sys
    import os
    
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    # Buscar arquivo de NFe enriquecido mais recente
    data_dir = "data/processed"
    pattern = os.path.join(data_dir, "nfe_enriquecido_*.csv")
    arquivos = glob.glob(pattern)
    
    if not arquivos:
        print(f"[ERRO] Nenhum arquivo encontrado em: {pattern}")
        print("[INFO] Execute primeiro: python main_nfe.py")
        exit(1)
    
    # Usar o arquivo mais recente
    arquivo_mais_recente = max(arquivos, key=os.path.getctime)
    print(f"[INFO] Carregando: {os.path.basename(arquivo_mais_recente)}")
    
    # Carregar dados
    df = pd.read_csv(arquivo_mais_recente, sep=';', encoding='utf-8-sig')
    print(f"[OK] {len(df):,} registros carregados\n")
    
    # Preparar para matching
    df_otimizado = preparar_nfe_para_matching(df)
    
    print(f"\n[INFO] DataFrame otimizado pronto para matching!")
    print(f"[INFO] Memória atual: {df_otimizado.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
