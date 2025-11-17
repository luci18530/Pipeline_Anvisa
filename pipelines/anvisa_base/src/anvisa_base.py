"""
Módulo de carregamento e preparação da base ANVISA (CMED)
Carrega dados de preços de medicamentos e otimiza uso de memória
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Adicionar o diretório modules ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

# Importar módulo de apresentação
from apresentacao import normalizar_apresentacao, limpar_apresentacao_final, expandir_cx_bl


# ============================================================
# CONFIGURAÇÕES
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = PROJECT_ROOT / "output"
ANVISA_CANON_CSV = OUTPUT_DIR / "anvisa" / "baseANVISA.csv"
ANVISA_LEGACY_CSV = OUTPUT_DIR / "baseANVISA.csv"
ANVISA_CANON_DTYPES = OUTPUT_DIR / "anvisa" / "baseANVISA_dtypes.json"
ANVISA_LEGACY_DTYPES = OUTPUT_DIR / "baseANVISA_dtypes.json"


def _resolver_caminho(preferencial: Path, legado: Path, aviso: str) -> Path:
    if preferencial.exists():
        return preferencial
    if legado.exists():
        print(aviso)
        return legado
    return preferencial


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def _obter_arquivos_anvisa():
    aviso_base = (
        "[AVISO] baseANVISA.csv encontrada em output/. "
        "Mova para output/anvisa/baseANVISA.csv para aderir à reorganização."
    )
    aviso_dtypes = (
        "[AVISO] baseANVISA_dtypes.json encontrado em output/. "
        "Prefira mantê-lo em output/anvisa/baseANVISA_dtypes.json."
    )

    csv_path = _resolver_caminho(ANVISA_CANON_CSV, ANVISA_LEGACY_CSV, aviso_base)
    dtypes_path = _resolver_caminho(ANVISA_CANON_DTYPES, ANVISA_LEGACY_DTYPES, aviso_dtypes)
    return csv_path, dtypes_path


def verificar_arquivos_anvisa():
    """Verifica se os arquivos da base ANVISA existem"""
    arquivos_faltantes = []
    csv_path, dtypes_path = _obter_arquivos_anvisa()
    
    if not csv_path.exists():
        arquivos_faltantes.append(csv_path)
    
    if not dtypes_path.exists():
        arquivos_faltantes.append(dtypes_path)
    
    if arquivos_faltantes:
        msg = "[ERRO] Arquivos da base ANVISA não encontrados!\n"
        msg += "[INFO] Coloque os seguintes arquivos na pasta 'output/':\n"
        for arq in arquivos_faltantes:
            msg += f"  - {os.path.basename(arq)}\n"
        raise FileNotFoundError(msg)
    
    return csv_path, dtypes_path


def carregar_dtypes_anvisa():
    """Carrega o JSON com os tipos de dados da base ANVISA"""
    _, dtypes_path = _obter_arquivos_anvisa()
    print(f"[INFO] Carregando tipos de dados de: {dtypes_path}")
    
    with open(dtypes_path, 'r', encoding='utf-8') as f:
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
    csv_path, _ = _obter_arquivos_anvisa()
    print(f"\n[INFO] Carregando CSV de: {csv_path}")
    print("[INFO] Aguarde, este processo pode demorar...")
    
    dfpre = pd.read_csv(
        csv_path,
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
    
    # 1. Preservar APRESENTACAO_ORIGINAL como APRESENTACAO antes de remover _ORIGINAL
    if 'APRESENTACAO_ORIGINAL' in dfpre.columns and 'APRESENTACAO' not in dfpre.columns:
        print("[INFO] Criando coluna 'APRESENTACAO' a partir de 'APRESENTACAO_ORIGINAL'...")
        dfpre['APRESENTACAO'] = dfpre['APRESENTACAO_ORIGINAL']
    
    # 2. Remover outras colunas que terminam com "_ORIGINAL" (exceto APRESENTACAO_ORIGINAL por enquanto)
    print("[INFO] Removendo colunas '_ORIGINAL' desnecessárias...")
    colunas_original = [col for col in dfpre.columns if col.endswith('_ORIGINAL') and col != 'APRESENTACAO_ORIGINAL']
    if colunas_original:
        dfpre = dfpre.drop(columns=colunas_original)
    
    # 3. Remover coluna "SUBSTANCIA_COMPOSTA" se existir
    if 'SUBSTANCIA_COMPOSTA' in dfpre.columns:
        print("[INFO] Removendo coluna 'SUBSTANCIA_COMPOSTA'...")
        dfpre = dfpre.drop(columns=['SUBSTANCIA_COMPOSTA'])
    
    # Verificar mudanças
    cols_depois = set(dfpre.columns)
    removidas = cols_antes - cols_depois
    
    print(f"\n[OK] {len(removidas)} colunas removidas:")
    for c in sorted(removidas):
        print(f"  - {c}")
    
    # Verificar colunas importantes
    if 'CLASSE TERAPEUTICA' in dfpre.columns:
        print("\n[OK] Coluna 'CLASSE TERAPEUTICA' presente")
    else:
        print("\n[AVISO] Coluna 'CLASSE TERAPEUTICA' não encontrada")
    
    if 'APRESENTACAO' in dfpre.columns:
        print("[OK] Coluna 'APRESENTACAO' presente")
    else:
        print("[AVISO] Coluna 'APRESENTACAO' não encontrada")
    
    print("="*60)
    print("[SUCESSO] Limpeza concluída")
    print("="*60)
    
    return dfpre


def normalizar_apresentacoes_anvisa(dfpre):
    """
    Normaliza a coluna APRESENTACAO da base ANVISA usando as mesmas regras
    aplicadas nas NFe (217+ substituições e formatações)
    
    Parâmetros:
        dfpre (DataFrame): Base ANVISA com coluna APRESENTACAO
        
    Retorna:
        DataFrame: Base com APRESENTACAO normalizada
    """
    print("\n" + "="*60)
    print("[INICIO] Normalização de APRESENTACAO")
    print("="*60 + "\n")
    
    if 'APRESENTACAO' not in dfpre.columns:
        print("[AVISO] Coluna 'APRESENTACAO' não encontrada. Pulando normalização.")
        return dfpre
    
    # Verificar se existe APRESENTACAO_ORIGINAL para backup
    if 'APRESENTACAO_ORIGINAL' not in dfpre.columns:
        print("[INFO] Criando backup: APRESENTACAO_ORIGINAL...")
        dfpre['APRESENTACAO_ORIGINAL'] = dfpre['APRESENTACAO'].copy()
    
    # Criar flag de substância composta se necessário
    if 'SUBSTANCIA_COMPOSTA' not in dfpre.columns:
        if 'PRINCIPIO ATIVO' in dfpre.columns:
            print("[INFO] Criando flag SUBSTANCIA_COMPOSTA...")
            dfpre['SUBSTANCIA_COMPOSTA'] = dfpre['PRINCIPIO ATIVO'].str.contains(r'\+', na=False)
            compostos = dfpre['SUBSTANCIA_COMPOSTA'].sum()
            print(f"[OK] {compostos:,} substâncias compostas identificadas")
        else:
            dfpre['SUBSTANCIA_COMPOSTA'] = False
    
    # Contar apresentações únicas antes
    unicas_antes = dfpre['APRESENTACAO'].nunique()
    print(f"[INFO] Apresentações únicas (antes): {unicas_antes:,}")
    
    # Normalizar apresentações
    print("[INFO] Aplicando normalização (217+ regras)...")
    print("[INFO] Este processo pode demorar alguns segundos...")
    
    def _normalizar_row(texto, composta):
        """Aplica normalização linha por linha"""
        if pd.isna(texto):
            return texto
        
        # Aplicar normalização principal
        resultado = normalizar_apresentacao(str(texto), bool(composta))
        
        # Aplicar limpeza final
        resultado = limpar_apresentacao_final(resultado)
        
        # Expandir CX BL
        resultado = expandir_cx_bl(resultado)
        
        return resultado
    
    # Aplicar normalização
    dfpre['APRESENTACAO'] = dfpre.apply(
        lambda row: _normalizar_row(row['APRESENTACAO'], row['SUBSTANCIA_COMPOSTA']),
        axis=1
    )
    
    # Contar apresentações únicas depois
    unicas_depois = dfpre['APRESENTACAO'].nunique()
    reducao = unicas_antes - unicas_depois
    pct_reducao = (reducao / unicas_antes * 100) if unicas_antes > 0 else 0
    
    print(f"[OK] Normalização concluída!")
    print(f"[INFO] Apresentações únicas (depois): {unicas_depois:,}")
    print(f"[INFO] Redução de variações: {reducao:,} ({pct_reducao:.1f}%)")
    
    # Mostrar exemplos
    print("\n[INFO] Exemplos de normalização:")
    exemplos = dfpre[['APRESENTACAO_ORIGINAL', 'APRESENTACAO']].dropna().head(5)
    for idx, row in exemplos.iterrows():
        if row['APRESENTACAO_ORIGINAL'] != row['APRESENTACAO']:
            print(f"\n  ANTES: {row['APRESENTACAO_ORIGINAL'][:80]}")
            print(f"  DEPOIS: {row['APRESENTACAO'][:80]}")
    
    print("\n" + "="*60)
    print("[SUCESSO] Normalização de APRESENTACAO concluída")
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
    
    # Normalizar apresentações
    dfpre = normalizar_apresentacoes_anvisa(dfpre)
    
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
