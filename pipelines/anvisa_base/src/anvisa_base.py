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
DATA_DIR = PROJECT_ROOT / "data"

# Prioridade de busca para o CSV da base ANVISA:
# 1. Base PROCESSADA FINAL em output/anvisa/ (10 etapas, com ID_CMED_PRODUTO)
# 2. Base RAW consolidada em data/processed/anvisa/ (só consolidação, sem processamento)
# 3. Base legada em output/ (formato muito antigo)
ANVISA_PROCESSED_CSV = OUTPUT_DIR / "anvisa" / "baseANVISA.csv"  # PRIORIDADE 1 - Base processada (312k registros)
ANVISA_RAW_CSV = DATA_DIR / "processed" / "anvisa" / "base_anvisa_precos_vigencias.csv"  # PRIORIDADE 2 - Base raw (493k)
ANVISA_LEGACY_CSV = OUTPUT_DIR / "baseANVISA.csv"  # PRIORIDADE 3 - Legado

# Arquivos de tipos de dados
ANVISA_CANON_DTYPES = OUTPUT_DIR / "anvisa" / "baseANVISA_dtypes.json"
ANVISA_LEGACY_DTYPES = OUTPUT_DIR / "baseANVISA_dtypes.json"


def _resolver_caminho_csv() -> Path:
    """
    Resolve o caminho do CSV da base ANVISA com prioridade CORRETA:
    1. Base PROCESSADA em output/anvisa/ (312k registros, 10 etapas, com ID_CMED_PRODUTO)
    2. Base RAW consolidada em data/processed/anvisa/ (493k registros, só consolidação)
    3. Base legada em output/
    """
    # PRIORIDADE 1: Base processada final (output/anvisa/baseANVISA.csv)
    if ANVISA_PROCESSED_CSV.exists():
        print(f"[INFO] Usando base PROCESSADA (10 etapas): {ANVISA_PROCESSED_CSV}")
        return ANVISA_PROCESSED_CSV
    
    # PRIORIDADE 2: Base raw consolidada (fallback se não tiver processado)
    if ANVISA_RAW_CSV.exists():
        print(f"[AVISO] Usando base RAW (só consolidação): {ANVISA_RAW_CSV}")
        print(f"[AVISO] Para melhor matching, execute: python pipelines/anvisa_base/src/processar_dados.py")
        return ANVISA_RAW_CSV
    
    # PRIORIDADE 3: Base legada
    if ANVISA_LEGACY_CSV.exists():
        print(f"[AVISO] Usando base legada em output/: {ANVISA_LEGACY_CSV}")
        print("[AVISO] Considere mover para output/anvisa/")
        return ANVISA_LEGACY_CSV
    
    # Se nenhum existe, retornar o caminho preferencial para mensagem de erro clara
    return ANVISA_PROCESSED_CSV


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
    aviso_dtypes = (
        "[AVISO] baseANVISA_dtypes.json encontrado em output/. "
        "Prefira mantê-lo em output/anvisa/baseANVISA_dtypes.json."
    )

    csv_path = _resolver_caminho_csv()
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
    
    # Detectar separador (pode ser ; ou \t)
    with open(csv_path, 'r', encoding='utf-8') as f:
        primeira_linha = f.readline()
        separador = ';' if ';' in primeira_linha else '\t'
    print(f"[INFO] Separador detectado: '{separador}'")
    
    dfpre = pd.read_csv(
        csv_path,
        sep=separador,
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
    csv_path, dtypes_path = verificar_arquivos_anvisa()
    print("[OK] Todos os arquivos encontrados!\n")
    
    # Carregar tipos
    dtypes = carregar_dtypes_anvisa()
    print()
    
    # Carregar base
    dfpre = carregar_base_anvisa(dtypes)
    
    # Testes de integridade
    print("\n" + "="*60)
    print("[TESTES] Verificando integridade da base")
    print("="*60)
    
    # Teste 1: Colunas essenciais
    colunas_essenciais = ['id_produto', 'VIG_INICIO', 'PRINCÍPIO ATIVO', 'LABORATÓRIO', 
                          'PRODUTO', 'APRESENTAÇÃO', 'REGIME DE PREÇO']
    colunas_faltantes = [col for col in colunas_essenciais if col not in dfpre.columns]
    
    if colunas_faltantes:
        print(f"[AVISO] Colunas essenciais faltantes: {colunas_faltantes}")
    else:
        print(f"[OK] Todas as {len(colunas_essenciais)} colunas essenciais presentes")
    
    # Teste 2: Verificar colunas de preço
    colunas_preco = ['PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%']
    colunas_preco_presentes = [col for col in colunas_preco if col in dfpre.columns]
    print(f"[INFO] Colunas de preço encontradas: {len(colunas_preco_presentes)}/{len(colunas_preco)}")
    
    # Teste 3: Verificar registros nulos em colunas críticas
    for col in ['id_produto', 'PRINCÍPIO ATIVO', 'LABORATÓRIO']:
        if col in dfpre.columns:
            nulos = dfpre[col].isna().sum()
            pct = (nulos / len(dfpre)) * 100
            if pct > 5:
                print(f"[AVISO] '{col}' tem {nulos:,} nulos ({pct:.1f}%)")
            else:
                print(f"[OK] '{col}': {nulos:,} nulos ({pct:.2f}%)")
    
    # Teste 4: Verificar range de datas
    if 'VIG_INICIO' in dfpre.columns:
        data_min = dfpre['VIG_INICIO'].min()
        data_max = dfpre['VIG_INICIO'].max()
        print(f"[INFO] Range de vigências: {data_min} até {data_max}")
        
        # Verificar se tem dados recentes (últimos 3 meses)
        from datetime import datetime, timedelta
        tres_meses_atras = datetime.now() - timedelta(days=90)
        registros_recentes = dfpre[dfpre['VIG_INICIO'] >= tres_meses_atras]
        if len(registros_recentes) > 0:
            print(f"[OK] {len(registros_recentes):,} registros nos últimos 3 meses")
        else:
            print(f"[AVISO] Nenhum registro nos últimos 3 meses")
    
    print("="*60)
    
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
    print("[SUCESSO] Base ANVISA carregada com sucesso!")
    print("="*60)
    print(f"\nEstatísticas finais:")
    print(f"  - Registros: {len(dfpre):,}")
    print(f"  - Colunas: {len(dfpre.columns)}")
    print(f"  - Memória: {dfpre.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Mostrar onde a base está salva
    print("\n" + "="*60)
    print("📁 LOCALIZAÇÃO DA BASE ANVISA PROCESSADA")
    print("="*60)
    print(f"\n[ARQUIVO FONTE]")
    print(f"  {csv_path}")
    print(f"  Tamanho: {csv_path.stat().st_size / 1024**2:.1f} MB")
    
    print("\n" + "="*60)
    print("💡 DICA: Use 'dfpre' para acessar os dados em memória")
    print("="*60 + "\n")
    
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
