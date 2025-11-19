"""
Módulo de matching manual NFe x Base Manual (Google Sheets)
Processa EANs que não tiveram match automático com ANVISA
"""

import pandas as pd
import numpy as np
import gc
import os
from datetime import datetime


# ============================================================
# CONFIGURAÇÕES
# ============================================================

# URL do Google Sheets (modo de exportação direta)
URL_MANUAL = "https://docs.google.com/spreadsheets/d/1X4SvEpQkjIa306IUUZUebNSwjqTJTo1e/export?format=xlsx"


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def remove_accents(text):
    """Remove acentos de uma string"""
    import unicodedata
    try:
        text = text.decode('utf-8')
    except:
        pass
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return text


def ean_norm(col: pd.Series) -> pd.Series:
    """
    Normaliza uma coluna para o formato EAN13 como string.
    CORRIGIDO: Trata int64/float64 corretamente (remove .0)
    """
    # Primeiro, converter números para string SEM notação científica/decimal
    if pd.api.types.is_numeric_dtype(col):
        # Para int64: converter direto
        if pd.api.types.is_integer_dtype(col):
            s = col.astype(str).replace("nan", np.nan)
        # Para float64: converter para int64 primeiro (remove .0)
        else:
            s = col.fillna(0).astype('int64').astype(str).replace("0", np.nan)
    else:
        s = col.astype("string[pyarrow]").fillna("").str.strip()
    
    # Converter para string[pyarrow] se ainda não for
    if s.dtype != "string[pyarrow]":
        s = s.astype("string[pyarrow]")
    
    # Remover caracteres não-numéricos
    s = s.str.replace(r"[^0-9]", "", regex=True).replace("", np.nan)
    
    # Converter GTIN-14 para GTIN-13 (pegar últimos 13 dígitos)
    s = s.where(s.str.len() != 14, s.str[-13:])
    
    # Preencher com zeros à esquerda até 13 dígitos
    s = s.str.zfill(13)
    
    # Filtrar apenas EANs com exatamente 13 dígitos
    s = s.where(s.str.len() == 13)
    
    # Remover EANs inválidos (todos zeros)
    s = s.replace("0000000000000", pd.NA)
    
    return s.astype("string")


def carregar_base_manual():
    """
    Carrega a base manual do Google Sheets
    
    Retorna:
        DataFrame: Base manual com EANs e metadados
    """
    print("[INFO] Carregando base manual do Google Sheets...")
    print(f"[INFO] URL: {URL_MANUAL}")
    
    try:
        df_manual = pd.read_excel(URL_MANUAL)
        print(f"[OK] Base manual carregada: {df_manual.shape}")
        return df_manual
    except Exception as e:
        print(f"[ERRO] Falha ao carregar base manual: {str(e)}")
        raise


def preparar_lookup_manual(df_manual):
    """
    Prepara tabela de lookup a partir da base manual
    
    Parâmetros:
        df_manual (DataFrame): Base manual carregada
        
    Retorna:
        DataFrame: Tabela de lookup com EANs normalizados
    """
    print("\n[INFO] Preparando tabela de lookup manual...")
    
    # DEBUG: Mostrar colunas disponíveis
    print(f"[DEBUG] Colunas da base manual ({len(df_manual.columns)}):")
    for col in df_manual.columns:
        print(f"  - {col}")
    
    # Verificar se colunas EAN existem
    ean_cols_expected = ['EAN_1', 'EAN_2', 'EAN_3']
    ean_cols_found = [col for col in ean_cols_expected if col in df_manual.columns]
    
    if not ean_cols_found:
        print(f"[AVISO] Nenhuma coluna EAN esperada encontrada!")
        print(f"[INFO] Procurando colunas similares a EAN...")
        ean_like_cols = [col for col in df_manual.columns if 'EAN' in col.upper()]
        if ean_like_cols:
            print(f"[INFO] Colunas encontradas com 'EAN': {ean_like_cols}")
        
    # Normalizar EANs
    df_manual['EAN1_KEY'] = ean_norm(df_manual['EAN_1']) if 'EAN_1' in df_manual.columns else pd.Series(dtype='string')
    df_manual['EAN2_KEY'] = ean_norm(df_manual['EAN_2']) if 'EAN_2' in df_manual.columns else pd.Series(dtype='string')
    df_manual['EAN3_KEY'] = ean_norm(df_manual['EAN_3']) if 'EAN_3' in df_manual.columns else pd.Series(dtype='string')

    # DEBUG: Mostrar quantos EANs foram normalizados
    print(f"[DEBUG] EANs normalizados:")
    print(f"  EAN1_KEY: {df_manual['EAN1_KEY'].notna().sum()} não-nulos")
    print(f"  EAN2_KEY: {df_manual['EAN2_KEY'].notna().sum()} não-nulos")
    print(f"  EAN3_KEY: {df_manual['EAN3_KEY'].notna().sum()} não-nulos")

    # Preservar colunas de EAN originais para exportar após o merge
    ean_cols_orig = [col for col in df_manual.columns if col.upper().startswith('EAN_') and not col.endswith('_KEY')]
    meta_cols = [col for col in df_manual.columns if col not in ean_cols_orig and col not in ['EAN1_KEY', 'EAN2_KEY', 'EAN3_KEY']]
    id_vars = meta_cols + ean_cols_orig
    
    print(f"[DEBUG] Preparando melt:")
    print(f"  id_vars ({len(id_vars)}): {id_vars[:5]}...")  # Mostrar primeiros 5
    print(f"  value_vars: ['EAN1_KEY', 'EAN2_KEY', 'EAN3_KEY']")

    # "Unpivot" dos EANs normalizados mantendo os valores originais
    df_lookup = pd.melt(
        df_manual,
        id_vars=id_vars,
        value_vars=['EAN1_KEY', 'EAN2_KEY', 'EAN3_KEY'],
        var_name='EAN_SOURCE',
        value_name='EAN_KEY_MANUAL'
    )
    
    print(f"[DEBUG] Após melt: {len(df_lookup)} linhas")
    
    # Limpeza
    df_lookup = df_lookup.dropna(subset=['EAN_KEY_MANUAL'])
    print(f"[DEBUG] Após dropna: {len(df_lookup)} linhas")
    
    df_lookup = df_lookup.drop_duplicates(subset=['EAN_KEY_MANUAL'], keep='first')
    print(f"[DEBUG] Após drop_duplicates: {len(df_lookup)} linhas")
    df_lookup = df_lookup.dropna(subset=['EAN_KEY_MANUAL'])
    df_lookup = df_lookup.drop_duplicates(subset=['EAN_KEY_MANUAL'], keep='first')
    
    print(f"[OK] Lookup manual criado com {len(df_lookup):,} EANs únicos")
    
    return df_lookup


def executar_matching_manual(df):
    """
    Executa matching manual para EANs sem correspondência automática
    
    Parâmetros:
        df (DataFrame): DataFrame com resultados do matching ANVISA
        
    Retorna:
        DataFrame: DataFrame atualizado com matches manuais
    """
    print("\n" + "="*80)
    print("MATCHING MANUAL - BASE GOOGLE SHEETS")
    print("="*80 + "\n")
    
    # ================================================================
    # 1. ANÁLISE INICIAL
    # ================================================================
    print("--- ANÁLISE ANTES DO MATCHING MANUAL ---")
    
    # Verificar coluna PRODUTO
    if 'PRODUTO' not in df.columns:
        print("[AVISO] Coluna 'PRODUTO' não encontrada. Nenhum matching manual necessário.")
        return df
    
    mask_nulos_antes = df['PRODUTO'].isna()
    nulos_antes = mask_nulos_antes.sum()
    
    print(f"Total de linhas: {len(df):,}")
    print(f"Linhas sem match (PRODUTO nulo): {nulos_antes:,} ({nulos_antes/len(df)*100:.2f}%)")
    
    if nulos_antes == 0:
        print("\n[OK] Nenhuma linha sem match. Matching manual não necessário.")
        return df
    
    # ================================================================
    # 2. CARREGAR BASE MANUAL
    # ================================================================
    try:
        df_manual = carregar_base_manual()
    except Exception as e:
        print(f"[ERRO] Não foi possível carregar a base manual. Pulando matching manual.")
        return df
    
    # ================================================================
    # 3. PREPARAR LOOKUP
    # ================================================================
    try:
        df_lookup = preparar_lookup_manual(df_manual)
    except Exception as e:
        print(f"[ERRO] Falha ao preparar lookup manual: {str(e)}")
        return df
    
    # ================================================================
    # 4. EXECUTAR MATCHING
    # ================================================================
    print("\n--- EXECUTANDO MATCHING MANUAL ---")
    
    # Verificar coluna EAN
    if 'codigo_ean' not in df.columns:
        print("[ERRO] Coluna 'codigo_ean' não encontrada. Matching manual cancelado.")
        return df
    
    # Criar chave temporária normalizada
    temp_key = '_temp_ean_manual_'
    print(f"[INFO] Criando chave temporária '{temp_key}' a partir de 'codigo_ean'...")
    df[temp_key] = ean_norm(df['codigo_ean'])
    
    try:
        # Selecionar colunas a atualizar (exceto colunas auxiliares)
        cols_to_update = [
            col for col in df_lookup.columns
            if col not in {'EAN_SOURCE', 'EAN_KEY_MANUAL', 'EAN1_KEY', 'EAN2_KEY', 'EAN3_KEY'}
        ]
        
        print(f"[INFO] Colunas a atualizar: {len(cols_to_update)}")
        
        # Preparar subset para merge
        df_para_join = df.loc[mask_nulos_antes, [temp_key]].reset_index()
        
        # Executar merge
        merged = df_para_join.merge(
            df_lookup,
            left_on=temp_key,
            right_on='EAN_KEY_MANUAL',
            how='left'
        )
        
        # Filtrar sucessos (onde PRODUTO foi preenchido)
        if 'PRODUTO' in merged.columns:
            sucessos = merged.dropna(subset=['PRODUTO']).set_index('index')
        else:
            sucessos = pd.DataFrame()
        
        # ================================================================
        # 5. ATUALIZAR DATAFRAME PRINCIPAL
        # ================================================================
        if not sucessos.empty:
            print(f"\n[INFO] Atualizando {len(sucessos):,} registros com matches manuais...")
            
            # Atualizar cada coluna
            for col in cols_to_update:
                if col in df.columns:
                    df.loc[sucessos.index, col] = sucessos[col].values
            
            # Marcar via de match
            if 'match_via' in df.columns:
                df.loc[sucessos.index, 'match_via'] = 'manual_ean'
        
        # ================================================================
        # 6. ANÁLISE FINAL
        # ================================================================
        nulos_depois = df['PRODUTO'].isna().sum()
        novos_matches = nulos_antes - nulos_depois
        
        print("\n" + "="*80)
        print("RESULTADO DO MATCHING MANUAL")
        print("="*80)
        print(f"Linhas sem match ANTES:  {nulos_antes:,}")
        print(f"Linhas sem match DEPOIS: {nulos_depois:,}")
        print(f"Novos matches:           {novos_matches:,}")
        
        if novos_matches > 0:
            pct_melhoria = (novos_matches / nulos_antes) * 100
            print(f"Melhoria:                {pct_melhoria:.2f}%")
        
        print("="*80)
        
        # Auditoria de matches por via
        if 'match_via' in df.columns:
            print("\n[Auditoria] Contagem de matches por via (ATUALIZADA):")
            print(df['match_via'].value_counts(dropna=False).rename("contagem"))
        
    finally:
        # Limpar coluna temporária
        if temp_key in df.columns:
            print(f"\n[INFO] Removendo coluna temporária '{temp_key}'...")
            df.drop(columns=[temp_key], inplace=True)
    
    # ================================================================
    # 7. LIMPEZA DE MEMÓRIA
    # ================================================================
    del df_manual, df_lookup
    if 'merged' in locals():
        del merged
    gc.collect()
    
    return df


def limpar_colunas_temporarias(df):
    """
    Remove colunas temporárias usadas no matching
    
    Parâmetros:
        df (DataFrame): DataFrame a limpar
        
    Retorna:
        DataFrame: DataFrame limpo
    """
    print("\n[INFO] Limpando colunas temporárias...")
    
    cols_to_drop = [
        'codigo_ean_original',
        'cod_anvisa_original',
        'EAN_KEY',
        'REG_KEY',
        'match_via'
    ]
    
    cols_removidas = [c for c in cols_to_drop if c in df.columns]
    
    if cols_removidas:
        df = df.drop(columns=cols_removidas)
        print(f"[OK] {len(cols_removidas)} colunas removidas: {', '.join(cols_removidas)}")
    else:
        print("[INFO] Nenhuma coluna temporária para remover")
    
    return df


def converter_tipos_finais(df):
    """
    Converte tipos de dados para formato final
    
    Parâmetros:
        df (DataFrame): DataFrame a converter
        
    Retorna:
        DataFrame: DataFrame com tipos corrigidos
    """
    print("\n[INFO] Convertendo tipos de dados finais...")
    
    # Colunas inteiras (compatíveis com NaN)
    cols_int = [
        'ano_emissao',
        'mes_emissao',
        'ID_CMED_PRODUTO',
        'REGISTRO',
        'QUANTIDADE UNIDADES'
    ]
    
    conversoes = 0
    for col in cols_int:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            conversoes += 1
    
    print(f"[OK] {conversoes} colunas convertidas para Int64")
    
    return df


def processar_matching_manual(arquivo_entrada):
    """
    Processa matching manual completo
    
    Parâmetros:
        arquivo_entrada (str): Caminho do arquivo nfe_matched_*.csv
        
    Retorna:
        tuple: (DataFrame processado, caminho do arquivo de saída)
    """
    print("="*80)
    print("PIPELINE DE MATCHING MANUAL")
    print("="*80 + "\n")
    
    # Carregar dados
    print(f"[INFO] Carregando arquivo: {arquivo_entrada}")
    df = pd.read_csv(arquivo_entrada, sep=';', dtype={'codigo_ean': str})
    print(f"[OK] {len(df):,} registros carregados\n")
    
    # Remover acentos das colunas
    print("[INFO] Normalizando nomes de colunas...")
    df.columns = [remove_accents(col) for col in df.columns]
    print(f"[OK] {len(df.columns)} colunas normalizadas")
    
    # Executar matching manual
    df = executar_matching_manual(df)
    
    # Limpar colunas temporárias
    df = limpar_colunas_temporarias(df)
    
    # Converter tipos finais
    df = converter_tipos_finais(df)
    
    # Salvar resultado (SEM timestamp - usando overwriting)
    arquivo_saida = f"data/processed/nfe_etapa08_matched_manual.csv"
    
    print(f"\n[INFO] Salvando resultado em: {arquivo_saida}")
    df.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8')
    
    tamanho_mb = os.path.getsize(arquivo_saida) / (1024*1024)
    print(f"[OK] Arquivo salvo com sucesso ({tamanho_mb:.1f} MB)")
    
    print("\n" + "="*80)
    print("[SUCESSO] Matching manual concluído!")
    print("="*80 + "\n")
    
    return df, arquivo_saida


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    import os
    import glob
    
    # Encontrar arquivo matched (etapa 7)
    arquivo = "data/processed/nfe_etapa07_matched.csv"
    
    if not os.path.exists(arquivo):
        # Fallback: procura com padrão antigo
        arquivos = glob.glob("data/processed/nfe_matched_*.csv")
        if not arquivos:
            print("[ERRO] Nenhum arquivo nfe_matched encontrado!")
            exit(1)
        arquivo = max(arquivos, key=os.path.getmtime)
    
    print(f"[INFO] Processando: {os.path.basename(arquivo)}\n")
