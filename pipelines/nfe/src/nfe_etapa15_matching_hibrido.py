# -*- coding: utf-8 -*-
"""
ETAPA 15: MATCHING HIBRIDO PONDERADO

Para os produtos mais dificeis que restaram da etapa 14, aplica um algoritmo
de matching que calcula similaridade hibrida ponderando multiplos atributos:
- Nome do Produto: Similaridade textual
- Laboratorio: Similaridade textual  
- Atributos Numericos: Comparacao de dosagem, volume, unidades com tolerancia

Input:  df_etapa14_final_enriquecido.zip
Output: df_etapa15_resultado_matching_hibrido.zip
"""

import pandas as pd
import numpy as np
import zipfile
import os
import time
import re
import io
from pathlib import Path
import sys
from rapidfuzz import process, fuzz
from tqdm import tqdm
from paths import DATA_DIR, OUTPUT_DIR as PROJECT_OUTPUT_DIR

# Adicionar path do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

# ==============================================================================
#      CONFIGURACOES
# ==============================================================================

# Caminhos
PROCESSED_DIR = DATA_DIR / 'processed'
INPUT_ZIP = PROCESSED_DIR / 'df_etapa14_final_enriquecido.zip'
OUTPUT_ZIP = PROCESSED_DIR / 'df_etapa15_resultado_matching_hibrido.zip'

# Base mestre ANVISA
BASE_MESTRE_CANDIDATES = [
    PROJECT_OUTPUT_DIR / 'anvisa' / 'baseANVISA.csv',
    PROJECT_OUTPUT_DIR / 'baseANVISA.csv'
]

# Parametros do Matcher
W_NAME = 0.60  # Peso da similaridade do nome
W_LAB = 0.10   # Peso da similaridade do laboratorio
W_NUM = 0.30   # Peso dos atributos numericos
NUMERIC_TOLERANCE = 0.06  # 6% de tolerancia
JACCARD_PRE_FILTER_THRESHOLD = 0.175

# Dicionarios
SYNONYM_MAP = {
    'VITAMINA C': 'ACIDO ASCORBICO',
    'VITAMINA K': 'FITOMENADIONA',
    'VITAMINA D': 'COLECALCIFEROL',
    'VITAMINA D3': 'COLECALCIFEROL',
    'VITAMINA A': 'RETINOL',
    'VITAMINA E': 'ACETATO DE RACEALFATOCOFEROL',
    'RING': 'RINGER',
    'SORO': 'SOLUCAO'
}

PHARMA_STOPWORDS = {
    'SOLUCAO', 'INJETAVEL', 'LACTATO', 'MG', 'ML', 'G', 'UI', 'MCG',
    'COM', 'DE', 'DO', 'DA', 'FRASCO', 'CAIXA', 'AMPOLA', 'BISNAGA',
    'GOTAS', 'REV', 'LIB', 'PROL', 'SISTEMA', 'FECHADO', 'SIMPLES'
}

# CORRECAO: Colunas que sao realmente lixo (resultados vazios de matches anteriores)
# IMPORTANTE: NAO remover APRESENTACAO e EAN_* que sao colunas originais da NFe!
COLUNAS_LIXO = [
    'ID_CMED_PRODUTO_LIST', 'PRECO_MAXIMO_REFINADO', 
    'CAP_FLAG_CORRIGIDO', 'ICMS0_FLAG_CORRIGIDO'
]


def _resolver_base_mestre_path():
    """Localiza baseANVISA.csv aceitando estrutura antiga em output/."""
    for caminho in BASE_MESTRE_CANDIDATES:
        if caminho.exists():
            if caminho == BASE_MESTRE_CANDIDATES[1]:
                print(
                    "[AVISO] Base ANVISA encontrada em output/baseANVISA.csv. "
                    "Migre para output/anvisa/baseANVISA.csv quando possível."
                )
            return caminho
    raise FileNotFoundError(
        "Base mestre nao encontrada. Coloque baseANVISA.csv em output/anvisa/ ou mantenha uma copia temporaria em output/."
    )


def preencher_por_prioridade(df, colunas_prioridade):
    """Combina múltiplas colunas, respeitando ordem de prioridade linha a linha."""
    serie_resultado = pd.Series(pd.NA, index=df.index, dtype='object')
    for coluna in colunas_prioridade:
        if coluna in df.columns:
            serie_resultado = serie_resultado.combine_first(df[coluna])
    return serie_resultado


# ==============================================================================
#      FUNCOES DE LIMPEZA E PREPARACAO
# ==============================================================================

def clean_text(text):
    """Remove caracteres especiais e normaliza texto."""
    if not isinstance(text, str):
        return ''
    return re.sub(r'[^\w\s]', '', text.upper()).strip()


def clean_numeric(series):
    """Converte serie para numerico com tratamento de erros."""
    return pd.to_numeric(
        series.astype(str).str.replace(',', '.', regex=False),
        errors='coerce'
    ).fillna(0)


def remover_acentos(series):
    """Remove acentos de uma serie pandas."""
    return series.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')


def apply_synonyms(text, synonym_map):
    """Aplica sinonimos em um texto."""
    if not isinstance(text, str):
        return ""
    for key in sorted(synonym_map, key=len, reverse=True):
        text = re.sub(
            r'\b' + re.escape(key) + r'\b',
            synonym_map[key],
            text,
            flags=re.IGNORECASE
        )
    return text


def remove_stopwords(text, stopwords):
    """Remove stopwords de um texto."""
    if not isinstance(text, str):
        return ""
    return " ".join([word for word in text.split() if word not in stopwords])


def build_inverted_index(series):
    """Constroi indice invertido para busca rapida."""
    inverted_index = {}
    for idx, text in series.items():
        if isinstance(text, str):
            for word in set(text.split()):
                if len(word) > 2:
                    inverted_index.setdefault(word, []).append(idx)
    return inverted_index


# ==============================================================================
#      CARREGAMENTO E PREPARACAO DOS DADOS
# ==============================================================================

def carregar_dados_etapa14():
    """Carrega e limpa o DataFrame da etapa 14."""
    print("\n" + "="*80)
    print("CARREGANDO DADOS DA ETAPA 14")
    print("="*80)
    
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {INPUT_ZIP}")
    
    with zipfile.ZipFile(INPUT_ZIP, 'r') as z:
        csv_name = 'df_etapa14_final_enriquecido.csv'
        with z.open(csv_name) as f:
            df = pd.read_csv(f, sep=';')
    
    print(f"[OK] Carregado: {len(df):,} registros, {len(df.columns)} colunas")
    
    # Remover colunas completamente nulas
    colunas_nulas = df.columns[df.isna().all()].tolist()
    if colunas_nulas:
        print(f"\n[1/2] Removendo {len(colunas_nulas)} colunas completamente nulas")
        df = df.drop(columns=colunas_nulas)
    
    # Remover colunas lixo de merges anteriores
    colunas_dropar = [col for col in COLUNAS_LIXO if col in df.columns]
    if colunas_dropar:
        print(f"[2/2] Removendo {len(colunas_dropar)} colunas lixo de merges anteriores")
        df = df.drop(columns=colunas_dropar)
    
    print(f"\n[OK] Limpeza concluida: {len(df.columns)} colunas restantes")
    
    return df


def carregar_base_mestre():
    """Carrega e prepara a base mestre ANVISA."""
    print("\n" + "="*80)
    print("CARREGANDO BASE MESTRE ANVISA")
    print("="*80)
    
    base_path = _resolver_base_mestre_path()

    df = pd.read_csv(base_path, sep='\t', dtype=str, low_memory=False)
    
    print(f"[OK] Base mestre carregada: {len(df):,} registros (fonte: {base_path})")
    
    # Padronizar nomes de colunas
    mapa_colunas = {
        'QUANTIDADE MG': 'QUANTIDADE MG (POR UNIDADE/ML)'
    }
    df = df.rename(columns=mapa_colunas)
    
    # Limpeza pre-deduplicacao
    chave_dedup = ['PRODUTO', 'APRESENTACAO_ORIGINAL', 'LABORATORIO']
    for col in chave_dedup:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip().str.upper()
    
    # Deduplicacao
    antes = len(df)
    df = df.drop_duplicates(subset=chave_dedup).reset_index(drop=True)
    removidos = antes - len(df)
    if removidos > 0:
        print(f"[OK] Removidas {removidos:,} duplicatas")
    
    return df


def preparar_base_mestre(df_master):
    """Prepara base mestre para matching."""
    print("\n" + "="*80)
    print("PREPARANDO BASE MESTRE PARA MATCHING")
    print("="*80)
    
    df = df_master.copy()
    
    # Preparar colunas de texto
    print("[1/4] Limpando colunas de texto...")
    colunas_texto = ['PRODUTO', 'PRINCIPIO ATIVO', 'LABORATORIO']
    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].fillna('')
    
    # Criar colunas limpas
    print("[2/4] Criando colunas normalizadas...")
    if 'PRODUTO' in df.columns:
        df['PRODUTO_CLEAN'] = remover_acentos(
            df['PRODUTO'].apply(clean_text).apply(lambda t: apply_synonyms(t, SYNONYM_MAP))
        )
    
    if 'PRINCIPIO ATIVO' in df.columns:
        df['PRINCIPIO_ATIVO_CLEAN'] = remover_acentos(
            df['PRINCIPIO ATIVO'].apply(clean_text).apply(lambda t: apply_synonyms(t, SYNONYM_MAP))
        )
    
    if 'LABORATORIO' in df.columns:
        df['LABORATORIO_CLEAN'] = remover_acentos(df['LABORATORIO'].apply(clean_text))
    
    # Preparar colunas numericas
    print("[3/4] Preparando colunas numericas...")
    num_cols = ['QUANTIDADE MG (POR UNIDADE/ML)', 'QUANTIDADE ML', 'QUANTIDADE UI', 'QUANTIDADE UNIDADES']
    for col in num_cols:
        if col in df.columns:
            df[col] = clean_numeric(df[col])
        else:
            df[col] = 0
    
    # Criar indices e colunas otimizadas
    print("[4/4] Criando indices invertidos...")
    prod_index = build_inverted_index(df['PRODUTO_CLEAN'])
    pa_index = build_inverted_index(df['PRINCIPIO_ATIVO_CLEAN'])
    
    df['PRODUTO_SPECIFIC'] = df['PRODUTO_CLEAN'].apply(lambda t: remove_stopwords(t, PHARMA_STOPWORDS))
    df['PA_SPECIFIC'] = df['PRINCIPIO_ATIVO_CLEAN'].apply(lambda t: remove_stopwords(t, PHARMA_STOPWORDS))
    df['WORD_SET'] = df['PRODUTO_CLEAN'].str.split().apply(
        lambda ws: {w for w in ws if len(w) > 2} if isinstance(ws, list) else set()
    )
    
    print("[OK] Base mestre preparada")
    
    return df, prod_index, pa_index


def preparar_entrada(df_entrada):
    """Prepara DataFrame de entrada para matching."""
    print("\n" + "="*80)
    print("PREPARANDO DADOS DE ENTRADA")
    print("="*80)
    
    df = df_entrada.copy()
    total_linhas = len(df)

    # Garantir texto básico para produto
    if 'NOME_PRODUTO_LIMPO' not in df.columns:
        df['NOME_PRODUTO_LIMPO'] = pd.NA
    colunas_produto = ['NOME_PRODUTO_LIMPO', 'IA_PRODUTO', 'descricao_produto']
    df['NOME_PRODUTO_LIMPO'] = preencher_por_prioridade(df, colunas_produto).fillna('').astype(str)

    # Garantir texto básico para laboratório
    if 'IA_LABORATORIO' not in df.columns:
        df['IA_LABORATORIO'] = pd.NA
    colunas_lab = ['IA_LABORATORIO', 'LABORATORIO', 'razao_social_emitente']
    df['IA_LABORATORIO'] = preencher_por_prioridade(df, colunas_lab).fillna('').astype(str)

    disponiveis_produto = (df['NOME_PRODUTO_LIMPO'].str.strip() != '').sum()
    disponiveis_lab = (df['IA_LABORATORIO'].str.strip() != '').sum()
    print(f"[INFO] Texto de produto disponível para {disponiveis_produto:,}/{total_linhas:,} registros")
    print(f"[INFO] Texto de laboratório disponível para {disponiveis_lab:,}/{total_linhas:,} registros")
    
    # Criar colunas limpas
    print("[1/2] Normalizando colunas de texto...")
    df['IA_PRODUTO_CLEAN'] = remover_acentos(df['NOME_PRODUTO_LIMPO'].apply(clean_text))
    df['IA_LABORATORIO_CLEAN'] = remover_acentos(df['IA_LABORATORIO'].apply(clean_text))
    
    # Preparar colunas numericas
    print("[2/2] Preparando colunas numericas...")
    num_cols = [
        'IA_QUANTIDADE MG (POR UNIDADE/ML)',
        'IA_QUANTIDADE ML',
        'IA_QUANTIDADE UI',
        'IA_QUANTIDADE UNIDADES'
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = clean_numeric(df[col])
        else:
            df[col] = 0
    
    print("[OK] Dados de entrada preparados")
    
    return df


# ==============================================================================
#      ALGORITMO DE MATCHING
# ==============================================================================

def calculate_numeric_score(val1, val2, tolerance):
    """Compara dois valores numericos com tolerancia."""
    if val1 == 0 and val2 == 0:
        return 1.0
    if val1 == 0 or val2 == 0:
        return 0.0
    return 1.0 if abs(val1 - val2) / max(val1, val2) <= tolerance else 0.0


def find_best_match(row, med_df, prod_index, pa_index):
    """Encontra o melhor match para uma linha de entrada."""
    
    # 1. Preparar produto de entrada
    produto_in_original = row['IA_PRODUTO_CLEAN']
    produto_in = apply_synonyms(produto_in_original, SYNONYM_MAP)
    
    if not produto_in:
        return None, 0.0, 'Produto de entrada vazio'
    
    # 2. Encontrar candidatos via indice invertido
    search_words = {w for w in produto_in.split() if len(w) > 2}
    candidate_indices = set()
    
    for word in search_words:
        candidate_indices.update(prod_index.get(word, []))
        candidate_indices.update(pa_index.get(word, []))
    
    if not candidate_indices:
        return None, 0.0, 'Sem candidatos pelo indice'
    
    candidates = med_df.loc[list(candidate_indices)].copy()
    
    # 3. Pre-filtragem com Jaccard
    candidates['jaccard_sim'] = candidates['WORD_SET'].apply(
        lambda s: len(s & search_words) / len(s | search_words) if (s or search_words) else 0.0
    )
    candidates = candidates[candidates['jaccard_sim'] > JACCARD_PRE_FILTER_THRESHOLD]
    
    if candidates.empty:
        return None, 0.0, 'Sem candidatos apos filtro Jaccard'
    
    # 4. Calcular scores de similaridade
    
    # Score de nome
    produto_in_specific = remove_stopwords(produto_in, PHARMA_STOPWORDS)
    
    base_score = np.maximum(
        process.cdist([produto_in], candidates['PRODUTO_CLEAN'].tolist(), 
                     scorer=fuzz.token_set_ratio, workers=1)[0],
        process.cdist([produto_in], candidates['PRINCIPIO_ATIVO_CLEAN'].tolist(),
                     scorer=fuzz.token_set_ratio, workers=1)[0]
    )
    
    spec_score = np.maximum(
        process.cdist([produto_in_specific], candidates['PRODUTO_SPECIFIC'].tolist(),
                     scorer=fuzz.token_sort_ratio, workers=1)[0],
        process.cdist([produto_in_specific], candidates['PA_SPECIFIC'].tolist(),
                     scorer=fuzz.token_sort_ratio, workers=1)[0]
    )
    
    candidates['name_score'] = ((base_score * 0.4) + (spec_score * 0.6)) / 100.0
    candidates['precision_bonus'] = (
        process.cdist([produto_in], candidates['PRODUTO_CLEAN'].tolist(),
                     scorer=fuzz.QRatio, workers=1)[0] > 98
    ).astype(float) * 0.15
    
    # Score de laboratorio
    lab_in = row['IA_LABORATORIO_CLEAN']
    if lab_in:
        candidates['lab_score'] = process.cdist(
            [lab_in], candidates['LABORATORIO_CLEAN'].tolist(),
            scorer=fuzz.token_set_ratio, workers=1
        )[0] / 100.0
    else:
        candidates['lab_score'] = 0.5
    
    # Score numerico
    num_scores = []
    mapeamento_numerico = {
        'IA_QUANTIDADE MG (POR UNIDADE/ML)': 'QUANTIDADE MG (POR UNIDADE/ML)',
        'IA_QUANTIDADE ML': 'QUANTIDADE ML',
        'IA_QUANTIDADE UI': 'QUANTIDADE UI',
        'IA_QUANTIDADE UNIDADES': 'QUANTIDADE UNIDADES'
    }
    
    for col_entrada, col_candidato in mapeamento_numerico.items():
        val_in = row[col_entrada]
        if val_in > 0:
            scores = candidates[col_candidato].apply(
                lambda val2: calculate_numeric_score(val_in, val2, NUMERIC_TOLERANCE)
            )
            num_scores.append(scores)
    
    candidates['avg_num_score'] = pd.DataFrame(num_scores).mean(axis=0).values if num_scores else 0.5
    
    # 5. Score final
    candidates['total_score'] = (
        (candidates['name_score'] * W_NAME) +
        (candidates['lab_score'] * W_LAB) +
        (candidates['avg_num_score'] * W_NUM) +
        candidates['precision_bonus']
    )
    
    if candidates.empty:
        return None, 0.0, 'Nenhum candidato pontuado'
    
    best_match_row = candidates.loc[candidates['total_score'].idxmax()]
    return best_match_row.name, best_match_row['total_score'], 'Match encontrado'


# ==============================================================================
#      EXECUCAO DO MATCHING
# ==============================================================================

def executar_matching(df_entrada, df_master, prod_index, pa_index):
    """Executa o pipeline de matching hibrido."""
    print("\n" + "="*80)
    print("EXECUTANDO MATCHING HIBRIDO")
    print("="*80)
    
    # Criar DataFrame de trabalho com combinacoes unicas
    print("\n[1/4] Identificando combinacoes unicas...")
    group_keys = [
        'IA_PRODUTO_CLEAN',
        'IA_LABORATORIO_CLEAN',
        'IA_QUANTIDADE MG (POR UNIDADE/ML)',
        'IA_QUANTIDADE ML',
        'IA_QUANTIDADE UI',
        'IA_QUANTIDADE UNIDADES'
    ]
    
    df_workload = df_entrada[group_keys].drop_duplicates().reset_index(drop=True)
    print(f"  -> Processando {len(df_workload):,} combinacoes unicas")
    print(f"     (em vez de {len(df_entrada):,} linhas totais)")
    
    # Executar matching
    print("\n[2/4] Executando matching...")
    results = []
    
    for idx, row in tqdm(df_workload.iterrows(), total=len(df_workload), desc="Matching"):
        match_idx, score, status = find_best_match(row, df_master, prod_index, pa_index)
        results.append({
            'best_match_index': match_idx,
            'match_score': score,
            'match_status': status
        })
    
    df_results = pd.DataFrame(results)
    df_workload_results = pd.concat([df_workload, df_results], axis=1)
    
    # Mapear resultados de volta
    print("\n[3/4] Mapeando resultados para DataFrame completo...")
    df_final_matched = pd.merge(
        df_entrada,
        df_workload_results,
        on=group_keys,
        how='left'
    )
    
    # Remover colunas temporarias
    df_final_matched = df_final_matched.drop(
        columns=['IA_PRODUTO_CLEAN', 'IA_LABORATORIO_CLEAN'],
        errors='ignore'
    )
    
    # Juntar com base mestre
    print("\n[4/4] Juntando com base mestre...")
    # CORRECAO: Converter best_match_index para inteiro antes do merge
    df_final_matched['best_match_index'] = pd.to_numeric(
        df_final_matched['best_match_index'], 
        errors='coerce'
    ).astype('Int64')  # Nullable integer type
    
    df_resultado = df_final_matched.merge(
        df_master,
        left_on='best_match_index',
        right_index=True,
        how='left',
        suffixes=('_ENTRADA', '_MATCH')
    )
    
    # Estatisticas
    print("\n" + "="*80)
    print("ESTATISTICAS DO MATCHING")
    print("="*80)
    
    status_counts = df_resultado['match_status'].value_counts()
    for status, count in status_counts.items():
        pct = (count / len(df_resultado)) * 100
        print(f"  {status}: {count:,} ({pct:.1f}%)")
    
    matched = df_resultado[df_resultado['match_status'] == 'Match encontrado']
    if len(matched) > 0:
        score_medio = matched['match_score'].mean()
        print(f"\n  Score medio dos matches: {score_medio:.3f}")
    
    return df_resultado


# ==============================================================================
#      EXPORTACAO
# ==============================================================================

def exportar_resultado(df_resultado):
    """Exporta o resultado do matching."""
    print("\n" + "="*80)
    print("EXPORTANDO RESULTADO")
    print("="*80)
    
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(OUTPUT_ZIP, 'w', zipfile.ZIP_DEFLATED) as z:
        csv_buffer = io.StringIO()
        df_resultado.to_csv(csv_buffer, sep=';', index=False)
        z.writestr('df_etapa15_resultado_matching_hibrido.csv', csv_buffer.getvalue())
    
    tamanho = OUTPUT_ZIP.stat().st_size / (1024 * 1024)
    print(f"[OK] Exportado: {OUTPUT_ZIP.name}")
    print(f"  -> {len(df_resultado):,} registros, {tamanho:.2f} MB")


# ==============================================================================
#      FUNCAO PRINCIPAL
# ==============================================================================

def processar_matching_hibrido():
    """Orquestra toda a etapa 15."""
    print("\n" + "="*80)
    print("ETAPA 15: MATCHING HIBRIDO PONDERADO")
    print("="*80)
    
    inicio = time.time()
    
    try:
        # 1. Carregar dados
        df_entrada = carregar_dados_etapa14()
        df_master = carregar_base_mestre()
        
        # 2. Preparar dados
        df_master_prep, prod_index, pa_index = preparar_base_mestre(df_master)
        df_entrada_prep = preparar_entrada(df_entrada)
        
        # 3. Executar matching
        df_resultado = executar_matching(df_entrada_prep, df_master_prep, prod_index, pa_index)
        
        # 4. Exportar
        exportar_resultado(df_resultado)
        
        duracao = time.time() - inicio
        print("\n" + "="*80)
        print(f"[SUCESSO] ETAPA 15 CONCLUIDA EM {duracao:.1f}s")
        print("="*80)
        
        return df_resultado
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"[ERRO] Falha na Etapa 15: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        return None


# ==============================================================================
#      EXECUCAO
# ==============================================================================

if __name__ == "__main__":
    df_resultado = processar_matching_hibrido()
    
    if df_resultado is not None:
        print(f"\n[OK] DataFrame final disponivel com {len(df_resultado):,} registros")
        print(f"[OK] Colunas: {len(df_resultado.columns)}")
