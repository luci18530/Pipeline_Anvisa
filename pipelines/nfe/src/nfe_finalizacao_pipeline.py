# -*- coding: utf-8 -*-
"""
ETAPA 16: FINALIZACAO DO PIPELINE NFe

Finaliza o processamento separando:
1. df_matched_hibrido: Registros com match bem-sucedido
2. df_restante: Registros sem match (para analise manual)
3. df_atributos_ia: Tabela auxiliar com atributos extraidos pela IA

Input:  df_etapa15_resultado_matching_hibrido.zip
Output: df_etapa16_matched_hibrido.zip
        df_etapa16_restante.zip
        df_etapa16_atributos_ia.zip
"""

import pandas as pd
import zipfile
import os
import time
import io
from pathlib import Path
import sys
from paths import DATA_DIR

# Adicionar path do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

# ==============================================================================
#      CONFIGURACOES
# ==============================================================================

# Caminhos
PROCESSED_DIR = DATA_DIR / 'processed'
INPUT_ZIP = PROCESSED_DIR / 'df_etapa15_resultado_matching_hibrido.zip'

OUTPUT_MATCHED = PROCESSED_DIR / 'df_etapa16_matched_hibrido.zip'
OUTPUT_RESTANTE = PROCESSED_DIR / 'df_etapa16_restante.zip'
OUTPUT_ATRIBUTOS_IA = PROCESSED_DIR / 'df_etapa16_atributos_ia.zip'

# Colunas da IA para extrair em tabela separada
COLUNAS_IA = [
    'chave_codigo',  # Chave para vincular de volta
    'IA_PRODUTO',
    'IA_LABORATORIO',
    'IA_TIPO DA UNIDADE',
    'IA_QUANTIDADE MG (POR UNIDADE/ML)',
    'IA_QUANTIDADE ML',
    'IA_QUANTIDADE UI',
    'IA_QUANTIDADE UNIDADES'
]

# Colunas de trabalho para remover do DataFrame final
COLUNAS_TRABALHO = [
    'best_match_index',
    'match_score',
    'match_status',
    'NOME_PRODUTO',  # Redundante
    # Colunas da IA (serao extraidas separadamente)
    'IA_PRODUTO',
    'IA_LABORATORIO',
    'IA_TIPO DA UNIDADE',
    'IA_QUANTIDADE MG (POR UNIDADE/ML)',
    'IA_QUANTIDADE ML',
    'IA_QUANTIDADE UI',
    'IA_QUANTIDADE UNIDADES'
]


# ==============================================================================
#      CARREGAMENTO
# ==============================================================================

def carregar_dados_etapa15():
    """Carrega o DataFrame da etapa 15."""
    print("\n" + "="*80)
    print("CARREGANDO DADOS DA ETAPA 15")
    print("="*80)
    
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {INPUT_ZIP}")
    
    with zipfile.ZipFile(INPUT_ZIP, 'r') as z:
        csv_name = 'df_etapa15_resultado_matching_hibrido.csv'
        with z.open(csv_name) as f:
            df = pd.read_csv(f, sep=';')
    
    print(f"[OK] Carregado: {len(df):,} registros, {len(df.columns)} colunas")
    
    return df


# ==============================================================================
#      EXTRACAO DE ATRIBUTOS DA IA
# ==============================================================================

def extrair_atributos_ia(df):
    """Extrai atributos da IA para tabela separada."""
    print("\n" + "="*80)
    print("EXTRAINDO ATRIBUTOS DA IA")
    print("="*80)
    
    # Selecionar apenas colunas existentes
    colunas_existentes = [col for col in COLUNAS_IA if col in df.columns]
    
    if not colunas_existentes:
        print("[AVISO] Nenhuma coluna da IA encontrada")
        return pd.DataFrame()
    
    # Criar DataFrame de atributos
    df_ia = df[colunas_existentes].copy()
    
    # Remover linhas onde todos os atributos (exceto chave) sao nulos
    colunas_valores = [c for c in colunas_existentes if c != 'chave_codigo']
    if colunas_valores:
        antes = len(df_ia)
        df_ia = df_ia.dropna(subset=colunas_valores, how='all')
        removidos = antes - len(df_ia)
        
        print(f"[OK] Atributos extraidos: {len(df_ia):,} registros")
        if removidos > 0:
            print(f"  -> Removidos {removidos:,} registros vazios")
    
    return df_ia


# ==============================================================================
#      LIMPEZA DO DATAFRAME PRINCIPAL
# ==============================================================================

def limpar_dataframe_principal(df):
    """Remove colunas de trabalho do DataFrame principal."""
    print("\n" + "="*80)
    print("LIMPANDO DATAFRAME PRINCIPAL")
    print("="*80)
    
    # Remover apenas colunas que existem
    colunas_remover = [col for col in COLUNAS_TRABALHO if col in df.columns]
    
    if colunas_remover:
        print(f"[1/1] Removendo {len(colunas_remover)} colunas de trabalho...")
        df_limpo = df.drop(columns=colunas_remover)
    else:
        df_limpo = df.copy()
    
    print(f"[OK] DataFrame limpo: {len(df_limpo.columns)} colunas restantes")
    
    return df_limpo


# ==============================================================================
#      PARTICAO DOS RESULTADOS
# ==============================================================================

def particionar_resultados(df):
    """Particiona DataFrame em matched e restante."""
    print("\n" + "="*80)
    print("PARTICIONANDO RESULTADOS")
    print("="*80)
    
    # Verificar se coluna PRODUTO existe (vem do match)
    if 'PRODUTO' not in df.columns:
        print("[AVISO] Coluna PRODUTO nao encontrada - tratando todos como sem match")
        return pd.DataFrame(), df.copy()
    
    # Criar mascara: match bem-sucedido tem PRODUTO preenchido
    mask_matched = df['PRODUTO'].notna()
    
    df_matched = df[mask_matched].copy()
    df_restante = df[~mask_matched].copy()
    
    total = len(df)
    n_matched = len(df_matched)
    n_restante = len(df_restante)
    
    pct_matched = (n_matched / total * 100) if total > 0 else 0
    pct_restante = (n_restante / total * 100) if total > 0 else 0
    
    print(f"[OK] Particao concluida:")
    print(f"  -> Matched:  {n_matched:,} registros ({pct_matched:.1f}%)")
    print(f"  -> Restante: {n_restante:,} registros ({pct_restante:.1f}%)")
    
    return df_matched, df_restante


# ==============================================================================
#      EXPORTACAO
# ==============================================================================

def exportar_dataframe(df, output_path, csv_name):
    """Exporta um DataFrame para ZIP."""
    if df.empty:
        print(f"[AVISO] DataFrame vazio - pulando {csv_name}")
        return
    
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as z:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, sep=';', index=False)
        z.writestr(csv_name, csv_buffer.getvalue())
    
    tamanho = output_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Exportado: {output_path.name}")
    print(f"  -> {len(df):,} registros, {len(df.columns)} colunas, {tamanho:.2f} MB")


def exportar_resultados(df_matched, df_restante, df_ia):
    """Exporta todos os resultados da etapa 16."""
    print("\n" + "="*80)
    print("EXPORTANDO RESULTADOS")
    print("="*80)
    
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Matched
    if not df_matched.empty:
        print("\n[1/3] Exportando registros com match...")
        exportar_dataframe(
            df_matched,
            OUTPUT_MATCHED,
            'df_etapa16_matched_hibrido.csv'
        )
    else:
        print("\n[1/3] Nenhum registro com match para exportar")
    
    # 2. Restante
    if not df_restante.empty:
        print("\n[2/3] Exportando registros sem match...")
        exportar_dataframe(
            df_restante,
            OUTPUT_RESTANTE,
            'df_etapa16_restante.csv'
        )
    else:
        print("\n[2/3] Nenhum registro restante para exportar")
    
    # 3. Atributos IA
    if not df_ia.empty:
        print("\n[3/3] Exportando atributos da IA...")
        exportar_dataframe(
            df_ia,
            OUTPUT_ATRIBUTOS_IA,
            'df_etapa16_atributos_ia.csv'
        )
    else:
        print("\n[3/3] Nenhum atributo da IA para exportar")


# ==============================================================================
#      RELATORIO FINAL
# ==============================================================================

def gerar_relatorio(df_original, df_matched, df_restante, df_ia):
    """Gera relatorio final da etapa 16."""
    print("\n" + "="*80)
    print("RELATORIO FINAL - ETAPA 16")
    print("="*80)
    
    total = len(df_original)
    n_matched = len(df_matched)
    n_restante = len(df_restante)
    n_ia = len(df_ia)
    
    print(f"\nTotal processado: {total:,} registros")
    print(f"\nResultados:")
    print(f"  1. Matched (com correspondencia ANVISA):")
    print(f"     -> {n_matched:,} registros ({n_matched/total*100:.1f}%)")
    print(f"  2. Restante (sem correspondencia):")
    print(f"     -> {n_restante:,} registros ({n_restante/total*100:.1f}%)")
    print(f"  3. Atributos IA (tabela auxiliar):")
    print(f"     -> {n_ia:,} registros")
    
    if n_matched > 0 and 'match_score' in df_original.columns:
        df_with_score = df_original[df_original['match_score'].notna()]
        if len(df_with_score) > 0:
            score_medio = df_with_score['match_score'].mean()
            score_min = df_with_score['match_score'].min()
            score_max = df_with_score['match_score'].max()
            
            print(f"\nQualidade dos Matches:")
            print(f"  Score medio: {score_medio:.3f}")
            print(f"  Score minimo: {score_min:.3f}")
            print(f"  Score maximo: {score_max:.3f}")
    
    print("\nArquivos gerados:")
    if OUTPUT_MATCHED.exists():
        print(f"  ✓ {OUTPUT_MATCHED.name}")
    if OUTPUT_RESTANTE.exists():
        print(f"  ✓ {OUTPUT_RESTANTE.name}")
    if OUTPUT_ATRIBUTOS_IA.exists():
        print(f"  ✓ {OUTPUT_ATRIBUTOS_IA.name}")


# ==============================================================================
#      FUNCAO PRINCIPAL
# ==============================================================================

def processar_finalizacao():
    """Orquestra toda a etapa 16."""
    print("\n" + "="*80)
    print("ETAPA 16: FINALIZACAO DO PIPELINE NFe")
    print("="*80)
    
    inicio = time.time()
    
    try:
        # 1. Carregar dados
        df_original = carregar_dados_etapa15()
        
        # 2. Extrair atributos IA
        df_ia = extrair_atributos_ia(df_original)
        
        # 3. Limpar DataFrame principal
        df_limpo = limpar_dataframe_principal(df_original)
        
        # 4. Particionar resultados
        df_matched, df_restante = particionar_resultados(df_limpo)
        
        # 5. Exportar tudo
        exportar_resultados(df_matched, df_restante, df_ia)
        
        # 6. Gerar relatorio
        gerar_relatorio(df_original, df_matched, df_restante, df_ia)
        
        duracao = time.time() - inicio
        print("\n" + "="*80)
        print(f"[SUCESSO] ETAPA 16 CONCLUIDA EM {duracao:.1f}s")
        print("="*80)
        
        return df_matched, df_restante, df_ia
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"[ERRO] Falha na Etapa 16: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        return None, None, None


# ==============================================================================
#      EXECUCAO
# ==============================================================================

if __name__ == "__main__":
    df_matched, df_restante, df_ia = processar_finalizacao()
    
    if df_matched is not None:
        print(f"\n✓ Pipeline finalizado com sucesso")
        print(f"✓ Matched: {len(df_matched):,} registros")
        print(f"✓ Restante: {len(df_restante):,} registros")
        print(f"✓ Atributos IA: {len(df_ia):,} registros")
