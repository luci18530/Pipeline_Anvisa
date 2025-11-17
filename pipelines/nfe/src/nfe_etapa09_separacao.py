"""
Módulo: nfe_etapa09_separacao.py
Descrição: Separa o DataFrame em dois fluxos (completo vs trabalhando) e
           filtra itens não-medicinais do fluxo de trabalho.
Autor: Pipeline ANVISA
Data: 2025-11-13
"""

import pandas as pd
import numpy as np
import json
import re
import gc
import os
from pathlib import Path
from datetime import datetime

from paths import SUPPORT_DIR

# ============================================================
# FUNÇÕES DE CARREGAMENTO
# ============================================================

def carregar_json_local(caminho_arquivo: str) -> dict:
    """
    Carrega arquivo JSON local.
    
    Args:
        caminho_arquivo: Caminho completo para o arquivo JSON
        
    Returns:
        Dicionário com conteúdo do JSON ou dict vazio em caso de erro
    """
    caminho = Path(caminho_arquivo)
    try:
        if not caminho.exists():
            print(f"[AVISO] Arquivo não encontrado: {caminho}")
            return {}
            
        with caminho.open("r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"[OK] Arquivo '{caminho.name}' carregado com sucesso.")
        return data
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"[AVISO] Erro ao carregar '{caminho_arquivo}': {e}")
        return {}


# ============================================================
# FUNÇÕES DE SEPARAÇÃO
# ============================================================

def separar_fluxos(df: pd.DataFrame) -> tuple:
    """
    Separa DataFrame em dois fluxos baseado na coluna PRODUTO.
    
    Args:
        df: DataFrame com coluna PRODUTO
        
    Returns:
        Tupla (df_completo, df_trabalhando)
        - df_completo: Registros onde PRODUTO não é nulo (matching bem-sucedido)
        - df_trabalhando: Registros onde PRODUTO é nulo (requer trabalho adicional)
    """
    print("\n" + "="*80)
    print("SEPARANDO FLUXOS: COMPLETO vs TRABALHANDO")
    print("="*80)
    
    # Validação
    if 'PRODUTO' not in df.columns:
        print("[ERRO] Erro: A coluna 'PRODUTO' não foi encontrada no DataFrame.")
        return None, None
    
    # Separação
    df_completo = df[df['PRODUTO'].notna()].copy()
    df_trabalhando = df[df['PRODUTO'].isna()].copy()
    
    # Estatísticas
    total = len(df)
    n_completo = len(df_completo)
    n_trabalhando = len(df_trabalhando)
    
    print(f"\n[INFO] Estatísticas da Separação:")
    print(f"   Total de registros:        {total:,}")
    print(f"   [OK] df_completo (matched):   {n_completo:,} ({n_completo/total*100:.2f}%)")
    print(f"   [AVISO]  df_trabalhando (unmatched): {n_trabalhando:,} ({n_trabalhando/total*100:.2f}%)")
    print(f"\n   Shape df_completo:     {df_completo.shape}")
    print(f"   Shape df_trabalhando:  {df_trabalhando.shape}")
    
    return df_completo, df_trabalhando


# ============================================================
# FUNÇÕES DE FILTRAGEM
# ============================================================

def filtrar_nao_medicinais(
    df_trabalhando: pd.DataFrame,
    caminho_palavras: str = str(SUPPORT_DIR / "palavras_remocao.json"),
    caminho_termos: str = str(SUPPORT_DIR / "termos_remocao.json"),
    coluna_descricao: str = "descricao_produto"
) -> pd.DataFrame:
    """
    Remove itens não-medicinais do df_trabalhando usando listas de palavras e termos.
    
    Args:
        df_trabalhando: DataFrame com registros não matcheados
        caminho_palavras: Caminho para JSON com palavras a remover
        caminho_termos: Caminho para JSON com termos/frases a remover
        coluna_descricao: Nome da coluna com descrição do produto
        
    Returns:
        DataFrame filtrado (sem itens não-medicinais)
    """
    print("\n" + "="*80)
    print("FILTRAGEM DE ITENS NÃO-MEDICINAIS")
    print("="*80)
    
    # Validações iniciais
    if df_trabalhando is None or df_trabalhando.empty:
        print("[AVISO] DataFrame 'df_trabalhando' está vazio ou não foi fornecido.")
        return df_trabalhando
    
    if coluna_descricao not in df_trabalhando.columns:
        print(f"[ERRO] Erro: Coluna '{coluna_descricao}' não encontrada no DataFrame.")
        return df_trabalhando
    
    # Carrega listas de filtros
    palavras_json = carregar_json_local(caminho_palavras)
    termos_json = carregar_json_local(caminho_termos)
    
    palavras_remover = palavras_json.get("palavras_a_remover", [])
    termos_remover = termos_json.get("termos_a_remover", [])
    
    print(f"\n[INFO] Listas de Filtros Carregadas:")
    print(f"   Palavras individuais: {len(palavras_remover)}")
    print(f"   Termos/frases:        {len(termos_remover)}")
    
    if not palavras_remover and not termos_remover:
        print("[AVISO] Nenhum filtro carregado. Retornando DataFrame original.")
        return df_trabalhando
    
    # Guarda estado inicial
    linhas_antes = len(df_trabalhando)
    print(f"\n[INFO] Aplicando Filtros...")
    print(f"   Shape inicial: {df_trabalhando.shape}")
    
    # Filtragem por PALAVRAS (palavras individuais)
    if palavras_remover:
        print(f"\n   [INFO] Filtrando por palavras individuais...")
        # Cria padrão com word boundaries para evitar matches parciais
        # Ex: "SORO" deve encontrar "SORO", mas não "DESORO"
        padrao_palavras = r'\b(' + '|'.join([re.escape(p) for p in palavras_remover]) + r')\b'
        
        mascara_palavras = df_trabalhando[coluna_descricao].str.contains(
            padrao_palavras,
            case=False,
            na=False,
            regex=True
        )
        
        removidas_palavras = mascara_palavras.sum()
        df_trabalhando = df_trabalhando[~mascara_palavras]
        print(f"      Linhas removidas: {removidas_palavras:,}")
    
    # Filtragem por TERMOS (frases completas)
    if termos_remover:
        print(f"\n   [INFO] Filtrando por termos/frases...")
        # Para termos, não usa word boundaries (permite match em qualquer posição)
        padrao_termos = '|'.join([re.escape(t) for t in termos_remover])
        
        mascara_termos = df_trabalhando[coluna_descricao].str.contains(
            padrao_termos,
            case=False,
            na=False,
            regex=True
        )
        
        removidas_termos = mascara_termos.sum()
        df_trabalhando = df_trabalhando[~mascara_termos]
        print(f"      Linhas removidas: {removidas_termos:,}")
    
    # Relatório final
    linhas_depois = len(df_trabalhando)
    total_removidas = linhas_antes - linhas_depois
    
    print(f"\n" + "="*80)
    print("[INFO] RESULTADO DA FILTRAGEM")
    print("="*80)
    print(f"   Linhas ANTES:     {linhas_antes:,}")
    print(f"   Linhas DEPOIS:    {linhas_depois:,}")
    print(f"   Total REMOVIDAS:  {total_removidas:,} ({total_removidas/linhas_antes*100:.2f}%)")
    print(f"   Shape final:      {df_trabalhando.shape}")
    print("="*80)
    
    return df_trabalhando


# ============================================================
# FUNÇÃO DE EXPORTAÇÃO
# ============================================================

def exportar_zip_fast(
    df: pd.DataFrame,
    nome: str,
    diretorio: str = "data/processed"
) -> str:
    """
    Exporta DataFrame para CSV comprimido (.zip).
    
    Args:
        df: DataFrame a exportar
        nome: Nome base do arquivo (sem extensão)
        diretorio: Diretório de destino
        
    Returns:
        Caminho completo do arquivo gerado
    """
    # Cria diretório se não existir
    os.makedirs(diretorio, exist_ok=True)
    
    # Gera nome SEM timestamp (usando overwriting)
    nome_arquivo = f"{nome.lower()}"
    caminho_zip = os.path.join(diretorio, f"{nome_arquivo}.zip")
    
    print(f"\n[INFO] Exportando: {nome}")
    print(f"   Registros: {len(df):,}")
    print(f"   Colunas:   {len(df.columns)}")
    
    # Exporta com compressão ZIP
    df.to_csv(
        caminho_zip,
        sep=';',
        index=False,
        encoding='utf-8-sig',
        compression={
            'method': 'zip',
            'archive_name': f"{nome_arquivo}.csv"
        }
    )
    
    # Mostra tamanho do arquivo
    tamanho_mb = os.path.getsize(caminho_zip) / (1024 * 1024)
    print(f"   Arquivo: {os.path.basename(caminho_zip)}")
    print(f"   Tamanho: {tamanho_mb:.2f} MB")
    print(f"   [OK] Exportação concluída!")
    
    return caminho_zip


# ============================================================
# FUNÇÃO PRINCIPAL
# ============================================================

def processar_separacao_e_filtragem(
    df: pd.DataFrame,
    exportar: bool = True,
    diretorio: str = "data/processed"
) -> tuple:
    """
    Processa separação em fluxos e filtragem de itens não-medicinais.
    
    Args:
        df: DataFrame resultado da Etapa 8 (nfe_matched_manual)
        exportar: Se True, exporta os DataFrames resultantes
        diretorio: Diretório para exportação
        
    Returns:
        Tupla (df_completo, df_trabalhando_filtrado)
    """
    print("\n" + "="*80)
    print("ETAPA 9: SEPARAÇÃO E FILTRAGEM")
    print("="*80)
    
    inicio = datetime.now()
    
    # Passo 1: Separar fluxos
    df_completo, df_trabalhando = separar_fluxos(df)
    
    if df_completo is None or df_trabalhando is None:
        print("[ERRO] Erro na separação de fluxos. Abortando.")
        return None, None
    
    # Passo 2: Filtrar itens não-medicinais
    df_trabalhando_filtrado = filtrar_nao_medicinais(df_trabalhando)
    
    # Passo 3: Exportar (opcional)
    if exportar:
        print("\n" + "="*80)
        print("EXPORTANDO RESULTADOS")
        print("="*80)
        
        arquivo_completo = exportar_zip_fast(df_completo, "DF_ETAPA09_COMPLETO", diretorio)
        arquivo_trabalhando = exportar_zip_fast(df_trabalhando_filtrado, "DF_ETAPA09_TRABALHANDO", diretorio)
    
    # Limpeza de memória
    del df_trabalhando
    gc.collect()
    
    # Tempo decorrido
    duracao = (datetime.now() - inicio).total_seconds()
    print(f"\n[INFO]  Tempo de execução: {duracao:.2f}s")
    print("="*80)
    
    return df_completo, df_trabalhando_filtrado


# ============================================================
# SCRIPT STANDALONE
# ============================================================

if __name__ == "__main__":
    import sys
    
    print("="*80)
    print("SCRIPT: Separação e Filtragem de NFe")
    print("="*80)
    
    # Procura arquivo mais recente nfe_matched_manual_*.csv
    diretorio_dados = "data/processed"
    arquivos = sorted([
        f for f in os.listdir(diretorio_dados)
        if f.startswith("nfe_matched_manual_") and f.endswith(".csv")
    ], reverse=True)
    
    if not arquivos:
        print("[ERRO] Erro: Nenhum arquivo 'nfe_matched_manual_*.csv' encontrado.")
        print("   Execute primeiro as Etapas 1-8 do pipeline.")
        sys.exit(1)
    
    arquivo_entrada = os.path.join(diretorio_dados, arquivos[0])
    print(f"\n📂 Arquivo de entrada: {os.path.basename(arquivo_entrada)}")
    
    # Carrega dados
    print(f"\n📖 Carregando dados...")
    df = pd.read_csv(arquivo_entrada, sep=';', encoding='utf-8-sig')
    print(f"   Shape: {df.shape}")
    
    # Processa separação e filtragem
    df_completo, df_trabalhando = processar_separacao_e_filtragem(
        df=df,
        exportar=True,
        diretorio=diretorio_dados
    )
    
    print("\n[OK] Processamento concluído!")

