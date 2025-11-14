"""
NFe Unificacao e Matching Final
================================

Etapa 12: Unifica bases mestre (ANVISA + Manual), identifica itens sem correspondência,
aplica regras específicas e filtra produtos não medicinais.

Fluxo:
1. Carrega base manual (Google Sheets)
2. Normaliza e unifica com base ANVISA
3. Identifica produtos sem match
4. Aplica regras específicas (Enoxaparina, etc)
5. Reaplicação do dicionário fuzzy
6. Filtro final de produtos não medicinais
7. Reaplicação fuzzy pós-filtro
8. Exporta resultados

Autor: Pipeline Anvisa
Data: 2025-11-14
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import json
from datetime import datetime
from tqdm import tqdm

# Configuração de caminhos
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
SUPPORT_DIR = BASE_DIR / 'support'


def carregar_recursos_unificacao():
    """
    Carrega recursos necessários para unificação:
    - Base ANVISA (CSV ou Parquet)
    - Base Manual (Google Sheets)
    - Dicionário fuzzy
    """
    print("\n[INFO] Carregando recursos para unificacao...")
    recursos = {}
    
    # 1. Base ANVISA (output/baseANVISA.csv)
    try:
        anvisa_output_path = BASE_DIR / 'output' / 'baseANVISA.csv'
        parquet_path = DATA_DIR / 'anvisa' / 'dados_anvisa.parquet'
        csv_path = DATA_DIR / 'anvisa' / 'TA_PRECO_MEDICAMENTO.csv'
        
        if anvisa_output_path.exists():
            df_anvisa = pd.read_csv(anvisa_output_path, sep='\t', encoding='utf-8', low_memory=False)
            print(f"[OK] Base ANVISA (output/baseANVISA.csv): {len(df_anvisa):,} registros, {df_anvisa['PRODUTO'].nunique():,} produtos unicos")
        elif parquet_path.exists():
            df_anvisa = pd.read_parquet(parquet_path)
            print(f"[OK] Base ANVISA (Parquet): {len(df_anvisa):,} registros")
        elif csv_path.exists():
            df_anvisa = pd.read_csv(csv_path, sep=';', encoding='latin1', low_memory=False)
            print(f"[OK] Base ANVISA (CSV): {len(df_anvisa):,} registros")
        else:
            print("[AVISO] Base ANVISA nao encontrada. Continuando sem ela.")
            df_anvisa = None
        
        recursos['df_anvisa'] = df_anvisa
    except Exception as e:
        print(f"[ERRO] Falha ao carregar base ANVISA: {e}")
        recursos['df_anvisa'] = None
    
    # 2. Base Manual (Google Sheets)
    try:
        url_manual = "https://docs.google.com/spreadsheets/d/1X4SvEpQkjIa306IUUZUebNSwjqTJTo1e/export?format=xlsx"
        print("[INFO] Baixando base manual do Google Sheets...")
        df_manual = pd.read_excel(url_manual)
        print(f"[OK] Base Manual: {len(df_manual)} registros")
        recursos['df_manual'] = df_manual
    except Exception as e:
        print(f"[ERRO] Falha ao carregar base manual: {e}")
        recursos['df_manual'] = None
    
    # 3. Dicionário fuzzy
    fuzzy_path = SUPPORT_DIR / 'fuzzy_matches.json'
    if fuzzy_path.exists():
        with open(fuzzy_path, 'r', encoding='utf-8') as f:
            fuzzy_dict = json.load(f)
        print(f"[OK] Dicionario fuzzy: {len(fuzzy_dict)} mapeamentos")
        recursos['fuzzy_dict'] = fuzzy_dict
    else:
        print("[AVISO] fuzzy_matches.json nao encontrado")
        recursos['fuzzy_dict'] = {}
    
    return recursos


def normalizar_colunas(df):
    """
    Normaliza nomes de colunas: remove acentos, converte para maiúsculas.
    """
    df = df.copy()
    df.columns = (
        df.columns.str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.upper()
        .str.strip()
    )
    return df


def criar_base_master_unificada(df_anvisa, df_manual):
    """
    Cria base mestre unificada combinando ANVISA + Manual.
    Retorna set de produtos únicos.
    """
    print("\n[INFO] Criando base mestre unificada...")
    
    required_cols = ['PRODUTO', 'PRINCIPIO ATIVO']
    df_master_list = []
    
    # Normalizar ANVISA
    if df_anvisa is not None:
        df_anvisa_norm = normalizar_colunas(df_anvisa)
        if all(c in df_anvisa_norm.columns for c in required_cols):
            df_master_list.append(df_anvisa_norm[required_cols])
            print(f"[OK] Base ANVISA adicionada")
    
    # Normalizar Manual
    if df_manual is not None:
        df_manual_norm = normalizar_colunas(df_manual)
        if all(c in df_manual_norm.columns for c in required_cols):
            df_master_list.append(df_manual_norm[required_cols])
            print(f"[OK] Base Manual adicionada")
    
    if not df_master_list:
        raise ValueError("Nenhuma base mestre valida encontrada!")
    
    # Unificar e deduplicar
    df_master_unificado = (
        pd.concat(df_master_list, ignore_index=True)
        .dropna(subset=['PRODUTO'])
        .drop_duplicates(subset=['PRODUTO'])
        .reset_index(drop=True)
    )
    
    set_produtos = set(df_master_unificado['PRODUTO'])
    print(f"[OK] Base mestre unificada: {len(set_produtos)} produtos unicos")
    
    return set_produtos, df_master_unificado


def verificar_matches(df, set_master, coluna, step_name="verificacao"):
    """
    Verifica quantos produtos têm match com base master.
    """
    matched = df[coluna].isin(set_master).sum()
    total = len(df)
    percent = (matched / total * 100) if total > 0 else 0
    print(f"  [{step_name}] Matches: {matched}/{total} ({percent:.2f}%)")
    return matched


def aplicar_regras_especificas(df, set_master):
    """
    Aplica regras específicas baseadas em padrões na descrição.
    Exemplo: ENOXAPARINA + CRISTALIA -> HEPARINOX
    """
    print("\n[INFO] Aplicando regras especificas...")
    
    mask_nao_match = ~df['NOME_PRODUTO_LIMPO'].isin(set_master)
    total_antes = mask_nao_match.sum()
    print(f"[INFO] Produtos sem match: {total_antes}")
    
    # Lista de regras: (padrões, produto)
    regras = [
        (["ENOXAPARINA", "CRISTALIA"], "HEPARINOX"),
        (["ENOXAPARINA", "BLAU"], "ENOXALOW"),
        (["ENOXAPARINA", "MYLAN"], "HEPTRIS"),
        (["ENOXAPARINA", "EURO"], "VERSA"),
        (["HEPTRIS"], "HEPTRIS"),
        (["ACALABRUTINIBE"], "CALQUENCE"),
    ]
    
    alteracoes = 0
    for padroes, nome_produto in regras:
        cond = mask_nao_match.copy()
        for padrao in padroes:
            cond &= df['descricao_produto'].str.contains(padrao, case=False, na=False)
        
        count = cond.sum()
        if count > 0:
            df.loc[cond, 'NOME_PRODUTO_LIMPO'] = nome_produto
            alteracoes += count
            print(f"  [OK] Regra {padroes} -> {nome_produto}: {count} alteracoes")
    
    print(f"[OK] Total de alteracoes por regras: {alteracoes}")
    verificar_matches(df, set_master, 'NOME_PRODUTO_LIMPO', "Apos Regras")
    
    return df


def aplicar_fuzzy_dict(df, set_master, fuzzy_dict, step_name="Fuzzy"):
    """
    Aplica dicionário de correções fuzzy nos produtos sem match.
    """
    if not fuzzy_dict:
        print(f"[AVISO] Dicionario fuzzy vazio. Pulando {step_name}.")
        return df
    
    mask_nao_match = ~df['NOME_PRODUTO_LIMPO'].isin(set_master)
    total_antes = mask_nao_match.sum()
    print(f"\n[INFO] Aplicando {step_name} em {total_antes} produtos...")
    
    df.loc[mask_nao_match, 'NOME_PRODUTO_LIMPO'] = (
        df.loc[mask_nao_match, 'NOME_PRODUTO_LIMPO'].replace(fuzzy_dict)
    )
    
    verificar_matches(df, set_master, 'NOME_PRODUTO_LIMPO', f"Apos {step_name}")
    return df


def filtrar_nao_medicinais_final(df):
    """
    Remove produtos claramente não medicinais (óleo mineral, livros, equipamentos, etc).
    """
    print("\n[INFO] Filtrando produtos nao medicinais (lista final)...")
    
    total_antes = len(df)
    
    # Padrões de produtos não medicinais
    padroes_nao_medicinais = [
        "OLEO MINERAL", "ULTRASSONOGRAFIA", "DIAGNOSTICOS", "ESPIROMETRIA",
        "ENDOTRAQUEAIS", "ULTRASOM", "LOCACAO", "SUPORTE", "AGUA OXIGENADA",
        "LIMPEZA", "LIVRO", "SHERLOCK", "ELETROCARDIOGRAMA", "ELETROCARDIOGRAFO",
        "VASELINA", "DELETAR", "PVPI ", "ALVEJANTE", "LIDOVET", "DOXINEW",
        "ZELOTRIL ", "VITAKA ", "QUINOLON ", "IVERMIN ", "DEXACORT", "BIOXAN COMPOSTO"
    ]
    
    # Criar regex pattern
    pattern = "|".join(padroes_nao_medicinais)
    
    # Filtrar
    df = df[
        ~df['NOME_PRODUTO_LIMPO'].str.contains(pattern, case=False, na=False)
    ].copy()
    
    total_depois = len(df)
    removidos = total_antes - total_depois
    percent = (removidos / total_antes * 100) if total_antes > 0 else 0
    
    print(f"[OK] Produtos removidos: {removidos} ({percent:.2f}%)")
    print(f"[OK] Produtos restantes: {total_depois}")
    
    return df


def gerar_relatorio_final(df, set_master):
    """
    Gera relatório de produtos sem correspondência final.
    """
    print("\n" + "="*80)
    print("RELATORIO FINAL - PRODUTOS SEM CORRESPONDENCIA")
    print("="*80)
    
    no_match_df = df[~df['NOME_PRODUTO_LIMPO'].isin(set_master)].copy()
    
    if no_match_df.empty:
        print("\n[SUCESSO] Todos os produtos encontraram correspondencia na base unificada!")
        return no_match_df
    
    total_trabalhando = len(df)
    total_no_match = len(no_match_df)
    percent = (total_no_match / total_trabalhando * 100) if total_trabalhando > 0 else 0
    
    print(f"\nTotal de produtos em analise: {total_trabalhando}")
    print(f"Produtos SEM correspondencia: {total_no_match} ({percent:.2f}%)")
    
    # Top produtos sem match
    print("\n--- Nomes Mais Comuns Sem Correspondencia ---")
    top_nomes = no_match_df['NOME_PRODUTO_LIMPO'].value_counts().head(10)
    for nome, count in top_nomes.items():
        print(f"  {nome}: {count} ocorrencias")
    
    # Por valor total
    if 'valor_produtos' in no_match_df.columns:
        no_match_df['valor_produtos'] = pd.to_numeric(
            no_match_df['valor_produtos'], errors='coerce'
        ).fillna(0)
        
        print("\n--- Top 5 por Valor Total ---")
        aggregated = (
            no_match_df.groupby('NOME_PRODUTO_LIMPO')
            .agg(
                contagem=('NOME_PRODUTO_LIMPO', 'size'),
                valor_total=('valor_produtos', 'sum')
            )
            .sort_values(by='valor_total', ascending=False)
            .reset_index()
            .head(5)
        )
        
        for _, row in aggregated.iterrows():
            print(f"  {row['NOME_PRODUTO_LIMPO']}: {row['contagem']} itens - R$ {row['valor_total']:,.2f}")
        
        soma_total = no_match_df['valor_produtos'].sum()
        print(f"\nValor total sem correspondencia: R$ {soma_total:,.2f}")
    
    return no_match_df


def exportar_zip_fast(df, prefixo='df_final_trabalhando'):
    """
    Exporta DataFrame para ZIP (CSV comprimido).
    """
    filename = f"{prefixo}.zip"
    output_path = DATA_DIR / 'processed' / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    compression_opts = dict(
        method='zip',
        archive_name=f"{prefixo}.csv"
    )
    
    df.to_csv(
        output_path,
        sep=';',
        index=False,
        encoding='utf-8-sig',
        compression=compression_opts
    )
    
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Arquivo salvo: {filename} ({size_mb:.2f} MB)")
    
    return output_path


def processar_unificacao_matching():
    """
    Função principal: executa toda a etapa 12.
    """
    print("\n" + "="*80)
    print("ETAPA 12: UNIFICACAO DE BASES MESTRE E MATCHING FINAL")
    print("="*80)
    
    # 1. Carregar recursos
    recursos = carregar_recursos_unificacao()
    df_anvisa = recursos['df_anvisa']
    df_manual = recursos['df_manual']
    fuzzy_dict = recursos['fuzzy_dict']
    
    # 2. Criar base master unificada
    set_master, df_master = criar_base_master_unificada(df_anvisa, df_manual)
    
    # 3. Carregar df_trabalhando_refinado
    print("\n[INFO] Carregando df_trabalhando_refinado...")
    processed_dir = DATA_DIR / 'processed'
    
    # MODIFICADO: Busca arquivo SEM timestamp (overwriting)
    arquivo_path = processed_dir / 'df_etapa11_trabalhando_refinado.zip'
    
    if not arquivo_path.exists():
        # Fallback: procura por arquivos com timestamp (compatibilidade)
        zip_files = sorted(processed_dir.glob('df_trabalhando_refinado_*.zip'))
        if not zip_files:
            raise FileNotFoundError("Nenhum arquivo df_trabalhando_refinado encontrado!")
        arquivo_path = zip_files[-1]
        print(f"[INFO] Usando arquivo legado: {arquivo_path.name}")
    
    latest_zip = arquivo_path
    print(f"[INFO] Carregando: {latest_zip.name}")
    
    # Ler CSV de dentro do ZIP (sep=';' conforme salvo no refinamento)
    import zipfile
    with zipfile.ZipFile(latest_zip, 'r') as zip_ref:
        csv_name = zip_ref.namelist()[0]  # Pega o primeiro (único) CSV
        with zip_ref.open(csv_name) as f:
            df = pd.read_csv(f, sep=';')
    
    print(f"   [OK] Carregado com sucesso!")
    print(f"   Shape: {df.shape}")
    
    # Verificar colunas necessárias
    if 'NOME_PRODUTO_LIMPO' not in df.columns:
        raise ValueError("Coluna 'NOME_PRODUTO_LIMPO' nao encontrada!")
    if 'descricao_produto' not in df.columns:
        raise ValueError("Coluna 'descricao_produto' nao encontrada!")
    
    # 4. Baseline inicial
    print("\n" + "="*80)
    print("BASELINE INICIAL")
    print("="*80)
    verificar_matches(df, set_master, 'NOME_PRODUTO_LIMPO', "Baseline")
    
    # 5. Aplicar regras específicas
    print("\n" + "="*80)
    print("APLICANDO REGRAS ESPECIFICAS")
    print("="*80)
    df = aplicar_regras_especificas(df, set_master)
    
    # 6. Primeira aplicação fuzzy
    print("\n" + "="*80)
    print("PRIMEIRA APLICACAO FUZZY")
    print("="*80)
    df = aplicar_fuzzy_dict(df, set_master, fuzzy_dict, "Fuzzy 1")
    
    # 7. Filtrar não medicinais
    print("\n" + "="*80)
    print("FILTRAGEM DE NAO MEDICINAIS")
    print("="*80)
    df = filtrar_nao_medicinais_final(df)
    
    # 8. Segunda aplicação fuzzy (pós-filtro)
    print("\n" + "="*80)
    print("SEGUNDA APLICACAO FUZZY (POS-FILTRO)")
    print("="*80)
    df = aplicar_fuzzy_dict(df, set_master, fuzzy_dict, "Fuzzy 2")
    
    # 9. Relatório final
    no_match_df = gerar_relatorio_final(df, set_master)
    
    # 10. Exportar resultados
    print("\n" + "="*80)
    print("EXPORTANDO RESULTADOS")
    print("="*80)
    
    output_path = exportar_zip_fast(df, 'df_etapa12_final_trabalhando')
    
    if not no_match_df.empty:
        no_match_path = exportar_zip_fast(no_match_df, 'df_etapa12_no_match')
    
    print("\n" + "="*80)
    print("[SUCESSO] ETAPA 12 CONCLUIDA!")
    print("="*80)
    
    return output_path


if __name__ == '__main__':
    try:
        processar_unificacao_matching()
    except Exception as e:
        print(f"\n[ERRO] Falha na execucao: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
