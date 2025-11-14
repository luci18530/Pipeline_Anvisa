"""
NFe Matching de Apresentacao Unica
===================================

Etapa 13: Identifica produtos com apenas uma apresentação na base mestre (ANVISA+Manual)
e realiza join direto de alta confiança.

Fluxo:
1. Carrega base mestre completa (ANVISA + Manual) SEM deduplicar
2. Conta apresentações únicas por produto
3. Filtra produtos com exatamente 1 apresentação
4. Particiona df_trabalhando: candidatos vs não candidatos
5. Executa join direto nos candidatos
6. Exporta matches de alta confiança
7. Atualiza df_trabalhando apenas com não candidatos

Autor: Pipeline Anvisa
Data: 2025-11-14
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import json
import zipfile
from datetime import datetime
from tqdm import tqdm
import gc

# Importar utilitários de limpeza
sys.path.insert(0, str(Path(__file__).resolve().parent / 'modules'))
from utils_limpeza import merge_seguro, validar_integridade_colunas

# Configuração de caminhos
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
SUPPORT_DIR = BASE_DIR / 'support'


def carregar_schema_referencia(df_trabalhando):
    """
    Tenta carregar o schema oficial do df_etapa09_completo para garantir que
    os matches exportados tenham colunas idênticas às da base completa.
    Caso o arquivo não exista, utiliza as colunas atuais de df_trabalhando
    como referência.
    """
    referencia_path = DATA_DIR / 'processed' / 'df_etapa09_completo.zip'

    if referencia_path.exists():
        try:
            with zipfile.ZipFile(referencia_path, 'r') as zip_ref:
                csv_name = zip_ref.namelist()[0]
                with zip_ref.open(csv_name) as f:
                    df_header = pd.read_csv(f, sep=';', nrows=0)

            colunas_ref = df_header.columns.tolist()
            print(f"[INFO] Schema referencia (df_etapa09_completo): {len(colunas_ref)} colunas")
            return colunas_ref
        except Exception as exc:
            print(f"[AVISO] Falha ao carregar schema de df_etapa09_completo: {exc}")

    print("[AVISO] Schema referencia df_etapa09_completo indisponivel. Usando colunas de df_trabalhando.")
    return df_trabalhando.columns.tolist()


def alinhar_dataframe_para_schema(df, colunas_referencia, contexto="Match apresentacao unica"):
    """
    Remove colunas extras, adiciona colunas ausentes e reordena conforme
    colunas de referência, garantindo compatibilidade com df_completo.
    """
    if df.empty:
        return df

    df_alinhado = df.copy()

    colunas_extras = [col for col in df_alinhado.columns if col not in colunas_referencia]
    if colunas_extras:
        preview = ', '.join(colunas_extras[:5])
        if len(colunas_extras) > 5:
            preview += ', ...'
        print(f"[INFO] {contexto}: removendo colunas extras ({len(colunas_extras)}): {preview}")
        df_alinhado = df_alinhado.drop(columns=colunas_extras, errors='ignore')

    colunas_faltantes = [col for col in colunas_referencia if col not in df_alinhado.columns]
    if colunas_faltantes:
        preview = ', '.join(colunas_faltantes[:5])
        if len(colunas_faltantes) > 5:
            preview += ', ...'
        print(f"[INFO] {contexto}: adicionando colunas ausentes ({len(colunas_faltantes)}): {preview}")
        for coluna in colunas_faltantes:
            df_alinhado[coluna] = np.nan

    df_alinhado = df_alinhado[colunas_referencia]
    print(f"[OK] {contexto}: schema alinhado com {len(colunas_referencia)} colunas")

    return df_alinhado


def preparar_master_para_join(df_master_para_join):
    """
    Renomeia colunas da base mestre com sufixo _MASTER para evitar conflitos
    e permitir preenchimento posterior das colunas principais.
    """
    df_master = df_master_para_join.copy()
    rename_map = {}
    for coluna in df_master.columns:
        if coluna == 'PRODUTO':
            rename_map[coluna] = 'PRODUTO_MASTER'
        else:
            rename_map[coluna] = f"{coluna}_MASTER"
    return df_master.rename(columns=rename_map)

ALTERNATIVAS_COLUNAS_MASTER = {
    'APRESENTACAO': ['APRESENTACAO_ORIGINAL'],
    'APRESENTACAO_ORIGINAL': ['APRESENTACAO'],
    'PRINCIPIO ATIVO': ['PRINCIPIO_ATIVO'],
    'PRINCIPIO_ATIVO': ['PRINCIPIO ATIVO']
}


def _variacoes_nome_coluna(nome_base):
    """Gera variantes de um nome de coluna (com/sem espaços e underscores)."""
    variacoes = []
    candidatos = [
        nome_base,
        nome_base.replace('_', ' '),
        nome_base.replace('_', '').replace(' ', ''),
        nome_base.replace(' ', '_')
    ]
    for cand in candidatos:
        if cand and cand not in variacoes:
            variacoes.append(cand)
    return variacoes


def _candidatos_coluna_master(nome_base):
    bases = [nome_base]
    bases.extend(ALTERNATIVAS_COLUNAS_MASTER.get(nome_base, []))

    vistos = set()
    resultado = []
    for base in bases:
        for variacao in _variacoes_nome_coluna(base):
            candidato = f"{variacao}_MASTER"
            if candidato not in vistos:
                vistos.add(candidato)
                resultado.append(candidato)
    return resultado


def _normalizar_valores_placeholder(serie, coluna):
    """Transforma textos como 'SEM GTIN' ou zeros em NaN para permitir preenchimento."""
    if serie is None:
        return serie

    placeholders = {'', 'SEM GTIN', 'SEMGTIN', 'NAN', 'NONE', '0', '0000000000000'}
    if coluna.upper().startswith('EAN'):
        serie_limpa = serie.copy()

        def _clean(value):
            if pd.isna(value):
                return pd.NA
            if isinstance(value, (int, float)):
                if value == 0 or pd.isna(value):
                    return pd.NA
                return value
            texto = str(value).strip().upper()
            if texto in placeholders:
                return pd.NA
            return value

        serie_limpa = serie_limpa.apply(_clean)
        return serie_limpa

    return serie


def integrar_colunas_master(df, colunas_referencia):
    """
    Preenche as colunas do DataFrame com valores vindos da base mestre
    (colunas *_MASTER) e remove os sufixos auxiliares ao final.
    """
    if df.empty:
        return df

    df_resultado = df.copy()

    colunas_master_utilizadas = set()

    for coluna in colunas_referencia:
        candidatos = _candidatos_coluna_master(coluna)
        serie_master = pd.Series(pd.NA, index=df_resultado.index)
        candidatos_existentes = []

        for candidato in candidatos:
            if candidato in df_resultado.columns:
                serie_master = serie_master.combine_first(df_resultado[candidato])
                candidatos_existentes.append(candidato)

        if not candidatos_existentes:
            continue

        colunas_master_utilizadas.update(candidatos_existentes)

        if coluna in df_resultado.columns:
            base_series = _normalizar_valores_placeholder(df_resultado[coluna], coluna)
            df_resultado[coluna] = base_series.combine_first(serie_master)
        else:
            df_resultado[coluna] = serie_master

    colunas_master_restantes = [
        c for c in df_resultado.columns
        if c.endswith('_MASTER') and c not in colunas_master_utilizadas
    ]
    if colunas_master_restantes:
        df_resultado = df_resultado.drop(columns=colunas_master_restantes, errors='ignore')

    return df_resultado


def carregar_base_anvisa():
    """
    Carrega base ANVISA completa (todas as apresentações).
    """
    print("\n[INFO] Carregando base ANVISA...")
    
    # CORRIGIDO: Caminho correto para base ANVISA
    anvisa_output_path = OUTPUT_DIR / 'anvisa' / 'baseANVISA.csv'
    
    if not anvisa_output_path.exists():
        raise FileNotFoundError(f"Base ANVISA nao encontrada: {anvisa_output_path}")
    
    df_anvisa = pd.read_csv(anvisa_output_path, sep='\t', encoding='utf-8', low_memory=False)
    print(f"[OK] Base ANVISA: {len(df_anvisa):,} registros")
    
    return df_anvisa


def carregar_base_manual():
    """
    Carrega base manual do Google Sheets.
    """
    print("\n[INFO] Carregando base manual...")
    
    url_manual = "https://docs.google.com/spreadsheets/d/1X4SvEpQkjIa306IUUZUebNSwjqTJTo1e/export?format=xlsx"
    
    try:
        df_manual = pd.read_excel(url_manual)
        print(f"[OK] Base Manual: {len(df_manual)} registros")
        return df_manual
    except Exception as e:
        print(f"[ERRO] Falha ao carregar base manual: {e}")
        return None


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


def criar_base_mestre_completa(df_anvisa, df_manual):
    """
    Cria base mestre completa (ANVISA + Manual) SEM remover duplicatas.
    Cada linha representa uma apresentação única.
    
    IMPORTANTE: NÃO normaliza colunas - usa nomes originais da base!
    """
    print("\n[INFO] Criando base mestre completa (todas as apresentacoes)...")
    
    df_master_list = []
    
    # Base ANVISA (usar nomes originais - JÁ estão corretos!)
    if df_anvisa is not None:
        df_master_list.append(df_anvisa)
        print(f"[OK] Base ANVISA adicionada: {len(df_anvisa):,} linhas")
        print(f"     Colunas disponíveis: {list(df_anvisa.columns)[:5]}...")
    
    # Base Manual (normalizar apenas esta)
    if df_manual is not None:
        df_manual_norm = normalizar_colunas(df_manual)
        df_master_list.append(df_manual_norm)
        print(f"[OK] Base Manual adicionada: {len(df_manual_norm)} linhas")
    
    if not df_master_list:
        raise ValueError("Nenhuma base mestre disponivel!")
    
    # Concatenar TODAS as linhas (sem deduplicar)
    df_master_completo = pd.concat(df_master_list, ignore_index=True, sort=False)
    
    print(f"[OK] Base mestre completa: {len(df_master_completo):,} linhas (apresentacoes totais)")
    
    return df_master_completo


def identificar_produtos_apresentacao_unica(df_master_completo):
    """
    Identifica produtos que têm exatamente UMA apresentação única.
    """
    print("\n[INFO] Identificando produtos com apresentacao unica...")
    
    # Verificar qual coluna de apresentação existe
    col_apresentacao = None
    for col_nome in ['APRESENTACAO', 'APRESENTACAO_ORIGINAL']:
        if col_nome in df_master_completo.columns:
            col_apresentacao = col_nome
            print(f"[INFO] Usando coluna: {col_apresentacao}")
            break
    
    if col_apresentacao is None:
        print("[AVISO] Nenhuma coluna de apresentacao encontrada!")
        print("[INFO] Considerando cada registro como apresentacao unica...")
        # Fallback: considera cada linha como apresentação única
        contagem = df_master_completo.groupby('PRODUTO').size().reset_index(name='qtd_apresentacoes')
    else:
        # Contar apresentações únicas por produto
        contagem = (
            df_master_completo.groupby('PRODUTO')[col_apresentacao]
            .nunique()
            .reset_index(name='qtd_apresentacoes')
        )
    
    # Filtrar produtos com exatamente 1 apresentação
    produtos_unica_apresentacao = contagem[
        contagem['qtd_apresentacoes'] == 1
    ]['PRODUTO'].tolist()
    
    print(f"[OK] Produtos com apresentacao unica: {len(produtos_unica_apresentacao):,}")
    
    # Criar DataFrame mestre para join (apenas produtos únicos)
    df_master_para_join = df_master_completo[
        df_master_completo['PRODUTO'].isin(produtos_unica_apresentacao)
    ].copy()
    
    # Remover duplicatas de PRODUTO (pode haver duplicatas por vigência, etc)
    if col_apresentacao:
        df_master_para_join = df_master_para_join.drop_duplicates(
            subset=['PRODUTO', col_apresentacao], 
            keep='first'
        )
    else:
        df_master_para_join = df_master_para_join.drop_duplicates(
            subset=['PRODUTO'], 
            keep='first'
        )
    
    print(f"[OK] Base mestre para join: {len(df_master_para_join):,} linhas")
    
    return produtos_unica_apresentacao, df_master_para_join


def particionar_trabalhando(df, produtos_unicos):
    """
    Particiona df_trabalhando em candidatos (com produto único) e não candidatos.
    """
    print("\n[INFO] Particionando df_trabalhando...")
    
    mask_candidatos = df['NOME_PRODUTO_LIMPO'].isin(produtos_unicos)
    
    df_candidatos = df[mask_candidatos].copy()
    df_nao_candidatos = df[~mask_candidatos].copy()
    
    print(f"[OK] Candidatos para join: {len(df_candidatos):,} linhas")
    print(f"[OK] Nao candidatos (proximas etapas): {len(df_nao_candidatos):,} linhas")
    
    return df_candidatos, df_nao_candidatos


def executar_join_apresentacao_unica(df_candidatos, df_master_para_join, colunas_referencia=None):
    """
    Executa join direto de alta confiança entre candidatos e base mestre.
    """
    print("\n[INFO] Executando join de alta confianca...")
    
    if df_candidatos.empty:
        print("[AVISO] Nenhum candidato para join. Retornando DataFrame vazio.")
        return pd.DataFrame()
    
    df_right_preparado = preparar_master_para_join(df_master_para_join)
    
    # Executar merge (left mantém suas colunas, right traz apenas novas)
    df_sucesso = pd.merge(
        df_candidatos,
        df_right_preparado,
        left_on='NOME_PRODUTO_LIMPO',
        right_on='PRODUTO_MASTER',
        how='left',
        suffixes=('', '_DROP')  # Sufixo para colunas inesperadas
    )
    
    # Remover colunas com sufixo _DROP (caso existam)
    colunas_drop = [c for c in df_sucesso.columns if c.endswith('_DROP')]
    if colunas_drop:
        df_sucesso = df_sucesso.drop(columns=colunas_drop, errors='ignore')
        print(f"[INFO] Colunas _DROP removidas: {len(colunas_drop)}")
    
    # LIMPEZA: Remover colunas duplicadas com .1 ou .2 (se ainda existirem)
    colunas_duplicadas = [c for c in df_sucesso.columns if '.1' in c or '.2' in c]
    if colunas_duplicadas:
        print(f"[AVISO] Colunas duplicadas (.1/.2) detectadas: {len(colunas_duplicadas)}")
        print(f"[INFO] Removendo: {colunas_duplicadas[:5]}")
        df_sucesso = df_sucesso.drop(columns=colunas_duplicadas, errors='ignore')

    if colunas_referencia is not None:
        df_sucesso = integrar_colunas_master(df_sucesso, colunas_referencia)
    else:
        # Caso referência não tenha sido informada, ainda garantimos remoção do sufixo
        colunas_master_restantes = [c for c in df_sucesso.columns if c.endswith('_MASTER')]
        if colunas_master_restantes:
            df_sucesso = df_sucesso.drop(columns=colunas_master_restantes, errors='ignore')

    
    # Remover duplicatas por chave_codigo (se existir)
    if 'chave_codigo' in df_sucesso.columns:
        df_sucesso = df_sucesso.drop_duplicates(subset=['chave_codigo'], keep='first')
    else:
        df_sucesso = df_sucesso.drop_duplicates()
    
    # Validação final de integridade
    validar_integridade_colunas(df_sucesso, etapa="Join Apresentacao Unica")
    
    print(f"[OK] Join concluido: {len(df_sucesso):,} linhas, {len(df_sucesso.columns)} colunas")
    
    return df_sucesso


def exportar_zip_fast(df, prefixo='df_match_apresentacao_unica'):
    """
    Exporta DataFrame para ZIP (CSV comprimido).
    MODIFICADO: Usa overwriting (sem timestamp) para evitar acúmulo de arquivos.
    """
    filename = f"{prefixo}.zip"
    output_path = DATA_DIR / 'processed' / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Se arquivo existe, será sobrescrito
    if output_path.exists():
        print(f"[INFO] Sobrescrevendo arquivo existente: {filename}")
    
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


def processar_matching_apresentacao_unica():
    """
    Função principal: executa toda a etapa 13.
    """
    print("\n" + "="*80)
    print("ETAPA 13: MATCHING DE PRODUTOS COM APRESENTACAO UNICA")
    print("="*80)
    
    inicio_total = datetime.now()
    
    # 1. Carregar bases mestre
    df_anvisa = carregar_base_anvisa()
    df_manual = carregar_base_manual()
    
    # 2. Criar base mestre completa (todas as apresentações)
    df_master_completo = criar_base_mestre_completa(df_anvisa, df_manual)
    
    # 3. Identificar produtos com apresentação única
    produtos_unicos, df_master_para_join = identificar_produtos_apresentacao_unica(
        df_master_completo
    )
    
    # 4. Carregar df_final_trabalhando (resultado da Etapa 12)
    print("\n[INFO] Carregando df_final_trabalhando...")
    processed_dir = DATA_DIR / 'processed'
    
    # MODIFICADO: Busca arquivo SEM timestamp (overwriting)
    trabalhando_path = processed_dir / 'df_etapa12_final_trabalhando.zip'
    
    if not trabalhando_path.exists():
        # Fallback: procura por arquivos com timestamp (compatibilidade)
        zip_files = sorted(processed_dir.glob('df_final_trabalhando_*.zip'))
        if not zip_files:
            raise FileNotFoundError("Nenhum arquivo df_final_trabalhando encontrado!")
        trabalhando_path = zip_files[-1]
        print(f"[INFO] Usando arquivo legado: {trabalhando_path.name}")
    
    print(f"[INFO] Carregando: {trabalhando_path.name}")
    
    with zipfile.ZipFile(trabalhando_path, 'r') as zip_ref:
        csv_name = zip_ref.namelist()[0]
        with zip_ref.open(csv_name) as f:
            df_trabalhando = pd.read_csv(f, sep=';')
    
    print(f"   [OK] Carregado com sucesso!")
    print(f"   Shape: {df_trabalhando.shape}")
    
    # Verificar coluna necessária
    if 'NOME_PRODUTO_LIMPO' not in df_trabalhando.columns:
        raise ValueError("Coluna 'NOME_PRODUTO_LIMPO' nao encontrada!")

    # Guardar schema oficial para alinhar exportação dos matches
    colunas_referencia = carregar_schema_referencia(df_trabalhando)
    
    # 5. Particionar df_trabalhando
    df_candidatos, df_nao_candidatos = particionar_trabalhando(
        df_trabalhando, 
        produtos_unicos
    )
    
    # 6. Executar join de alta confiança
    df_sucesso = executar_join_apresentacao_unica(
        df_candidatos,
        df_master_para_join,
        colunas_referencia
    )
    if not df_sucesso.empty:
        df_sucesso = alinhar_dataframe_para_schema(
            df_sucesso,
            colunas_referencia,
            contexto="Match apresentacao unica"
        )
    
    # CORREÇÃO: Alinhar df_nao_candidatos ao schema de referência
    # para garantir que todas as colunas sejam mantidas
    print("\n[INFO] Alinhando df_nao_candidatos ao schema de referencia...")
    df_nao_candidatos = alinhar_dataframe_para_schema(
        df_nao_candidatos,
        colunas_referencia,
        contexto="Trabalhando restante"
    )
    
    # 7. Relatório final
    print("\n" + "="*80)
    print("RELATORIO FINAL")
    print("="*80)
    print(f"\nLinhas com match de alta confianca: {len(df_sucesso):,}")
    print(f"Linhas para proximas etapas: {len(df_nao_candidatos):,}")
    
    # 8. Exportar resultados
    print("\n" + "="*80)
    print("EXPORTANDO RESULTADOS")
    print("="*80)
    
    if not df_sucesso.empty:
        output_path_sucesso = exportar_zip_fast(df_sucesso, 'df_etapa13_match_apresentacao_unica')
    else:
        print("[AVISO] Nenhum match de apresentacao unica encontrado para exportar.")
    
    # Exportar df_trabalhando atualizado (apenas não candidatos)
    output_path_trabalhando = exportar_zip_fast(df_nao_candidatos, 'df_etapa13_trabalhando_restante')
    
    # 9. Limpeza de memória
    del df_anvisa, df_manual, df_master_completo, df_master_para_join
    del df_candidatos, df_trabalhando
    gc.collect()
    print("\n[OK] Variaveis intermediarias limpas da memoria")
    
    duracao = (datetime.now() - inicio_total).total_seconds()
    print("\n" + "="*80)
    print(f"[SUCESSO] ETAPA 13 CONCLUIDA EM {duracao:.1f}s!")
    print("="*80)
    
    return output_path_sucesso if not df_sucesso.empty else output_path_trabalhando


if __name__ == '__main__':
    try:
        processar_matching_apresentacao_unica()
    except Exception as e:
        print(f"\n[ERRO] Falha na execucao: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
