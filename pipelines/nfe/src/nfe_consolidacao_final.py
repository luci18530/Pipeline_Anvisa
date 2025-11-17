# -*- coding: utf-8 -*-
"""
ETAPA 17: CONSOLIDACAO FINAL DO PIPELINE NFe

Consolida os resultados de todas as etapas anteriores em um único DataFrame:
1. df_completo (Etapa 9) - Matches via código EAN
2. df_match_apresentacao_unica (Etapa 13) - Matches de apresentação única
3. df_matched_hibrido (Etapa 16) - Matches via algoritmo híbrido

Padroniza todas as colunas para o schema do df_completo e gera arquivo consolidado final.

Input:  df_etapa09_completo.zip
        df_etapa13_match_apresentacao_unica.zip
        df_etapa16_matched_hibrido.zip
Output: df_etapa17_consolidado_final.zip
"""

import pandas as pd
import numpy as np
import zipfile
import os
import time
import io
import warnings
import unicodedata

from paths import DATA_DIR

# ==============================================================================
#      CONFIGURACOES
# ==============================================================================

# Caminhos dos inputs
INPUT_COMPLETO = DATA_DIR / 'processed' / 'df_etapa09_completo.zip'
INPUT_APRESENTACAO = DATA_DIR / 'processed' / 'df_etapa13_match_apresentacao_unica.zip'
INPUT_HIBRIDO = DATA_DIR / 'processed' / 'df_etapa16_matched_hibrido.zip'

# Caminho do output
OUTPUT_DIR = DATA_DIR / 'processed'
OUTPUT_ZIP = OUTPUT_DIR / 'df_etapa17_consolidado_final.zip'

# Schema de referência (baseado no df_completo)
SCHEMA_REFERENCIA = [
    # Colunas originais da NFe
    'id_descricao', 'descricao_produto', 'id_medicamento', 'cod_anvisa',
    'codigo_municipio_destinatario', 'municipio', 'data_emissao', 'codigo_ncm',
    'codigo_ean', 'valor_produtos', 'valor_unitario', 'quantidade', 'unidade',
    'cpf_cnpj_emitente', 'chave_codigo', 'cpf_cnpj', 'razao_social_emitente',
    'nome_fantasia_emitente', 'razao_social_destinatario', 'nome_fantasia_destinatario',
    'id_data_fabricacao', 'id_data_validade', 'data_emissao_original',
    'ano_emissao', 'mes_emissao', 'municipio_bruto',
    # Colunas da base ANVISA
    'ID_CMED_PRODUTO_LIST', 'GRUPO ANATOMICO', 'PRINCIPIO ATIVO', 'PRODUTO',
    'STATUS', 'APRESENTACAO', 'TIPO DE PRODUTO', 'QUANTIDADE UNIDADES',
    'QUANTIDADE MG', 'QUANTIDADE ML', 'QUANTIDADE UI', 'LABORATORIO',
    'CLASSE TERAPEUTICA', 'GRUPO TERAPEUTICO', 'GGREM', 'EAN_1', 'EAN_2', 'EAN_3',
    'REGISTRO', 'PRECO_MAXIMO_REFINADO', 'CAP_FLAG_CORRIGIDO', 'ICMS0_FLAG_CORRIGIDO'
]

# Mapeamento para df_match_apresentacao_unica
# (já está no formato correto, mas algumas colunas podem ter nomes ligeiramente diferentes)
MAP_APRESENTACAO = {
    'QUANTIDADE MG': 'QUANTIDADE MG',
    'QUANTIDADE ML': 'QUANTIDADE ML',
    'QUANTIDADE UI': 'QUANTIDADE UI',
}

# Mapeamento para df_matched_hibrido
MAP_HIBRIDO = {
    # Colunas com nomes diferentes vindas da base mestre
    'QUANTIDADE MG (POR UNIDADE/ML)': 'QUANTIDADE MG',
    'LABORATORIO_CLEAN': None,  # Remover
    'PRODUTO_CLEAN': None,  # Remover
    'PRINCIPIO_ATIVO_CLEAN': None,  # Remover
    'PRODUTO_SPECIFIC': None,  # Remover
    'PA_SPECIFIC': None,  # Remover
    'WORD_SET': None,  # Remover
    'PRODUTO_ORIGINAL': None,  # Remover
    'PRINCIPIO_ATIVO_ORIGINAL': None,  # Remover
    'LABORATORIO_ORIGINAL': None,  # Remover
    'CLASSE_TERAPEUTICA_ORIGINAL': None,  # Remover
        'APRESENTACAO_ORIGINAL': 'APRESENTACAO',  # Preservar apresentacao oficial
        'CLASSE TERAPEUTICA.1': None,  # Remover duplicada
    'SUBSTANCIA_COMPOSTA': None,  # Remover
    'ID_PRECO': None,  # Remover
    'ID_PRODUTO': None,  # Remover
    'VIG_INICIO': None,  # Remover
    'VIG_FIM': None,  # Remover
    'REGIME DE PREÇO': None,  # Remover
    'PF 0%': None,  # Remover
    'PF 20%': None,  # Remover
    'PMVG 0%': None,  # Remover
    'PMVG 20%': None,  # Remover
    'ICMS 0%': None,  # Remover
    'CAP': None,  # Remover
    'NOME_PRODUTO_LIMPO': None,  # Remover
    # Renomear EAN
    'EAN 1': 'EAN_1',
    'EAN 2': 'EAN_2',
    'EAN 3': 'EAN_3',
    # Renomear GGREM
    'CÓDIGO GGREM': 'GGREM',
}


# ==============================================================================
#      FUNCOES AUXILIARES
# ==============================================================================

def read_csv_intelligently(filepath, encoding='utf-8'):
    """
    Lê um arquivo CSV de forma robusta:
    1. Descompacta se for ZIP
    2. Tenta diferentes separadores
    3. Lida com linhas malformadas
    """
    print(f"  [1/3] Abrindo arquivo: {filepath.name}")
    
    # Descompactar se for ZIP
    if filepath.suffix.lower() == '.zip':
        with zipfile.ZipFile(filepath, 'r') as zf:
            csv_filename = next((f for f in zf.namelist() if f.lower().endswith('.csv')), None)
            if not csv_filename:
                raise ValueError(f"Nenhum CSV encontrado em {filepath.name}")
            
            print(f"  [2/3] Extraindo: {csv_filename}")
            with zf.open(csv_filename) as f:
                file_buffer = io.BytesIO(f.read())
    else:
        file_buffer = filepath
    
    # Tentar ler com diferentes separadores
    print(f"  [3/3] Lendo CSV...")
    
    for sep in [';', '\t', ',']:
        try:
            if isinstance(file_buffer, io.BytesIO):
                file_buffer.seek(0)
            
            df = pd.read_csv(
                file_buffer,
                sep=sep,
                encoding=encoding,
                low_memory=False,
                on_bad_lines='warn'
            )
            
            # Verificar se a leitura foi bem-sucedida (mais de 2 colunas)
            if df.shape[1] > 2:
                print(f"  [OK] Lido com sucesso (sep='{sep}'): {len(df):,} linhas, {len(df.columns)} colunas")
                return df
        except Exception as e:
            continue
    
    raise IOError(f"Falha ao ler {filepath.name}")


def format_to_schema(df, schema, source_name):
    """
    Formata DataFrame para o schema de referência.
    Adiciona colunas faltantes como NA e remove colunas extras.
    """
    print(f"\n  [{source_name}] Formatando para schema de referência...")
    
    df_copy = df.copy()
    
    # Adicionar colunas faltantes
    colunas_faltantes = [col for col in schema if col not in df_copy.columns]
    if colunas_faltantes:
        print(f"    -> Adicionando {len(colunas_faltantes)} colunas faltantes como NA")
        for col in colunas_faltantes:
            df_copy[col] = pd.NA
    
    # Remover colunas extras
    colunas_extras = [col for col in df_copy.columns if col not in schema]
    if colunas_extras:
        print(f"    -> Removendo {len(colunas_extras)} colunas extras")
        df_copy = df_copy.drop(columns=colunas_extras)
    
    # Reordenar colunas
    df_copy = df_copy[schema]
    
    print(f"    [OK] Formatado: {len(df_copy):,} linhas, {len(df_copy.columns)} colunas")
    
    return df_copy


def aplicar_mapeamento(df, mapeamento, source_name):
    """
    Aplica mapeamento de colunas e remove colunas marcadas como None.
    """
    print(f"\n  [{source_name}] Aplicando mapeamento de colunas...")
    
    df_copy = df.copy()
    
    # Separar colunas para renomear e remover
    cols_renomear = {k: v for k, v in mapeamento.items() if v is not None and k in df_copy.columns}
    cols_remover = [k for k, v in mapeamento.items() if v is None and k in df_copy.columns]
    
    # Renomear
    if cols_renomear:
        print(f"    -> Renomeando {len(cols_renomear)} colunas")
        df_copy = df_copy.rename(columns=cols_renomear)
    
    # Remover
    if cols_remover:
        print(f"    -> Removendo {len(cols_remover)} colunas desnecessárias")
        df_copy = df_copy.drop(columns=cols_remover)
    
    print(f"    [OK] Mapeamento aplicado")
    
    return df_copy


def remover_acentos_texto(valor):
    if pd.isna(valor):
        return valor
    texto = str(valor)
    normalizado = unicodedata.normalize('NFKD', texto)
    return ''.join(ch for ch in normalizado if not unicodedata.combining(ch))


def normalizar_colunas_sem_acentos(df, colunas):
    df_norm = df.copy()
    for coluna in colunas:
        if coluna in df_norm.columns:
            df_norm[coluna] = df_norm[coluna].apply(remover_acentos_texto)
    return df_norm


# ==============================================================================
#      CARREGAMENTO E PROCESSAMENTO
# ==============================================================================

def carregar_e_processar_dataframes():
    """
    Carrega e processa os três DataFrames principais.
    """
    print("\n" + "="*80)
    print("CARREGANDO E PROCESSANDO DATAFRAMES")
    print("="*80)
    
    dataframes_processados = []
    
    # 1. DF_COMPLETO (referência - já está no formato correto)
    print("\n[1/3] DF_COMPLETO (Etapa 9 - Matches via EAN)")
    print("-" * 80)
    
    if not INPUT_COMPLETO.exists():
        print(f"  [AVISO] Arquivo não encontrado: {INPUT_COMPLETO.name}")
    else:
        try:
            df_completo = read_csv_intelligently(INPUT_COMPLETO)
            df_completo_formatado = format_to_schema(df_completo, SCHEMA_REFERENCIA, "DF_COMPLETO")
            dataframes_processados.append(('DF_COMPLETO', df_completo_formatado))
            print(f"  [OK] DF_COMPLETO processado: {len(df_completo_formatado):,} registros")
        except Exception as e:
            print(f"  [ERRO] Falha ao processar DF_COMPLETO: {e}")
    
    # 2. DF_MATCH_APRESENTACAO_UNICA
    print("\n[2/3] DF_MATCH_APRESENTACAO_UNICA (Etapa 13)")
    print("-" * 80)
    
    if not INPUT_APRESENTACAO.exists():
        print(f"  [AVISO] Arquivo não encontrado: {INPUT_APRESENTACAO.name}")
    else:
        try:
            df_apresentacao = read_csv_intelligently(INPUT_APRESENTACAO)
            df_apresentacao_mapeado = aplicar_mapeamento(df_apresentacao, MAP_APRESENTACAO, "APRESENTACAO")
            df_apresentacao_formatado = format_to_schema(df_apresentacao_mapeado, SCHEMA_REFERENCIA, "APRESENTACAO")
            dataframes_processados.append(('DF_APRESENTACAO', df_apresentacao_formatado))
            print(f"  [OK] DF_APRESENTACAO processado: {len(df_apresentacao_formatado):,} registros")
        except Exception as e:
            print(f"  [ERRO] Falha ao processar DF_APRESENTACAO: {e}")
    
    # 3. DF_MATCHED_HIBRIDO
    print("\n[3/3] DF_MATCHED_HIBRIDO (Etapa 16)")
    print("-" * 80)
    
    if not INPUT_HIBRIDO.exists():
        print(f"  [AVISO] Arquivo não encontrado: {INPUT_HIBRIDO.name}")
    else:
        try:
            df_hibrido = read_csv_intelligently(INPUT_HIBRIDO)
            df_hibrido_mapeado = aplicar_mapeamento(df_hibrido, MAP_HIBRIDO, "HIBRIDO")
            df_hibrido_formatado = format_to_schema(df_hibrido_mapeado, SCHEMA_REFERENCIA, "HIBRIDO")
            dataframes_processados.append(('DF_HIBRIDO', df_hibrido_formatado))
            print(f"  [OK] DF_HIBRIDO processado: {len(df_hibrido_formatado):,} registros")
        except Exception as e:
            print(f"  [ERRO] Falha ao processar DF_HIBRIDO: {e}")
    
    return dataframes_processados


# ==============================================================================
#      CONSOLIDACAO
# ==============================================================================

def consolidar_dataframes(dataframes_processados):
    """
    Concatena todos os DataFrames processados em um único DataFrame.
    """
    print("\n" + "="*80)
    print("CONSOLIDANDO DATAFRAMES")
    print("="*80)
    
    if not dataframes_processados:
        raise ValueError("Nenhum DataFrame foi processado com sucesso")
    
    print(f"\nConcatenando {len(dataframes_processados)} DataFrames...")
    
    # Extrair apenas os DataFrames (sem os nomes)
    dfs = [df for _, df in dataframes_processados]
    
    # Concatenar
    df_consolidado = pd.concat(dfs, ignore_index=True)
    df_consolidado = normalizar_colunas_sem_acentos(
        df_consolidado,
        ['STATUS', 'TIPO DE PRODUTO']
    )
    
    print(f"[OK] Concatenação concluída: {len(df_consolidado):,} registros totais")
    
    # Resumo por fonte
    print("\nResumo por fonte:")
    inicio = 0
    for nome, df in dataframes_processados:
        fim = inicio + len(df)
        pct = (len(df) / len(df_consolidado)) * 100
        print(f"  {nome:<25} {len(df):>8,} registros ({pct:>5.1f}%)")
        inicio = fim
    
    return df_consolidado


# ==============================================================================
#      LIMPEZA E VALIDACAO
# ==============================================================================

def limpar_e_validar(df):
    """
    Remove registros inválidos e valida o DataFrame consolidado.
    """
    print("\n" + "="*80)
    print("LIMPEZA E VALIDAÇÃO")
    print("="*80)
    
    df_limpo = df.copy()
    total_original = len(df_limpo)
    
    # 1. Remover registros sem município
    print("\n[1/3] Removendo registros sem município...")
    antes = len(df_limpo)
    df_limpo = df_limpo.dropna(subset=['municipio'])
    removidos = antes - len(df_limpo)
    if removidos > 0:
        print(f"  -> Removidos: {removidos:,} registros ({removidos/antes*100:.1f}%)")
    else:
        print(f"  -> Nenhum registro removido")
    
    # 2. Remover registros sem princípio ativo
    print("\n[2/3] Removendo registros sem princípio ativo...")
    antes = len(df_limpo)
    df_limpo = df_limpo.dropna(subset=['PRINCIPIO ATIVO'])
    removidos = antes - len(df_limpo)
    if removidos > 0:
        print(f"  -> Removidos: {removidos:,} registros ({removidos/antes*100:.1f}%)")
    else:
        print(f"  -> Nenhum registro removido")
    
    # 3. Validação de duplicatas
    print("\n[3/3] Verificando duplicatas...")
    duplicatas = df_limpo.duplicated(subset=['chave_codigo']).sum()
    if duplicatas > 0:
        print(f"  [AVISO] Encontradas {duplicatas:,} chaves duplicadas")
        print(f"  -> Mantendo primeira ocorrência de cada chave")
        df_limpo = df_limpo.drop_duplicates(subset=['chave_codigo'], keep='first')
    else:
        print(f"  [OK] Nenhuma duplicata encontrada")
    
    # Resumo final
    total_final = len(df_limpo)
    total_removidos = total_original - total_final
    
    print("\n" + "="*80)
    print("RESUMO DA LIMPEZA")
    print("="*80)
    print(f"Total original:      {total_original:>10,} registros")
    print(f"Total removidos:     {total_removidos:>10,} registros ({total_removidos/total_original*100:.1f}%)")
    print(f"Total final:         {total_final:>10,} registros ({total_final/total_original*100:.1f}%)")
    
    return df_limpo


# ==============================================================================
#      EXPORTACAO
# ==============================================================================

def exportar_consolidado(df):
    """
    Exporta o DataFrame consolidado para arquivo ZIP.
    """
    print("\n" + "="*80)
    print("EXPORTANDO RESULTADO CONSOLIDADO")
    print("="*80)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"\n[1/2] Criando arquivo ZIP: {OUTPUT_ZIP.name}")
    
    # Criar ZIP com compressão
    with zipfile.ZipFile(OUTPUT_ZIP, 'w', zipfile.ZIP_DEFLATED) as z:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8')
        z.writestr('df_etapa17_consolidado_final.csv', csv_buffer.getvalue())
    
    # Calcular tamanhos
    tamanho_memoria_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    tamanho_zip_mb = OUTPUT_ZIP.stat().st_size / (1024 * 1024)
    taxa_compressao = (1 - tamanho_zip_mb / tamanho_memoria_mb) * 100 if tamanho_memoria_mb > 0 else 0
    
    print(f"[2/2] Arquivo exportado com sucesso!")
    print(f"\n  Tamanho em memória:     {tamanho_memoria_mb:>8.2f} MB")
    print(f"  Tamanho arquivo ZIP:    {tamanho_zip_mb:>8.2f} MB")
    print(f"  Taxa de compressão:     {taxa_compressao:>8.1f}%")
    print(f"\n  Localização: {OUTPUT_ZIP}")


# ==============================================================================
#      RELATORIO
# ==============================================================================

def gerar_relatorio(df):
    """
    Gera relatório estatístico do DataFrame consolidado.
    """
    print("\n" + "="*80)
    print("RELATÓRIO ESTATÍSTICO")
    print("="*80)
    
    # 1. Informações gerais
    print(f"\nTotal de registros:      {len(df):>10,}")
    print(f"Total de colunas:        {len(df.columns):>10,}")
    
    # 2. Cobertura de dados ANVISA
    print("\n" + "-"*80)
    print("Cobertura de Dados ANVISA")
    print("-"*80)
    
    colunas_anvisa = ['PRODUTO', 'LABORATORIO', 'PRINCIPIO ATIVO', 'APRESENTACAO']
    for col in colunas_anvisa:
        nao_nulos = df[col].notna().sum()
        pct = (nao_nulos / len(df)) * 100
        print(f"  {col:<25} {nao_nulos:>10,} ({pct:>5.1f}%)")
    
    # 3. Top municípios
    print("\n" + "-"*80)
    print("Top 10 Municípios")
    print("-"*80)
    
    top_municipios = df['municipio'].value_counts().head(10)
    for municipio, count in top_municipios.items():
        pct = (count / len(df)) * 100
        print(f"  {municipio:<30} {count:>10,} ({pct:>5.1f}%)")
    
    # 4. Valores nulos críticos
    print("\n" + "-"*80)
    print("Valores Nulos em Colunas Críticas")
    print("-"*80)
    
    colunas_criticas = ['municipio', 'PRINCIPIO ATIVO', 'LABORATORIO', 'valor_produtos']
    for col in colunas_criticas:
        nulos = df[col].isna().sum()
        pct = (nulos / len(df)) * 100
        print(f"  {col:<25} {nulos:>10,} nulos ({pct:>5.1f}%)")


# ==============================================================================
#      FUNCAO PRINCIPAL
# ==============================================================================

def processar_consolidacao_final():
    """
    Orquestra toda a etapa 17.
    """
    print("\n" + "="*80)
    print("ETAPA 17: CONSOLIDAÇÃO FINAL DO PIPELINE NFe")
    print("="*80)
    
    inicio = time.time()
    
    try:
        # 1. Carregar e processar DataFrames
        dataframes_processados = carregar_e_processar_dataframes()
        
        if not dataframes_processados:
            raise ValueError("Nenhum DataFrame foi carregado com sucesso")
        
        # 2. Consolidar
        df_consolidado = consolidar_dataframes(dataframes_processados)
        
        # 3. Limpar e validar
        df_limpo = limpar_e_validar(df_consolidado)
        
        # 4. Gerar relatório
        gerar_relatorio(df_limpo)
        
        # 5. Exportar
        exportar_consolidado(df_limpo)
        
        duracao = time.time() - inicio
        print("\n" + "="*80)
        print(f"[SUCESSO] ETAPA 17 CONCLUÍDA EM {duracao:.1f}s")
        print("="*80)
        
        return df_limpo
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"[ERRO] Falha na Etapa 17: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        return None


# ==============================================================================
#      EXECUCAO
# ==============================================================================

if __name__ == "__main__":
    df_consolidado = processar_consolidacao_final()
    
    if df_consolidado is not None:
        print(f"\n✓ DataFrame consolidado disponível com {len(df_consolidado):,} registros")
        print(f"✓ Arquivo: {OUTPUT_ZIP}")
