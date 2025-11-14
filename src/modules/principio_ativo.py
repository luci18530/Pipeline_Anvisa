# -*- coding: utf-8 -*-
"""
Módulo para processamento e padronização da coluna 'PRINCIPIO ATIVO'.
Inclui normalização, correções manuais, imputação e fuzzy matching.
"""
import pandas as pd
import numpy as np
import re
import os
from tqdm.auto import tqdm
from rapidfuzz import fuzz
import itertools

from .dicionarios_correcao import (
    DICIONARIO_DE_CORRECAO,
    DIC_SUGERIDO_ATIVO,
    CORRECOES_CONTAINS,
    COLUNAS_PARA_NORMALIZAR
)
from .correcoes_ortograficas import processar_correcoes_ortograficas

# ==============================================================================
#      ETAPA 1: NORMALIZAÇÃO INICIAL E BACKUP
# ==============================================================================

def criar_backup_e_normalizar(df):
    """
    Cria backup da coluna original e aplica normalização inicial.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO' (com ou sem acento)
        
    Returns:
        pandas.DataFrame: DataFrame com backup criado e normalização inicial aplicada
    """
    print("=" * 80)
    print("ETAPA 1: NORMALIZAÇÃO INICIAL E BACKUP")
    print("=" * 80)
    
    # Renomear colunas com acento para sem acento (se existirem)
    renomear_map = {
        'PRINCÍPIO ATIVO': 'PRINCIPIO ATIVO',
        'LABORATÓRIO': 'LABORATORIO',
        'APRESENTAÇÃO': 'APRESENTACAO'
    }
    
    colunas_renomeadas = []
    for col_antiga, col_nova in renomear_map.items():
        if col_antiga in df.columns:
            df = df.rename(columns={col_antiga: col_nova})
            colunas_renomeadas.append(f"{col_antiga} -> {col_nova}")
    
    if colunas_renomeadas:
        print("[INFO] Colunas renomeadas para remover acentos:")
        for renomeacao in colunas_renomeadas:
            print(f"  - {renomeacao}")
    
    # Criar backup se ainda não existir
    if 'PRINCIPIO_ATIVO_ORIGINAL' not in df.columns:
        print("\nCriando backup 'PRINCIPIO_ATIVO_ORIGINAL'...")
        df['PRINCIPIO_ATIVO_ORIGINAL'] = df['PRINCIPIO ATIVO']
    else:
        print("\nBackup 'PRINCIPIO_ATIVO_ORIGINAL' já existe. Usando-o como fonte.")
    
    # Aplicar normalização inicial
    print("Aplicando normalização inicial (maiúsculas, espaços, etc.)...")
    df['PRINCIPIO ATIVO'] = (
        df['PRINCIPIO_ATIVO_ORIGINAL']
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(';', ' + ')
        .replace({'NAN': None, 'NONE': None})
    )
    
    print("\n[OK] Normalizacao inicial concluida.")
    print("Amostra apos a limpeza inicial:")
    print(df[['PRINCIPIO ATIVO', 'PRINCIPIO_ATIVO_ORIGINAL']].head(10))
    
    return df

# ==============================================================================
#      ETAPA 2: REMOÇÃO DE ACENTOS
# ==============================================================================

def remover_acentos_colunas(df):
    """
    Remove acentos e diacríticos de colunas especificadas.
    
    Args:
        df (pandas.DataFrame): DataFrame com colunas a serem normalizadas
        
    Returns:
        pandas.DataFrame: DataFrame com acentos removidos
    """
    print("\n" + "=" * 80)
    print("ETAPA 2: REMOÇÃO DE ACENTOS")
    print("=" * 80)
    
    print(f"Processando colunas: {', '.join(COLUNAS_PARA_NORMALIZAR)}")
    
    for coluna in COLUNAS_PARA_NORMALIZAR:
        if coluna in df.columns:
            print(f"  - Processando coluna: '{coluna}'...")
            
            df[coluna] = df[coluna].astype(str)
            
            # Método mais eficiente para remover acentos em Pandas
            df[coluna] = (
                df[coluna]
                .str.normalize('NFD')
                .str.encode('ascii', 'ignore')
                .str.decode('utf-8')
            )
            
            df[coluna] = df[coluna].replace({'nan': np.nan})
        else:
            print(f"  - Aviso: Coluna '{coluna}' não encontrada. Pulando.")
    
    print("\n[OK] Remocao de acentos concluida.")
    print("Amostra do resultado para 'PRINCIPIO ATIVO':")
    if 'PRINCIPIO ATIVO' in df.columns:
        print(df[['PRINCIPIO ATIVO']].head(10))
    
    return df

# ==============================================================================
#      ETAPA 3: CORREÇÕES COM DICIONÁRIO PRINCIPAL
# ==============================================================================

def aplicar_correcoes_dicionario(df):
    """
    Aplica correções usando o dicionário principal de forma segura e otimizada.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        
    Returns:
        pandas.DataFrame: DataFrame com correções aplicadas
    """
    print("\n" + "=" * 80)
    print("ETAPA 3: CORREÇÕES COM DICIONÁRIO PRINCIPAL")
    print("=" * 80)
    
    # Ordenar regras por tamanho (maior para menor)
    regras_ordenadas = sorted(DICIONARIO_DE_CORRECAO.items(), 
                              key=lambda item: len(item[0]), 
                              reverse=True)
    
    print("Aplicando correções de forma vetorizada...")
    textos_corrigidos = df['PRINCIPIO ATIVO'].astype(str).copy()
    
    # Aplicar todas as regras com barra de progresso
    for errado, certo in tqdm(regras_ordenadas, desc="Aplicando regras do dicionário", ncols=100):
        padrao_seguro = rf'\b{re.escape(errado)}\b'
        textos_corrigidos = textos_corrigidos.str.replace(
            pat=padrao_seguro,
            repl=certo,
            flags=re.IGNORECASE,
            regex=True
        )
    
    # Limpeza final
    print("\nAplicando limpeza final...")
    textos_corrigidos = (
        textos_corrigidos
        .str.replace(r'\s+PORT\s+344\s*/?\s*98\s+LISTA\s+[A-Z]\s*\d+', '', regex=True)
        .str.replace(r'\s+A EXCLUIR$', '', regex=True)
        .str.replace(r'^\d+\s+', '', regex=True)
        .str.replace(r'\s{2,}', ' ', regex=True)
        .str.strip()
    )
    
    # Finalizar tratamento de associações
    print("Finalizando tratamento de associações...")
    textos_corrigidos = textos_corrigidos.apply(finalizar_associacoes)
    
    df['PRINCIPIO ATIVO'] = textos_corrigidos.replace({'': np.nan})
    
    print("\n[OK] Correcoes manuais aplicadas de forma otimizada.")
    print("Amostra apos correcoes:")
    print(df[['PRINCIPIO ATIVO']].sample(min(15, len(df))))
    
    return df

def finalizar_associacoes(texto):
    """
    Remove duplicatas em associações de princípios ativos.
    
    Args:
        texto (str): Texto com associações separadas por '+'
        
    Returns:
        str: Texto com associações únicas e ordenadas
    """
    if pd.isna(texto) or ' + ' not in texto:
        return texto
    
    componentes = [comp.strip() for comp in texto.split('+') if comp.strip()]
    componentes_unicos = sorted(list(dict.fromkeys(componentes)))
    return ' + '.join(componentes_unicos)

# ==============================================================================
#      ETAPA 4: PREENCHIMENTO DE "NÃO ESPECIFICADO"
# ==============================================================================

def preencher_nao_especificado(df):
    """
    Preenche valores 'Não Especificado' usando o princípio ativo mais comum
    para a mesma combinação de PRODUTO + APRESENTAÇÃO.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        
    Returns:
        pandas.DataFrame: DataFrame com valores imputados
    """
    print("\n" + "=" * 80)
    print("ETAPA 4: PREENCHIMENTO DE 'NÃO ESPECIFICADO'")
    print("=" * 80)
    
    # Identificar registros a serem preenchidos
    unspecified_set = {'', 'NAO ESPECIFICADO', 'NAN', 'NONE', 'NI', 'NC', 'NA'}
    unspecified_mask = (df['PRINCIPIO ATIVO'].isna() | 
                       df['PRINCIPIO ATIVO'].str.upper().isin(unspecified_set))
    
    print(f"Encontrados {unspecified_mask.sum():,} registros com Princípio Ativo 'Não Especificado' ou nulo.")
    
    if not unspecified_mask.all():
        print("Criando mapa de imputação baseado no Princípio Ativo mais comum...")
        df['chave_descricao'] = df['PRODUTO'].astype(str) + ' | ' + df['APRESENTACAO'].astype(str)
        
        validos = df.loc[~unspecified_mask]
        mapa_imputacao = validos.groupby('chave_descricao')['PRINCIPIO ATIVO'].agg(
            lambda x: x.mode()[0] if not x.mode().empty else np.nan
        ).to_dict()
        
        # Aplicar imputação
        linhas_para_preencher = df[unspecified_mask & df['chave_descricao'].isin(mapa_imputacao)].index
        
        if not linhas_para_preencher.empty:
            df.loc[linhas_para_preencher, 'PRINCIPIO ATIVO'] = df.loc[linhas_para_preencher, 'chave_descricao'].map(mapa_imputacao)
            print(f"[OK] {len(linhas_para_preencher):,} registros foram preenchidos com sucesso.")
        else:
            print("Nenhum registro pode ser preenchido com base nas descricoes existentes.")
        
        # Limpeza
        df.drop(columns=['chave_descricao'], inplace=True)
    else:
        print("Nao ha dados validos para criar um mapa de imputacao. Pulando esta etapa.")
    
    # Relatório final
    remaining_unspecified = (df['PRINCIPIO ATIVO'].isna() | 
                            df['PRINCIPIO ATIVO'].str.upper().isin(unspecified_set))
    print(f"\nRegistros restantes com 'Nao Especificado': {remaining_unspecified.sum():,}")
    
    return df

# ==============================================================================
#      ETAPA 5: CORREÇÕES DIRECIONADAS
# ==============================================================================

def aplicar_correcoes_direcionadas(df):
    """
    Aplica correções direcionadas usando a lista de regras str.replace.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        
    Returns:
        pandas.DataFrame: DataFrame com correções aplicadas
    """
    print("\n" + "=" * 80)
    print("ETAPA 5: CORREÇÕES DIRECIONADAS")
    print("=" * 80)
    
    print("Aplicando correções direcionadas da lista de regras...")
    
    textos_corrigidos = df['PRINCIPIO ATIVO'].astype(str).copy()
    total_afetado = 0
    
    for pattern, replacement, use_regex in tqdm(CORRECOES_CONTAINS, desc="Aplicando regras", ncols=100):
        safe_pattern = pattern if use_regex else re.escape(pattern)
        linhas_afetadas = textos_corrigidos.str.contains(safe_pattern, regex=True, na=False).sum()
        
        if linhas_afetadas > 0:
            print(f"  - Regra ('{pattern}' -> '{replacement}'): {linhas_afetadas:,} linhas afetadas.")
            total_afetado += linhas_afetadas
            
            textos_corrigidos = textos_corrigidos.str.replace(
                pat=pattern,
                repl=replacement,
                regex=use_regex
            )
    
    # Limpeza final de espaços
    textos_corrigidos = textos_corrigidos.str.replace(r'\s{2,}', ' ', regex=True).str.strip()
    df['PRINCIPIO ATIVO'] = textos_corrigidos.replace({'': np.nan, 'nan': np.nan})
    
    if total_afetado > 0:
        print(f"\n[OK] Correcoes direcionadas concluidas.")
    else:
        print("\nNenhuma linha foi afetada pelas regras atuais.")
    
    print("\nAmostra do resultado:")
    print(df[['PRINCIPIO ATIVO']].sample(min(10, len(df))))
    
    return df

# ==============================================================================
#      ETAPA 6: CONSOLIDAÇÃO FINAL
# ==============================================================================

def aplicar_consolidacao_final(df):
    """
    Aplica o dicionário de consolidação final (fuzzy matching).
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        
    Returns:
        pandas.DataFrame: DataFrame com consolidação final aplicada
    """
    print("\n" + "=" * 80)
    print("ETAPA 6: CONSOLIDAÇÃO FINAL (FUZZY MATCHING)")
    print("=" * 80)
    
    # Pré-compilar os padrões regex
    dic_regex = [(re.compile(rf"\b{re.escape(errado)}\b", flags=re.IGNORECASE), certo.upper())
                 for errado, certo in DIC_SUGERIDO_ATIVO.items()]
    
    def aplicar_consolidacao_fast(texto):
        for padrao, certo in dic_regex:
            texto = padrao.sub(certo, texto)
        return texto
    
    print("Aplicando consolidacao final...")
    df['PRINCIPIO ATIVO'] = df['PRINCIPIO ATIVO'].astype(str).map(aplicar_consolidacao_fast)
    
    print("\n[OK] Consolidacao final concluida.")
    print("Amostra do resultado:")
    print(df[['PRINCIPIO ATIVO']].sample(min(10, len(df))))
    
    return df

# ==============================================================================
#      ETAPA 7: ANÁLISE DE FUZZY MATCHING (OPCIONAL - DESATIVADO POR PADRÃO)
# ==============================================================================

def analisar_fuzzy_matching(df, limiar_similaridade=85):
    """
    Analisa princípios ativos e sugere correções usando fuzzy matching.
    Esta função NÃO modifica o DataFrame.
    
    NOTA: Esta função está desativada por padrão pois foi usada para construir
    o dicionário DIC_SUGERIDO_ATIVO. Use apenas se precisar identificar novos
    padrões de similaridade para adicionar ao dicionário.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        limiar_similaridade (int): Percentual mínimo de similaridade (0-100)
        
    Returns:
        dict: Dicionário sugerido de correções
    """
    print("\n" + "=" * 80)
    print("ETAPA 7: ANALISE DE FUZZY MATCHING (OPCIONAL)")
    print("=" * 80)
    print("AVISO: Esta etapa e usada apenas para construir novos dicionarios.")
    print("Os dicionarios atuais ja contem as correcoes necessarias.")
    print("=" * 80)
    
    # Obter lista de princípios ativos únicos
    ativos_unicos = df['PRINCIPIO ATIVO'].dropna().unique()
    ativos_filtrados = sorted([atv for atv in ativos_unicos 
                              if atv.count('+') <= 3 and len(atv) > 3])
    
    print(f"Analisando {len(ativos_filtrados):,} princípios ativos únicos...")
    
    pares_proximos = []
    
    # Encontrar pares com alta similaridade
    for a, b in itertools.combinations(ativos_filtrados, 2):
        similaridade = fuzz.token_set_ratio(a, b)
        if similaridade >= limiar_similaridade:
            pares_proximos.append((a, b, similaridade))
    
    # Gerar dicionário sugerido
    dicionario_sugerido = {}
    for a, b, sim in pares_proximos:
        correto, errado = (a, b) if len(a) >= len(b) else (b, a)
        if errado not in dicionario_sugerido:
            dicionario_sugerido[errado] = correto
    
    # Exibir resultado
    print(f"\nEncontrados {len(dicionario_sugerido)} pares com similaridade >= {limiar_similaridade}%.")
    print("-" * 80)
    
    if not dicionario_sugerido:
        print("Nenhuma sugestão encontrada com os critérios atuais.")
    else:
        print("DICIONÁRIO SUGERIDO (para revisão):")
        print("DIC_SUGERIDO_ATIVO.update({")
        for errado, certo in list(dicionario_sugerido.items())[:20]:  # Mostrar apenas 20 primeiros
            print(f'    "{errado}": "{certo}",')
        if len(dicionario_sugerido) > 20:
            print(f"    ... e mais {len(dicionario_sugerido) - 20} sugestões")
        print("})")
    
    return dicionario_sugerido

# ==============================================================================
#      FUNÇÃO PRINCIPAL
# ==============================================================================

def processar_principio_ativo(df, executar_fuzzy_matching=False):
    """
    Executa todo o pipeline de processamento do princípio ativo.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        executar_fuzzy_matching (bool): Se True, executa análise de fuzzy matching
        
    Returns:
        pandas.DataFrame: DataFrame com princípio ativo processado
    """
    print("=" * 80)
    print("PROCESSAMENTO DO PRINCÍPIO ATIVO")
    print("=" * 80)
    
    # Fazer uma cópia para não modificar o original
    df_processado = df.copy()
    
    # Executar todas as etapas
    df_processado = criar_backup_e_normalizar(df_processado)
    df_processado = remover_acentos_colunas(df_processado)
    df_processado = aplicar_correcoes_dicionario(df_processado)
    df_processado = preencher_nao_especificado(df_processado)
    df_processado = aplicar_correcoes_direcionadas(df_processado)
    df_processado = aplicar_consolidacao_final(df_processado)
    
    # NOVA ETAPA: Correcoes ortograficas e padronizacao de combinacoes
    df_processado = processar_correcoes_ortograficas(df_processado, colunas=['PRINCIPIO ATIVO'])
    
    # Fuzzy matching opcional (para análise)
    if executar_fuzzy_matching:
        analisar_fuzzy_matching(df_processado)
    
    # Renomear coluna STATUS se existir
    if 'TIPO DE PRODUTO (STATUS DO PRODUTO)' in df_processado.columns:
        df_processado.rename(columns={'TIPO DE PRODUTO (STATUS DO PRODUTO)': 'STATUS'}, inplace=True)
        print("\n[OK] Coluna 'TIPO DE PRODUTO (STATUS DO PRODUTO)' renomeada para 'STATUS'.")
    
    print("\n" + "=" * 80)
    print("[OK] PROCESSAMENTO DO PRINCIPIO ATIVO CONCLUIDO!")
    print("=" * 80)
    
    # Estatísticas finais
    total_unicos = df_processado['PRINCIPIO ATIVO'].nunique()
    print(f"\nTotal de principios ativos unicos: {total_unicos:,}")
    
    return df_processado

def exportar_principios_ativos_unicos(df, arquivo_saida='output/anvisa/principios_ativos_unicos.txt'):
    """
    Exporta lista de princípios ativos únicos para arquivo texto.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        arquivo_saida (str): Nome do arquivo de saída
    """
    # Criar pasta output se nao existir
    os.makedirs(os.path.dirname(arquivo_saida), exist_ok=True)
    
    descricoes_unicas_ordenadas = df['PRINCIPIO ATIVO'].dropna().unique()
    descricoes_unicas_ordenadas.sort()
    
    with open(arquivo_saida, "w", encoding='utf-8') as f:
        for descricao in descricoes_unicas_ordenadas:
            f.write(str(descricao) + "\n")
    
    print(f"\n[OK] Arquivo gerado: {arquivo_saida}")
    print(f"Total de principios ativos unicos: {len(descricoes_unicas_ordenadas):,}")

if __name__ == "__main__":
    print("Este módulo deve ser importado e usado em conjunto com outros módulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")