# -*- coding: utf-8 -*-
"""
Módulo para processamento e padronização da coluna 'PRODUTO'.
Inclui normalização de STATUS, segmentação inteligente e correções.
"""
import pandas as pd
import numpy as np
import re
import os
from tqdm.auto import tqdm

from .dicionarios_produto import (
    NAO_SEPARA, PREP, POSFIXO_LETRA_OK,
    DICIONARIO_CORRECAO_PRODUTO,
    PRE_REPLACERS,
    POST_FIX_RULES,
    DIC_SUGERIDO_PRODUTO,
    CORRECOES_CONTAINS_PRODUTO
)
from .correcoes_ortograficas import processar_correcoes_ortograficas

# ==============================================================================
#      ETAPA 1: FILTROS INICIAIS
# ==============================================================================

def remover_produtos_teste_tabelado(df):
    """
    Remove registros com PRODUTO contendo 'TESTE' ou 'TABELADO'.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRODUTO'
        
    Returns:
        pandas.DataFrame: DataFrame filtrado
    """
    print("=" * 80)
    print("ETAPA 1: REMOCAO DE PRODUTOS TESTE/TABELADO")
    print("=" * 80)
    
    linhas_antes = len(df)
    df_filtrado = df[~df['PRODUTO'].str.contains('TESTE|TABELADO', na=False)].copy()
    linhas_removidas = linhas_antes - len(df_filtrado)
    
    print(f"Linhas removidas: {linhas_removidas:,}")
    print(f"Linhas restantes: {len(df_filtrado):,}")
    
    return df_filtrado

# ==============================================================================
#      ETAPA 2: NORMALIZAÇÃO DA COLUNA STATUS
# ==============================================================================

def normalizar_status(df):
    """
    Normaliza a coluna 'STATUS' (antiga 'TIPO DE PRODUTO (STATUS DO PRODUTO)').
    Remove variações, padroniza categorias e trata valores nulos.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'STATUS'
        
    Returns:
        pandas.DataFrame: DataFrame com STATUS normalizado
    """
    print("\n" + "=" * 80)
    print("ETAPA 2: NORMALIZACAO DA COLUNA STATUS")
    print("=" * 80)
    
    if 'STATUS' not in df.columns:
        print("[AVISO] Coluna 'STATUS' nao encontrada. Pulando normalizacao.")
        return df
    
    # Contar nulos antes
    nulos_antes = df['STATUS'].isna().sum()
    print(f"Valores nulos antes: {nulos_antes:,}")
    
    # Aplicar transformações
    df.loc[:, 'STATUS'] = (
        df['STATUS']
        .astype(str)
        .str.replace(r'\s+E$', '', regex=True)
        .str.replace(r'BIOLOGICOS', 'BIOLOGICO', regex=True)
        .str.replace(r'BIOLOGICO NOVO', 'BIOLOGICO', regex=True)
        .str.replace(r'GENERICO\s*\(REFERENCIA\)', 'GENERICO', regex=True)
        .str.replace(r'ESPECIFICO\s*\(REFERENCIA\)', 'ESPECIFICO', regex=True)
        .str.replace(r'SIMILAR\s*\(REFERENCIA\)', 'SIMILAR', regex=True)
        .str.replace(r'NOVO\s*\(REFERENCIA\)', 'NOVO', regex=True)
        .str.replace(r'RADIOFARMACO', 'ESPECIFICO', regex=True)
        .str.replace(r'\b0\b', 'ESPECIFICO', regex=True)
        .str.replace(r'-\(\*\)', '-', regex=True)
        .str.replace(r'NAN', '-', regex=True)
        .str.replace(r'NONE', '-', regex=True)
        .str.strip()
    )
    
    # Estatísticas finais
    print(f"\nCategorias de STATUS:")
    print(df['STATUS'].value_counts().head(10))
    print(f"\n[OK] Coluna STATUS normalizada com sucesso!")
    
    return df

# ==============================================================================
#      ETAPA 3: NORMALIZAÇÃO DA COLUNA APRESENTAÇÃO
# ==============================================================================

def criar_flag_substancia_composta(df):
    """
    Cria flag SUBSTANCIA_COMPOSTA para identificar medicamentos com múltiplos princípios ativos.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        
    Returns:
        pandas.DataFrame: DataFrame com nova coluna SUBSTANCIA_COMPOSTA
    """
    if 'PRINCIPIO ATIVO' in df.columns:
        df['SUBSTANCIA_COMPOSTA'] = df['PRINCIPIO ATIVO'].str.contains(r'\+', na=False)
        print(f"[OK] Flag SUBSTANCIA_COMPOSTA criada: {df['SUBSTANCIA_COMPOSTA'].sum():,} compostos identificados")
    else:
        df['SUBSTANCIA_COMPOSTA'] = False
        print("[AVISO] Coluna 'PRINCIPIO ATIVO' nao encontrada. Flag SUBSTANCIA_COMPOSTA definida como False")
    
    return df

def ajustar_espacos_em_mais(texto: str) -> str:
    """
    Insere espaço antes e depois de '+', removendo espaços duplicados.
    
    Args:
        texto (str): Texto a ser ajustado
        
    Returns:
        str: Texto com espaçamento normalizado ao redor de '+'
    """
    if not isinstance(texto, str):
        return texto
    texto = re.sub(r'\s*\+\s*', ' + ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

# Constantes para normalização de apresentação
UNIDADES_BASE = ["MG", "G", "MCG", "ML", "L", "UI", "MEQ", "MMOL", "%"]

PADRONIZACOES_APRESENTACAO = {
    r'\bAGU DESC COM SIST SEG\b': '',
    r'\bCOM SIST SEG\b': '',
    r'\bCOM SIST SEGURANCA\b': '',
    r'\b0 05 MG\b': '50 MCG',
    r'\b0 075 MG\b': '75 MCG',
    r'\bBISN COM 20 G\b': 'BG X 20 G',
    r'\b5 631\b': '5,631',
    r'\bX 14 14 28\b': 'X 56',
    r'\bX 20 10 40\b': 'X 70',
    r'\bX 28 14 42\b': 'X 84',
    r'\bX 56 28 42\b': 'X 126',
    r'\bX 28 24 4\b': 'X 56',
    r'\bX 84 72 12\b': 'X 168',
    r'\bCRE\b': 'CREME',
    r'\b1 G\b': '1000 MG',
    r'\b24 H\b': '',
    r'\bCREM\b': 'CREME',
    r'\bSOL\b': 'SOLUCAO',
    r'\bCOM\b': 'COMPRIMIDOS',
    r'\bCPD\b': 'COMPRIMIDOS',
    r'\bCOMP\b': 'COMPRIMIDOS',
    r'\bCAP\b': 'CAPSULAS',
    r'\bCP\b': 'COPO',
    r'\bCOP\b': 'COPO',
    r'\bDER\b': 'DERM',
    r'\bSEM ACUCAR\b': '',
    r'\b7 LUVAS\b': '',
    r'\b14 DEDEIRAS\b': '',
    r'\b2 DEDEIRAS\b': '',
    r'\b4 COM PLACEBO\b': '',
    r'\b7 PLACEBOS\b': '',
    r'\b21 PLACEBOS\b': '',
    r'\b12 PLACEBO\b': '',
    r'\b4 PLACEBO\b': '',
    r'\b12 PLACEBOS\b': '',
    r'\b4 PLACEBOS\b': '',
    r'\b6 PLACEBOS\b': '',
    r'\b8 PLACEBOS\b': '',
    r'\b04\b': '4',
    r'\bS ACUCAR\b': '',
    r'\bREMOVERRRRRR\b': '',
    r'\b50 COP\b': '',
    r'\b24 COP\b': '',
    r'\b96 COP\b': '',
    r'\b48 COP\b': '',
    r'\b25 COP\b': '',
    r'\bCOM 2 MMOL\b': 'CONTENDO 2 MMOL',
    r'\b1 PORTA COMPRIMIDO\b': '',
    r'\bAGU DISPOSITIVO DE SEGURANCA\b': '',
    r'\bSEM AGU\b': '',
    r'\bNBSP 01\b': '',
    r'\bEST AMP\b': 'AMP',
    r'\bOF\b': 'OFT',
    r'\bFRGOT\b': 'FR',
    r'\bSTR\b': 'STRIP',
    r'\bCAPS\b': 'CAPSULAS',
    r'\bBOMB\b': 'BOMBO',
    r'\bHOSP\b': 'HOSPITALAR',
    r'\bPREENC\b': 'PREENCHIDAS',
    r'\bPREENCH\b': 'PREENCHIDAS',
    r'\bMICROG\b': 'MICROGRANULADO',
    r'\bMCGRAN\b': 'MICROGRANULADO',
    r'\bDRG\b': 'COMPRIMIDOS',
    r'\bSAC\b': 'SACHES',
    r'\bPOM\b': 'POMADA',
    r'\bALX\b': 'AL X',
    r'\bSACH\b': 'SACHES',
    r'\bINAL\b': 'INALADOR',
    r'\bMGGRAN\b': 'MICROGRANULADO',
    r'\bORODISPERS\b': 'ORODISPERSIVEL',
    r'\bORODISP\b': 'ORODISPERSIVEL',
    r'\bSPR\b': 'SPRAY',
    r'\bACION\b': 'ACIONAMENTOS',
    r'\bOR\b': 'ORAL',
    r'\bSUS\b': 'SUSP',
    r'\bCAPI\b': 'CAPILAR',
    r'\bCAPIL\b': 'CAPILAR',
    r'\bCAPILA\b': 'CAPILAR',
    r'\bBOLS\b': 'BOLSA',
    r'\bNAS\b': 'NASAL',
    r'\bAPL\b': 'APLIC',
    r'\bGCREM\b': 'G CREME',
    r'\bSACHE\b': 'SACHES',
    r'\bXAMP\b': 'XAMP',
    r'\bSHAMP\b': 'XAMP',
    r'\bPAST\b': 'PASTILHA',
    r'\bPAS\b': 'PASTILHA',
    r'\bTB\b': 'TUBO',
    r'\bCAR\b': 'CARP',
    r'\bMGRAN\b': 'MICROGRANULADO',
    r'\bAGU\b': 'AGULHAS',
    r'\bAG\b': 'AGULHAS',
    r'\bCR\b': 'CREME',
    r'\bGIN\b': 'GINEC',
    r'\bPRENC\b': 'PREENCHIDAS',
    r'\bTRANSX\b': 'TRANS X',
    r'\bATOMIZACOES\b': 'ACIONAMENTOS',
}

# Padrão regex para blocos de dosagem
PADRAO_BLOCO = None  # Será compilado na primeira chamada

def _get_padrao_bloco():
    """Retorna o padrão compilado para blocos de dosagem."""
    global PADRAO_BLOCO
    if PADRAO_BLOCO is None:
        PADRAO_BLOCO = re.compile(
            r'((?:\d+\s+){1,40})'
            r'(' + '|'.join(UNIDADES_BASE) + r')'
            r'(?:\s+(' + '|'.join(UNIDADES_BASE) + r'))?'
        )
    return PADRAO_BLOCO
    """
    Padroniza a coluna STATUS removendo acentos e convertendo para maiúsculas.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'STATUS'
        
    Returns:
        pandas.DataFrame: DataFrame com STATUS normalizado
    """
    print("\n" + "=" * 80)
    print("ETAPA 2: NORMALIZACAO DA COLUNA STATUS")
    print("=" * 80)
    
    if 'STATUS' not in df.columns:
        print("[AVISO] Coluna 'STATUS' nao encontrada no DataFrame.")
        return df
    
    print("Normalizando a coluna 'STATUS'...")
    
    df['STATUS'] = (
        df['STATUS']
        .astype(str)
        # Remove acentos
        .str.normalize('NFD')
        .str.encode('ascii', 'ignore')
        .str.decode('utf-8')
        # Padroniza
        .str.upper()
        .str.strip()
    )
    
    print("\n[OK] Coluna 'STATUS' normalizada.")
    print("Valores unicos apos a normalizacao:")
    print(df['STATUS'].value_counts())
    
    return df

# ==============================================================================
#      ETAPA 3: CORREÇÃO E SEGMENTAÇÃO DA DESCRIÇÃO
# ==============================================================================

def criar_backup_produto(df):
    """
    Cria backup da coluna PRODUTO para permitir re-execuções.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRODUTO'
        
    Returns:
        pandas.DataFrame: DataFrame com backup criado
    """
    if 'PRODUTO_ORIGINAL' not in df.columns:
        print("Criando backup 'PRODUTO_ORIGINAL' para permitir re-execucoes.")
        df['PRODUTO_ORIGINAL'] = df['PRODUTO']
    else:
        print("Utilizando backup 'PRODUTO_ORIGINAL' como fonte para garantir consistencia.")
    
    return df

def corrigir_e_segmentar_descricao(texto):
    """
    Aplica lógica avançada de limpeza, correção e segmentação de nomes de produtos.
    
    Args:
        texto (str): Nome do produto original
        
    Returns:
        str: Nome do produto corrigido e segmentado
    """
    if not isinstance(texto, str) or not texto.strip():
        return texto
    
    # a) Limpeza básica e normalização
    t = texto.upper().strip()
    t = re.sub(r"^\d+\s+", "", t)
    t = re.sub(r"\s+PORT\s*344\s*/?\s*98\s*LISTA\s+[A-Z]\s*\d*|\bPORT\b.*|\bPORTARIA\b.*", "", t)
    t = re.sub(r"\s+A EXCLUIR$|\bGENERICO(S)?\b", "", t)
    t = t.replace(";", " + ")
    
    # b) Correções ortográficas do dicionário
    for errado, certo in DICIONARIO_CORRECAO_PRODUTO.items():
        t = re.sub(rf"\b{re.escape(errado)}\b", certo, t, flags=re.IGNORECASE)
    
    # c) Pré-fusão de termos
    for rx, rep in PRE_REPLACERS:
        t = rx.sub(rep, t)
    
    # d) Lógica de Segmentação
    tokens = t.split()
    if not tokens:
        return t
    
    comps, cur, root_seen = [], [], False
    
    def append_cur():
        nonlocal cur, comps
        if cur:
            comps.append(" ".join(cur).strip())
        cur = []
    
    for i, tok in enumerate(tokens):
        if tok == '+':
            append_cur()
            root_seen = False
            continue
        
        if len(tok) == 1 and cur and cur[-1] in POSFIXO_LETRA_OK:
            cur.append(tok)
            root_seen = True
            continue
        
        if tok in NAO_SEPARA or (cur and cur[-1] in PREP):
            cur.append(tok)
            if tok not in NAO_SEPARA and len(tok) > 2:
                root_seen = True
            continue
        
        if root_seen and (not cur or cur[-1] not in PREP):
            append_cur()
        
        cur.append(tok)
        root_seen = True
    
    append_cur()
    t = " + ".join(list(dict.fromkeys(comps)))  # Dedup
    
    # e) Correções finais com regex
    for pattern, replacement in POST_FIX_RULES:
        t = re.sub(pattern, replacement, t, flags=re.IGNORECASE)
    
    # f) Limpeza final de caracteres e espaços
    t = re.sub(r'\b(\w+)(?: \1\b)+', r'\1', t)  # Dedup palavras seguidas
    t = re.sub(r"\s*\+\s*", " + ", t).strip()
    t = re.sub(r"^\+\s*|\s*\+$", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    
    return t

def aplicar_segmentacao_produto(df):
    """
    Aplica correção e segmentação apenas em produtos GENERICO e GENERICO REFERENCIA.
    
    Args:
        df (pandas.DataFrame): DataFrame com colunas 'PRODUTO' e 'STATUS'
        
    Returns:
        pandas.DataFrame: DataFrame com produtos segmentados
    """
    print("\n" + "=" * 80)
    print("ETAPA 3: CORRECAO E SEGMENTACAO DA DESCRICAO")
    print("=" * 80)
    
    # Criar backup
    df = criar_backup_produto(df)
    
    # Reseta a coluna de trabalho para o estado original
    df['PRODUTO'] = df['PRODUTO_ORIGINAL']
    
    # Máscara para aplicar a lógica apenas nos produtos relevantes (com e sem acento)
    mask_genericos = df['STATUS'].isin(['GENERICO', 'GENERICO REFERENCIA', 'GENÉRICO', 'GENÉRICO REFERÊNCIA'])
    num_genericos = mask_genericos.sum()
    
    print(f"A logica de segmentacao sera aplicada a {num_genericos:,} registros (Genericos).")
    
    if num_genericos > 0:
        # Aplica a função de correção usando .loc para segurança
        df.loc[mask_genericos, 'PRODUTO'] = (
            df.loc[mask_genericos, 'PRODUTO_ORIGINAL']
            .astype(str)
            .apply(corrigir_e_segmentar_descricao)
        )
        
        print("\n[OK] Correcao e segmentacao da Descricao concluidas.")
        print("\nAmostra do resultado (comparando original e corrigido):")
        print(df.loc[mask_genericos, ['PRODUTO', 'PRODUTO_ORIGINAL']].sample(min(10, num_genericos)))
    else:
        print("[AVISO] Nenhum produto GENERICO encontrado para segmentacao.")
    
    return df

# ==============================================================================
#      ETAPA 4: APLICAÇÃO DO DICIONÁRIO SUGERIDO (FUZZY MATCHING)
# ==============================================================================

def aplicar_dicionario_sugerido(df):
    """
    Aplica correções do dicionário sugerido usando regex pré-compilado.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRODUTO'
        
    Returns:
        pandas.DataFrame: DataFrame com correções aplicadas
    """
    print("\n" + "=" * 80)
    print("ETAPA 4: APLICACAO DO DICIONARIO SUGERIDO")
    print("=" * 80)
    
    # Pré-compila os padrões de substituição
    dic_sugerido_regex = [
        (re.compile(rf"\b{re.escape(errado)}\b", flags=re.IGNORECASE), certo.upper())
        for errado, certo in DIC_SUGERIDO_PRODUTO.items()
    ]
    
    def aplicar_consolidacao(descricao):
        descricao = str(descricao).upper().strip()
        for padrao, certo in dic_sugerido_regex:
            descricao = padrao.sub(certo, descricao)
        return descricao
    
    print("Aplicando correcoes do dicionario sugerido...")
    df["PRODUTO"] = df["PRODUTO"].astype(str).apply(aplicar_consolidacao)
    
    print("[OK] Dicionario sugerido aplicado.")
    return df

# ==============================================================================
#      ETAPA 5: CORREÇÕES DIRECIONADAS
# ==============================================================================

def aplicar_correcoes_direcionadas_produto(df):
    """
    Aplica correções direcionadas usando lista de regras str.replace.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRODUTO'
        
    Returns:
        pandas.DataFrame: DataFrame com correções aplicadas
    """
    print("\n" + "=" * 80)
    print("ETAPA 5: CORRECOES DIRECIONADAS")
    print("=" * 80)
    
    print("Aplicando correcoes direcionadas da lista de regras...")
    
    textos_corrigidos = df['PRODUTO'].astype(str).copy()
    total_afetado = 0
    
    # Itera sobre a lista de regras com barra de progresso
    for pattern, replacement, use_regex in tqdm(CORRECOES_CONTAINS_PRODUTO, 
                                                 desc="Aplicando regras", 
                                                 ncols=100):
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
    
    # Limpeza final de espaços e resíduos
    textos_corrigidos = (
        textos_corrigidos
        .str.replace(r'\s*\+\s*', ' + ', regex=True)
        .str.replace(r'\+\s*\($', '', regex=True)
        .str.replace(r'^\s*\+\s*', '', regex=True)
        .str.replace(r'\s*\+\s*$', '', regex=True)
        .str.replace(r'\b(\w+)(?:\s+\1)+\b', r'\1', regex=True, case=False)
        .str.replace(r'\s{2,}', ' ', regex=True)
        .str.strip()
    )
    
    df['PRODUTO'] = textos_corrigidos.replace({'': np.nan, 'nan': np.nan})
    
    if total_afetado > 0:
        print(f"\n[OK] Correcoes direcionadas concluidas.")
    else:
        print("\nNenhuma linha foi afetada pelas regras atuais.")
    
    print("\nAmostra do resultado apos as correcoes:")
    print(df[['PRODUTO']].sample(min(10, len(df))))
    
    return df

# ==============================================================================
#      FUNÇÃO PRINCIPAL
# ==============================================================================

def processar_produto(df):
    """
    Executa todo o pipeline de processamento da coluna PRODUTO.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRODUTO'
        
    Returns:
        pandas.DataFrame: DataFrame com PRODUTO processado
    """
    print("=" * 80)
    print("PROCESSAMENTO DA COLUNA PRODUTO")
    print("=" * 80)
    
    # Fazer uma cópia para não modificar o original
    df_processado = df.copy()
    
    # Executar todas as etapas
    df_processado = remover_produtos_teste_tabelado(df_processado)
    df_processado = normalizar_status(df_processado)
    df_processado = aplicar_segmentacao_produto(df_processado)
    df_processado = aplicar_dicionario_sugerido(df_processado)
    df_processado = aplicar_correcoes_direcionadas_produto(df_processado)
    
    # NOVA ETAPA: Correcoes ortograficas e padronizacao de combinacoes
    df_processado = processar_correcoes_ortograficas(df_processado, colunas=['PRODUTO'])
    
    print("\n" + "=" * 80)
    print("[OK] PROCESSAMENTO DA COLUNA PRODUTO CONCLUIDO!")
    print("=" * 80)
    
    # Estatísticas finais
    total_unicos = df_processado['PRODUTO'].nunique()
    print(f"\nTotal de produtos unicos: {total_unicos:,}")
    
    return df_processado

def exportar_produtos_unicos(df, arquivo_saida='output/anvisa/produtos_unicos.txt'):
    """
    Exporta lista de produtos únicos para arquivo texto.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRODUTO'
        arquivo_saida (str): Nome do arquivo de saída
    """
    # Criar pasta output se nao existir
    os.makedirs(os.path.dirname(arquivo_saida), exist_ok=True)
    
    descricoes_unicas_ordenadas = df['PRODUTO'].dropna().unique()
    descricoes_unicas_ordenadas.sort()
    
    with open(arquivo_saida, "w", encoding='utf-8') as f:
        for descricao in descricoes_unicas_ordenadas:
            f.write(str(descricao) + "\n")
    
    print(f"\n[OK] Arquivo gerado: {arquivo_saida}")
    print(f"Total de produtos unicos: {len(descricoes_unicas_ordenadas):,}")

if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")