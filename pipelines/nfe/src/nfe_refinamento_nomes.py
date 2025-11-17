"""
Módulo: nfe_refinamento_nomes.py
Descrição: Aplica refinamento e limpeza avançada nos nomes extraídos
           usando regras de negócio, expansão de abreviações e correções fuzzy.
Autor: Pipeline ANVISA
Data: 2025-11-13
"""

import pandas as pd
import numpy as np
import json
import re
import os
from datetime import datetime
from tqdm.auto import tqdm

# ============================================================
# CARREGAMENTO DE RECURSOS
# ============================================================

def carregar_recursos_refinamento(
    caminho_base_anvisa: str = None,
    caminho_regras_letras: str = "support/regras_limpeza_letras.json",
    caminho_abreviacoes: str = "support/abreviacoes.json",
    caminho_regras_quimicas: str = "support/regras_quimicas.json",
    caminho_regras_negocio: str = "support/regras_de_negocio.json",
    caminho_fuzzy_matches: str = "support/fuzzy_matches.json"
) -> dict:
    """
    Carrega todos os recursos necessários para refinamento.
    
    Returns:
        Dicionário com todos os recursos carregados
    """
    print("\n" + "="*80)
    print("CARREGANDO RECURSOS PARA REFINAMENTO")
    print("="*80)
    
    recursos = {}
    
    # 1. Base mestre ANVISA (set de produtos válidos)
    try:
        # Tenta localizar arquivo ANVISA automaticamente
        if caminho_base_anvisa is None:
            # Procura parquet primeiro
            if os.path.exists("data/anvisa/dados_anvisa.parquet"):
                caminho_base_anvisa = "data/anvisa/dados_anvisa.parquet"
            # Procura CSV como fallback
            elif os.path.exists("data/anvisa/TA_PRECO_MEDICAMENTO.csv"):
                caminho_base_anvisa = "data/anvisa/TA_PRECO_MEDICAMENTO.csv"
            else:
                raise FileNotFoundError("Base ANVISA nao encontrada")
        
        if caminho_base_anvisa.endswith('.parquet'):
            df_anvisa = pd.read_parquet(caminho_base_anvisa)
        else:
            df_anvisa = pd.read_csv(caminho_base_anvisa, sep=';', encoding='utf-8-sig')
        
        recursos['set_produtos_master'] = set(df_anvisa['PRODUTO'].dropna().unique())
        print(f"[OK] Base ANVISA: {len(recursos['set_produtos_master']):,} produtos")
    except Exception as e:
        print(f"[AVISO] Falha ao carregar base ANVISA: {e}")
        print("[INFO] Continuando sem validacao contra base mestre")
        recursos['set_produtos_master'] = set()
    
    # 2. Regras de limpeza de letras
    try:
        with open(caminho_regras_letras, "r", encoding="utf-8") as f:
            regras_letras = json.load(f)
        recursos['letras_a_verificar'] = set(regras_letras.get("letras_a_verificar", []))
        recursos['termos_permitidos'] = set(regras_letras.get("termos_permitidos", []))
        print(f"[OK] Regras letras: {len(recursos['letras_a_verificar'])} letras")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar regras de letras: {e}")
        recursos['letras_a_verificar'] = set()
        recursos['termos_permitidos'] = set()
    
    # 3. Abreviações
    try:
        with open(caminho_abreviacoes, "r", encoding="utf-8") as f:
            abrev_json = json.load(f)
        recursos['abbreviation_mapping'] = abrev_json.get("abbreviation_mapping", {})
        
        # Compila regex para performance
        if recursos['abbreviation_mapping']:
            pattern = r'\b(' + '|'.join(map(re.escape, recursos['abbreviation_mapping'].keys())) + r')\b'
            recursos['abrev_pattern'] = re.compile(pattern, re.IGNORECASE)
        else:
            recursos['abrev_pattern'] = None
        
        print(f"[OK] Abreviacoes: {len(recursos['abbreviation_mapping'])} mapeamentos")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar abreviações: {e}")
        recursos['abbreviation_mapping'] = {}
        recursos['abrev_pattern'] = None
    
    # 4. Regras químicas
    try:
        with open(caminho_regras_quimicas, "r", encoding="utf-8") as f:
            regras_quimicas = json.load(f)
        recursos['termos_quimicos_set'] = set(regras_quimicas.get("termos_quimicos", []))
        print(f"[OK] Termos quimicos: {len(recursos['termos_quimicos_set'])} termos")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar regras químicas: {e}")
        recursos['termos_quimicos_set'] = set()
    
    # 5. Regras de negócio
    try:
        with open(caminho_regras_negocio, "r", encoding="utf-8") as f:
            recursos['regras_negocio_json'] = json.load(f)
        print(f"[OK] Regras de negocio: carregadas")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar regras de negócio: {e}")
        recursos['regras_negocio_json'] = {}
    
    # 6. Dicionário fuzzy
    try:
        with open(caminho_fuzzy_matches, "r", encoding="utf-8") as f:
            recursos['dicionario_correcoes_fuzzy'] = json.load(f)
        print(f"[OK] Correcoes fuzzy: {len(recursos['dicionario_correcoes_fuzzy'])} mapeamentos")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar correções fuzzy: {e}")
        recursos['dicionario_correcoes_fuzzy'] = {}
    
    print("="*80)
    
    return recursos


# ============================================================
# FUNÇÕES DE VERIFICAÇÃO
# ============================================================

def verificar_matches(df: pd.DataFrame, master_set: set, column: str, step_name: str = "") -> dict:
    """
    Calcula estatísticas de matching contra a base mestre.
    
    Returns:
        Dicionário com estatísticas
    """
    match_count = df[column].isin(master_set).sum()
    total_rows = len(df)
    match_percentage = (match_count / total_rows) * 100 if total_rows > 0 else 0
    
    stats = {
        'step': step_name,
        'matches': match_count,
        'total': total_rows,
        'percentage': match_percentage
    }
    
    print(f"\n--- Relatorio de Matches ({step_name}) ---")
    print(f"Coluna Verificada: '{column}'")
    print(f"Correspondencias Exatas: {match_count:,} de {total_rows:,} ({match_percentage:.2f}%)")
    print("-" * (28 + len(step_name)))
    
    return stats


# ============================================================
# FUNÇÕES DE LIMPEZA
# ============================================================

def limpar_letras_isoladas(nome_produto: str, letras_a_verificar: set, termos_permitidos: set) -> str:
    """
    Remove letras isoladas exceto quando precedidas de termos permitidos.
    """
    if not isinstance(nome_produto, str):
        return ""
    
    palavras = nome_produto.split()
    resultado = []
    
    for i, palavra_atual in enumerate(palavras):
        if palavra_atual in letras_a_verificar:
            # Mantém letra se precedida de termo permitido
            if i > 0 and palavras[i - 1] in termos_permitidos:
                resultado.append(palavra_atual)
        else:
            resultado.append(palavra_atual)
    
    return ' '.join(resultado)


def expandir_abreviacoes(nome_produto: str, abrev_pattern, abbreviation_mapping: dict) -> str:
    """
    Expande abreviações usando regex pré-compilado.
    """
    if not isinstance(nome_produto, str) or not abbreviation_mapping:
        return nome_produto
    
    return abrev_pattern.sub(
        lambda m: abbreviation_mapping.get(m.group(1).upper(), m.group(0)), 
        nome_produto
    )


def reestruturar_nome_quimico(nome_produto: str, termos_quimicos_set: set) -> str:
    """
    Reordena termos químicos para formato padronizado.
    Ex: "PRINCIPIO QUIMICO" -> "QUIMICO DE PRINCIPIO"
    """
    if not isinstance(nome_produto, str):
        return ""
    
    palavras = nome_produto.split()
    if len(palavras) < 2:
        return nome_produto
    
    # Reordenação se última palavra for termo químico
    if palavras[-1] in termos_quimicos_set:
        nome_reordenado = f"{palavras[-1]} DE {' '.join(palavras[:-1])}"
    else:
        nome_reordenado = nome_produto
    
    # Limpeza de duplicatas
    palavras = nome_reordenado.split()
    palavras_sem_duplicatas = [palavras[0]] if palavras else []
    
    for i in range(1, len(palavras)):
        if palavras[i] != palavras[i - 1]:
            palavras_sem_duplicatas.append(palavras[i])
    
    # Remove "DE" solto no início ou fim
    if palavras_sem_duplicatas and palavras_sem_duplicatas[0] == 'DE':
        palavras_sem_duplicatas.pop(0)
    if palavras_sem_duplicatas and palavras_sem_duplicatas[-1] == 'DE':
        palavras_sem_duplicatas.pop(-1)
    
    return ' '.join(palavras_sem_duplicatas)


def aplicar_regras_override_descricao(row: pd.Series, regras_negocio_json: dict) -> str:
    """
    Aplica regras de override baseadas na descrição original.
    """
    descricao = str(row['descricao_produto']).upper()
    current_limpo = row['NOME_PRODUTO_LIMPO']
    
    regras_override = regras_negocio_json.get("regras_override_descricao", [])
    
    for regra in regras_override:
        padroes = regra["padroes"]
        substituicao = regra["substituicao"]
        
        # Verifica se TODOS os padrões estão presentes
        if all(padrao.upper() in descricao for padrao in padroes):
            return substituicao
    
    return current_limpo


def aplicar_regras_negocio(nome_produto: str, regras_negocio_json: dict) -> str:
    """
    Aplica conjunto de regras de negócio em ordem específica.
    """
    if not isinstance(nome_produto, str):
        return ""
    
    # 1. Regras com múltiplas chaves
    regras_multi = regras_negocio_json.get("regras_substituicao_multi_chave", {})
    for chaves_str, substituicao in regras_multi.items():
        chaves = chaves_str.split(',')
        if all(chave in nome_produto for chave in chaves):
            return substituicao
    
    # 2. Regras condicionais complexas
    regras_complexas = regras_negocio_json.get("regras_condicionais_complexas", [])
    for regra in regras_complexas:
        contem_list = regra.get("contem", [])
        nao_contem_list = regra.get("nao_contem", [])
        if (all(termo in nome_produto for termo in contem_list) and
            not any(termo in nome_produto for termo in nao_contem_list)):
            return regra["substituir_por"]
    
    # 3. Regras com chave única
    regras_unica = regras_negocio_json.get("regras_substituicao_chave_unica", {})
    for chave, substituicao in regras_unica.items():
        if chave in nome_produto:
            return substituicao
    
    return nome_produto


# ============================================================
# PIPELINE DE REFINAMENTO
# ============================================================

def executar_refinamento_nomes(df: pd.DataFrame, recursos: dict) -> pd.DataFrame:
    """
    Executa pipeline completo de refinamento em cascata.
    """
    print("\n" + "="*80)
    print("INICIANDO PIPELINE DE REFINAMENTO DE NOMES")
    print("="*80)
    
    inicio = datetime.now()
    stats_list = []
    
    # Preparação
    df = df.copy()
    df = df[df['NOME_PRODUTO'] != 'DELETAR'].copy()
    df['NOME_PRODUTO_LIMPO'] = df['NOME_PRODUTO']
    
    stats = verificar_matches(
        df, recursos['set_produtos_master'], 
        'NOME_PRODUTO_LIMPO', 
        "Baseline Inicial"
    )
    stats_list.append(stats)
    
    # ETAPA 0.5: Override por descrição
    print("\n" + "="*80)
    print("ETAPA 0.5: OVERRIDE POR DESCRICAO")
    print("="*80)
    
    mask_nao_correspondido = ~df['NOME_PRODUTO_LIMPO'].isin(recursos['set_produtos_master'])
    print(f"[INFO] Processando {mask_nao_correspondido.sum():,} linhas...")
    
    tqdm.pandas(desc="Override Desc.")
    df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'] = \
        df.loc[mask_nao_correspondido].progress_apply(
            lambda row: aplicar_regras_override_descricao(row, recursos['regras_negocio_json']), 
            axis=1
        )
    
    stats = verificar_matches(df, recursos['set_produtos_master'], 'NOME_PRODUTO_LIMPO', "Apos Override")
    stats_list.append(stats)
    
    # ETAPA 1: Limpeza de letras isoladas
    print("\n" + "="*80)
    print("ETAPA 1: LIMPEZA DE LETRAS ISOLADAS")
    print("="*80)
    
    mask_nao_correspondido = ~df['NOME_PRODUTO_LIMPO'].isin(recursos['set_produtos_master'])
    print(f"[INFO] Processando {mask_nao_correspondido.sum():,} linhas...")
    
    tqdm.pandas(desc="Limpando Letras")
    df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'] = \
        df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'].progress_apply(
            lambda x: limpar_letras_isoladas(x, recursos['letras_a_verificar'], recursos['termos_permitidos'])
        )
    
    stats = verificar_matches(df, recursos['set_produtos_master'], 'NOME_PRODUTO_LIMPO', "Apos Limpeza Letras")
    stats_list.append(stats)
    
    # ETAPA 2: Expansão de abreviações
    print("\n" + "="*80)
    print("ETAPA 2: EXPANSAO DE ABREVIACOES")
    print("="*80)
    
    mask_nao_correspondido = ~df['NOME_PRODUTO_LIMPO'].isin(recursos['set_produtos_master'])
    print(f"[INFO] Processando {mask_nao_correspondido.sum():,} linhas...")
    
    if recursos['abrev_pattern']:
        tqdm.pandas(desc="Expandindo Abreviac.")
        df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'] = \
            df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'].progress_apply(
                lambda x: expandir_abreviacoes(x, recursos['abrev_pattern'], recursos['abbreviation_mapping'])
            )
    else:
        print("[AVISO] Sem abreviacoes para expandir")
    
    stats = verificar_matches(df, recursos['set_produtos_master'], 'NOME_PRODUTO_LIMPO', "Apos Abreviacoes")
    stats_list.append(stats)
    
    # ETAPA 3: Reestruturação química
    print("\n" + "="*80)
    print("ETAPA 3: REESTRUTURACAO QUIMICA")
    print("="*80)
    
    mask_nao_correspondido = ~df['NOME_PRODUTO_LIMPO'].isin(recursos['set_produtos_master'])
    print(f"[INFO] Processando {mask_nao_correspondido.sum():,} linhas...")
    
    tqdm.pandas(desc="Reestruturando")
    df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'] = \
        df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'].progress_apply(
            lambda x: reestruturar_nome_quimico(x, recursos['termos_quimicos_set'])
        )
    
    stats = verificar_matches(df, recursos['set_produtos_master'], 'NOME_PRODUTO_LIMPO', "Apos Reestruturacao")
    stats_list.append(stats)
    
    # ETAPA 4: Regras de negócio
    print("\n" + "="*80)
    print("ETAPA 4: REGRAS DE NEGOCIO")
    print("="*80)
    
    mask_nao_correspondido = ~df['NOME_PRODUTO_LIMPO'].isin(recursos['set_produtos_master'])
    print(f"[INFO] Processando {mask_nao_correspondido.sum():,} linhas...")
    
    tqdm.pandas(desc="Aplicando Regras")
    df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'] = \
        df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'].progress_apply(
            lambda x: aplicar_regras_negocio(x, recursos['regras_negocio_json'])
        )
    
    stats = verificar_matches(df, recursos['set_produtos_master'], 'NOME_PRODUTO_LIMPO', "Apos Regras Negocio")
    stats_list.append(stats)
    
    # ETAPA 5: Dicionário fuzzy
    print("\n" + "="*80)
    print("ETAPA 5: DICIONARIO FUZZY")
    print("="*80)
    
    mask_nao_correspondido = ~df['NOME_PRODUTO_LIMPO'].isin(recursos['set_produtos_master'])
    print(f"[INFO] Processando {mask_nao_correspondido.sum():,} linhas...")
    
    if recursos['dicionario_correcoes_fuzzy']:
        df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'] = \
            df.loc[mask_nao_correspondido, 'NOME_PRODUTO_LIMPO'].replace(recursos['dicionario_correcoes_fuzzy'])
    else:
        print("[AVISO] Dicionario fuzzy vazio")
    
    stats = verificar_matches(df, recursos['set_produtos_master'], 'NOME_PRODUTO_LIMPO', "Apos Dicionario Fuzzy")
    stats_list.append(stats)
    
    # ETAPA 6: Limpeza final
    print("\n" + "="*80)
    print("ETAPA 6: LIMPEZA FINAL")
    print("="*80)
    
    df['NOME_PRODUTO_LIMPO'] = (
        df['NOME_PRODUTO_LIMPO']
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip(';+ ')
        .str.slice(0, 100)
    )
    
    stats = verificar_matches(df, recursos['set_produtos_master'], 'NOME_PRODUTO_LIMPO', "Resultado Final")
    stats_list.append(stats)
    
    # Relatório final
    duracao = (datetime.now() - inicio).total_seconds()
    
    print("\n" + "="*80)
    print("RELATORIO DE REFINAMENTO")
    print("="*80)
    print(f"Total de linhas processadas: {len(df):,}")
    print(f"Tempo de execucao: {duracao:.2f}s")
    print("\nProgresso por etapa:")
    for stat in stats_list:
        print(f"  {stat['step']:<30} {stat['matches']:>6,} ({stat['percentage']:>6.2f}%)")
    
    melhoria = stats_list[-1]['matches'] - stats_list[0]['matches']
    melhoria_pct = (stats_list[-1]['percentage'] - stats_list[0]['percentage'])
    print(f"\nMelhoria total: +{melhoria:,} matches (+{melhoria_pct:.2f}%)")
    print("="*80)
    
    return df


# ============================================================
# FUNÇÃO PRINCIPAL
# ============================================================

def processar_refinamento_nomes(
    arquivo_entrada: str = None,
    diretorio_saida: str = "data/processed"
) -> pd.DataFrame:
    """
    Função principal para processar refinamento de nomes.
    """
    print("\n" + "="*80)
    print("ETAPA 11: REFINAMENTO DE NOMES")
    print("="*80)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*80)
    
    inicio_total = datetime.now()
    
    # Localizar arquivo de entrada
    if arquivo_entrada is None:
        print("\n[INFO] Procurando arquivo de entrada...")
        # Busca primeiro arquivo SEM timestamp
        arquivo_entrada = os.path.join(diretorio_saida, "df_etapa10_trabalhando_nomes.zip")
        
        if not os.path.exists(arquivo_entrada):
            # Fallback: procura com timestamp
            arquivos = sorted([
                f for f in os.listdir(diretorio_saida)
                if f.startswith("df_trabalhando_nomes_") and f.endswith(".zip")
            ], reverse=True)
            
            if not arquivos:
                print("[ERRO] Nenhum arquivo 'df_etapa10_trabalhando_nomes.zip' encontrado.")
                return None
            
            arquivo_entrada = os.path.join(diretorio_saida, arquivos[0])
    
    print(f"[OK] Arquivo: {os.path.basename(arquivo_entrada)}")
    
    # Carregar dados
    print(f"\n[INFO] Carregando dados...")
    df = pd.read_csv(arquivo_entrada, sep=';', encoding='utf-8-sig')
    print(f"   [OK] Shape: {df.shape}")
    
    # Carregar recursos
    recursos = carregar_recursos_refinamento()
    
    # Executar refinamento
    df = executar_refinamento_nomes(df, recursos)
    
    # Salvar resultado
    nome_saida = f"df_etapa11_trabalhando_refinado.zip"
    caminho_saida = os.path.join(diretorio_saida, nome_saida)
    
    print(f"\n[INFO] Salvando resultado...")
    df.to_csv(
        caminho_saida,
        sep=';',
        index=False,
        encoding='utf-8-sig',
        compression={
            'method': 'zip',
            'archive_name': f"df_trabalhando_refinado.csv"
        }
    )
    
    tamanho = os.path.getsize(caminho_saida) / (1024 * 1024)
    print(f"[OK] Arquivo salvo: {nome_saida} ({tamanho:.2f} MB)")
    
    duracao_total = (datetime.now() - inicio_total).total_seconds()
    print("\n" + "="*80)
    print("[SUCESSO] ETAPA 11 CONCLUIDA!")
    print("="*80)
    print(f"[INFO] Tempo total: {duracao_total:.2f}s")
    print("="*80)
    
    return df


# ============================================================
# SCRIPT STANDALONE
# ============================================================

if __name__ == "__main__":
    import sys
    
    try:
        df_resultado = processar_refinamento_nomes()
        
        if df_resultado is not None:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
