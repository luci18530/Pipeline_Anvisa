"""
Módulo: nfe_extracao_nomes.py
Descrição: Extrai nomes de produtos de descrições textuais usando
           dicionário de mapeamento e regras de parada.
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
from paths import SUPPORT_DIR

# ============================================================
# CARREGAMENTO DE RECURSOS
# ============================================================

def carregar_recursos(
    caminho_produtos: str = str(SUPPORT_DIR / "produtos.json"),
    caminho_ignorados: str = str(SUPPORT_DIR / "termos_ignorados.json"),
    caminho_parada: str = str(SUPPORT_DIR / "termos_de_parada.json")
) -> tuple:
    """
    Carrega arquivos JSON com recursos para extração de nomes.
    
    Args:
        caminho_produtos: Caminho para dicionário de produtos
        caminho_ignorados: Caminho para termos a ignorar no início
        caminho_parada: Caminho para termos de parada
        
    Returns:
        Tupla (produtos_dict, termos_ignorados_set, termos_parada_set, produtos_regex)
    """
    print("\n" + "="*80)
    print("CARREGANDO RECURSOS PARA EXTRACAO DE NOMES")
    print("="*80)
    
    # Carregar produtos (dicionário de mapeamento)
    try:
        with open(caminho_produtos, "r", encoding="utf-8") as f:
            produtos_dict = json.load(f)
        print(f"[OK] Produtos carregados: {len(produtos_dict)} mapeamentos")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar produtos: {e}")
        produtos_dict = {}
    
    # Carregar termos ignorados
    try:
        with open(caminho_ignorados, "r", encoding="utf-8") as f:
            termos_ignorados_json = json.load(f)
        termos_ignorados_set = set(termos_ignorados_json.get("termos_ignorados_inicio", []))
        print(f"[OK] Termos ignorados: {len(termos_ignorados_set)} termos")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar termos ignorados: {e}")
        termos_ignorados_set = set()
    
    # Carregar termos de parada
    try:
        with open(caminho_parada, "r", encoding="utf-8") as f:
            termos_parada_json = json.load(f)
        termos_parada_set = set(termos_parada_json.get("termos_de_parada", []))
        print(f"[OK] Termos de parada: {len(termos_parada_set)} termos")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar termos de parada: {e}")
        termos_parada_set = set()
    
    # Compilar regex para produtos (otimização)
    produtos_regex = None
    if produtos_dict:
        try:
            produtos_pattern = r'\b(' + '|'.join(re.escape(k) for k in produtos_dict.keys()) + r')\b'
            produtos_regex = re.compile(produtos_pattern, re.IGNORECASE)
            print(f"[OK] Regex de produtos compilado")
        except Exception as e:
            print(f"[AVISO] Falha ao compilar regex: {e}")
            produtos_regex = None
    
    print("="*80)
    
    return produtos_dict, termos_ignorados_set, termos_parada_set, produtos_regex


def carregar_recursos_extracao(
    caminho_produtos: str = str(SUPPORT_DIR / "produtos.json"),
    caminho_ignorados: str = str(SUPPORT_DIR / "termos_ignorados.json"),
    caminho_parada: str = str(SUPPORT_DIR / "termos_de_parada.json")
):
    """Wrapper compatível com chamadas antigas."""
    return carregar_recursos(caminho_produtos, caminho_ignorados, caminho_parada)


# ============================================================
# FUNÇÕES DE PRÉ-PROCESSAMENTO
# ============================================================

def preprocessar_descricoes(series: pd.Series) -> pd.Series:
    """
    Limpa e padroniza descrições usando operações vetorizadas.
    
    Args:
        series: Series com descrições brutas
        
    Returns:
        Series com descrições limpas
    """
    print("\n[INFO] Preprocessando descricoes...")
    
    # Regex para remover palavras inteiras como 'ID' ou 'ITEM'
    remover_palavras_pattern = r'\b(ID|ITEM)\b'
    
    processed_series = (
        series.astype(str)
        .str.strip()
        .str.upper()  # Padroniza para maiúsculas
        .str.replace(remover_palavras_pattern, '', regex=True)
        .str.removeprefix("C 1 ")  # Remove prefixo comum
        .str.replace(r'\s+', ' ', regex=True)  # Consolida espaços
        .str.strip()
    )
    
    print(f"[OK] {len(processed_series)} descricoes preprocessadas")
    
    return processed_series


# ============================================================
# FUNÇÕES DE EXTRAÇÃO
# ============================================================

# Regex para casos especiais (letras isoladas com símbolos)
LETRA_ESPECIAL_REGEX = re.compile(r"^[E-FKT](?:[/\\;,.:]*)$", re.IGNORECASE)

def extrair_nome_logica(
    descricao: str,
    produtos_dict: dict,
    termos_ignorados_set: set,
    termos_parada_set: set
) -> str:
    """
    Extrai nome do medicamento usando lógica de regras.
    
    Args:
        descricao: Descrição do produto (já preprocessada)
        produtos_dict: Dicionário de mapeamento direto
        termos_ignorados_set: Termos a ignorar no início
        termos_parada_set: Termos que indicam fim do nome
        
    Returns:
        Nome extraído do produto
    """
    if not isinstance(descricao, str) or not descricao:
        return ""
    
    # Extração baseada em regras
    palavras = descricao.split()
    
    # Remove termos ignorados do início
    while palavras and palavras[0] in termos_ignorados_set:
        palavras.pop(0)
    
    medicamento = []
    for palavra in palavras:
        # Condição de parada: termo de parada ou contém números
        if palavra in termos_parada_set or any(char.isdigit() for char in palavra):
            break
        
        # Tratamento de caso especial (ex: 'F;')
        if LETRA_ESPECIAL_REGEX.match(palavra):
            medicamento.append(palavra[0])
            break
        
        medicamento.append(palavra)
    
    return ' '.join(medicamento)


# ============================================================
# PIPELINE DE EXTRAÇÃO
# ============================================================

def executar_extracao_nomes(
    df: pd.DataFrame,
    coluna_descricao: str = "descricao_produto",
    produtos_dict: dict = None,
    termos_ignorados_set: set = None,
    termos_parada_set: set = None,
    produtos_regex = None
) -> pd.DataFrame:
    """
    Executa pipeline completo de extração de nomes.
    
    Args:
        df: DataFrame com coluna de descrições
        coluna_descricao: Nome da coluna com descrições
        produtos_dict: Dicionário de mapeamento (se None, carrega)
        termos_ignorados_set: Set de termos ignorados (se None, carrega)
        termos_parada_set: Set de termos de parada (se None, carrega)
        produtos_regex: Regex compilado (se None, carrega)
        
    Returns:
        DataFrame com coluna NOME_PRODUTO adicionada
    """
    print("\n" + "="*80)
    print("INICIANDO PIPELINE DE EXTRACAO DE NOMES")
    print("="*80)
    
    inicio = datetime.now()
    
    # Validação
    if coluna_descricao not in df.columns:
        print(f"[ERRO] Coluna '{coluna_descricao}' nao encontrada!")
        return df
    
    # Carregar recursos se não fornecidos
    if produtos_dict is None:
        recursos = carregar_recursos_extracao()
        produtos_dict, termos_ignorados_set, termos_parada_set, produtos_regex = recursos
    
    # Etapa 1: Pré-processamento vetorizado
    print("\n" + "="*80)
    print("ETAPA 1: PRE-PROCESSAMENTO")
    print("="*80)
    df['descricao_limpa'] = preprocessar_descricoes(df[coluna_descricao])
    
    # Etapa 2.1: Match rápido vetorizado com dicionário
    print("\n" + "="*80)
    print("ETAPA 2.1: MATCH RAPIDO (VETORIZADO)")
    print("="*80)
    
    df['NOME_PRODUTO'] = pd.NA
    matched_count = 0
    
    if produtos_regex:
        print("[INFO] Executando busca vetorizada com regex...")
        matches_rapidos = df['descricao_limpa'].str.extract(produtos_regex, expand=False)
        df['NOME_PRODUTO'] = matches_rapidos.map(produtos_dict)
        matched_count = df['NOME_PRODUTO'].notna().sum()
        print(f"[OK] {matched_count:,} linhas resolvidas na primeira passagem")
    else:
        print("[AVISO] Dicionario de produtos vazio, pulando primeira passagem")
    
    # Etapa 2.2: Lógica complexa com barra de progresso
    print("\n" + "="*80)
    print("ETAPA 2.2: LOGICA DETALHADA (COM PROGRESSO)")
    print("="*80)
    
    mask_nao_resolvido = df['NOME_PRODUTO'].isna()
    remaining_count = mask_nao_resolvido.sum()
    
    if remaining_count > 0:
        print(f"[INFO] Aplicando logica em {remaining_count:,} linhas restantes...")
        
        # Inicializa tqdm para Pandas
        tqdm.pandas(desc="Extraindo Nomes")
        
        # Aplica função com progress bar apenas no subconjunto
        resultados_lentos = df.loc[mask_nao_resolvido, 'descricao_limpa'].progress_apply(
            lambda x: extrair_nome_logica(
                x, 
                produtos_dict, 
                termos_ignorados_set, 
                termos_parada_set
            )
        )
        
        df.loc[mask_nao_resolvido, 'NOME_PRODUTO'] = resultados_lentos
        print(f"[OK] {remaining_count:,} linhas processadas com logica detalhada")
    else:
        print("[OK] Todas as linhas resolvidas na primeira passagem")
    
    # Etapa 3: Limpeza final
    print("\n" + "="*80)
    print("ETAPA 3: LIMPEZA FINAL")
    print("="*80)
    
    df = df.drop(columns=['descricao_limpa'])
    df['NOME_PRODUTO'] = df['NOME_PRODUTO'].fillna('')
    
    # Estatísticas finais
    total_extraido = (df['NOME_PRODUTO'] != '').sum()
    total_vazio = (df['NOME_PRODUTO'] == '').sum()
    
    duracao = (datetime.now() - inicio).total_seconds()
    
    print("\n" + "="*80)
    print("RELATORIO DE EXTRACAO")
    print("="*80)
    print(f"Total de linhas processadas:     {len(df):,}")
    print(f"  - Resolvidas via busca rapida: {matched_count:,}")
    print(f"  - Resolvidas via logica:       {remaining_count:,}")
    print(f"  - Nomes extraidos:             {total_extraido:,} ({total_extraido/len(df)*100:.2f}%)")
    print(f"  - Vazios:                      {total_vazio:,} ({total_vazio/len(df)*100:.2f}%)")
    print(f"\n[INFO] Tempo de execucao: {duracao:.2f}s")
    print("="*80)
    
    return df


# ============================================================
# FUNÇÃO PRINCIPAL
# ============================================================

def processar_extracao_nomes(
    arquivo_entrada: str = None,
    diretorio_saida: str = "data/processed"
) -> pd.DataFrame:
    """
    Função principal para processar extração de nomes.
    
    Args:
        arquivo_entrada: Caminho do arquivo df_trabalhando_*.zip
        diretorio_saida: Diretório para salvar resultado
        
    Returns:
        DataFrame com coluna NOME_PRODUTO
    """
    print("\n" + "="*80)
    print("ETAPA 10: EXTRACAO DE NOMES DE PRODUTOS")
    print("="*80)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*80)
    
    inicio_total = datetime.now()
    
    # Localizar arquivo de entrada se não especificado
    if arquivo_entrada is None:
        print("\n[INFO] Procurando arquivo de entrada...")
        # Busca primeiro arquivo SEM timestamp
        arquivo_entrada = os.path.join(diretorio_saida, "df_etapa09_trabalhando.zip")
        
        if not os.path.exists(arquivo_entrada):
            # Fallback: procura com timestamp
            arquivos = sorted([
                f for f in os.listdir(diretorio_saida)
                if f.startswith("df_trabalhando_") and f.endswith(".zip")
            ], reverse=True)
            
            if not arquivos:
                print("[ERRO] Nenhum arquivo 'df_etapa09_trabalhando.zip' encontrado.")
                return None
            
            arquivo_entrada = os.path.join(diretorio_saida, arquivos[0])
    
    tamanho_mb = os.path.getsize(arquivo_entrada) / (1024 * 1024)
    print(f"[OK] Arquivo encontrado:")
    print(f"   Nome: {os.path.basename(arquivo_entrada)}")
    print(f"   Tamanho: {tamanho_mb:.2f} MB")
    
    # Carregar dados
    print(f"\n[INFO] Carregando dados...")
    df = pd.read_csv(arquivo_entrada, sep=';', encoding='utf-8-sig')
    print(f"   [OK] Carregado com sucesso!")
    print(f"   Shape: {df.shape}")
    
    # Executar extração
    df = executar_extracao_nomes(df)
    
    # Salvar resultado
    nome_saida = f"df_etapa10_trabalhando_nomes.zip"
    caminho_saida = os.path.join(diretorio_saida, nome_saida)
    
    print(f"\n[INFO] Salvando resultado...")
    df.to_csv(
        caminho_saida,
        sep=';',
        index=False,
        encoding='utf-8-sig',
        compression={
            'method': 'zip',
            'archive_name': f"df_trabalhando_nomes.csv"
        }
    )
    
    tamanho_saida = os.path.getsize(caminho_saida) / (1024 * 1024)
    print(f"[OK] Arquivo salvo:")
    print(f"   Nome: {nome_saida}")
    print(f"   Tamanho: {tamanho_saida:.2f} MB")
    
    # Estatísticas de nomes extraídos
    print("\n" + "="*80)
    print("TOP 20 NOMES EXTRAIDOS")
    print("="*80)
    top_nomes = df['NOME_PRODUTO'].value_counts().head(20)
    for i, (nome, count) in enumerate(top_nomes.items(), 1):
        print(f"{i:2d}. {nome:<40} {count:>6,} ocorrencias")
    
    # Tempo total
    duracao_total = (datetime.now() - inicio_total).total_seconds()
    print("\n" + "="*80)
    print("[SUCESSO] ETAPA 10 CONCLUIDA!")
    print("="*80)
    print(f"[INFO] Tempo total de execucao: {duracao_total:.2f}s")
    print("="*80)
    
    return df


# ============================================================
# SCRIPT STANDALONE
# ============================================================

if __name__ == "__main__":
    import sys
    
    try:
        df_resultado = processar_extracao_nomes()
        
        if df_resultado is not None:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
