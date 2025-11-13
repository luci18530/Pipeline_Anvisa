"""
Módulo de processamento de datas e categorização de vencimento de Notas Fiscais
Adaptado do pipeline Colab para ambiente local
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime


def limpar_datas(serie):
    """
    Limpa e padroniza uma série de datas com diversos formatos.
    
    - Remove espaços em branco
    - Converte valores inválidos para NaT
    - Formata YYYYMMDD para YYYY-MM-DD
    - Remove datas placeholder (2000-01-01, 2010-01-01, 2020-01-01)
    
    Parâmetros:
    - serie: pandas Series com datas em diversos formatos
    
    Retorna:
    - pandas Series com datas em datetime64
    """
    print(f"[INFO] Limpando série de {len(serie)} valores...")
    
    # Converter para string e remover espaços
    serie = serie.astype(str).str.strip()
    
    # Substituir valores inválidos por NaN
    serie = serie.replace(["-1", "", "nan", "None", "NaT", "NULL", "null"], pd.NA)
    
    # Se tiver 8 dígitos (YYYYMMDD), inserir hífens
    mascara_yyyymmdd = serie.str.match(r"^\d{8}$", na=False)
    serie.loc[mascara_yyyymmdd] = (
        serie[mascara_yyyymmdd].str.slice(0, 4) + "-" +
        serie[mascara_yyyymmdd].str.slice(4, 6) + "-" +
        serie[mascara_yyyymmdd].str.slice(6, 8)
    )
    
    # Converter para datetime
    datas = pd.to_datetime(serie, errors="coerce", format="mixed")
    
    # Remover datas placeholder (provavelmente dados inválidos)
    datas = datas.mask(datas == pd.Timestamp("2000-01-01"))
    datas = datas.mask(datas == pd.Timestamp("2010-01-01"))
    datas = datas.mask(datas == pd.Timestamp("2020-01-01"))
    
    # Estatísticas
    natos = datas.isna().sum()
    print(f"[INFO] Conversão concluída: {natos} valores inválidos (NaT)")
    
    return datas


def calcular_metricas_vencimento(df):
    """
    Calcula métricas de vencimento baseadas em datas de fabricação, validade e emissão.
    
    Parâmetros:
    - df: DataFrame com colunas dt_fabricacao, dt_validade, dt_emissao
    
    Retorna:
    - DataFrame com colunas adicionadas:
      - vida_total: dias entre fabricação e validade
      - vida_usada: dias entre fabricação e emissão
      - dias_restantes: dias entre emissão e validade
      - vida_usada_porcento: percentual de vida usada
    """
    print("[INFO] Calculando métricas de vencimento...")
    
    df['vida_total'] = (df['dt_validade'] - df['dt_fabricacao']).dt.days
    df['vida_usada'] = (df['dt_emissao'] - df['dt_fabricacao']).dt.days.clip(lower=0)
    df['dias_restantes'] = (df['dt_validade'] - df['dt_emissao']).dt.days
    
    # Calcular percentual de vida usada (evitar divisão por zero)
    df['vida_usada_porcento'] = np.where(
        df['vida_total'] > 0,
        df['vida_usada'] / df['vida_total'],
        np.nan
    )
    
    print("[OK] Métricas calculadas")
    return df


def categorizar_vencimento(df):
    """
    Categoriza o status de vencimento dos produtos baseado em métricas de vida.
    
    Categorias:
    - VENCIDO: data de emissão > data de validade
    - MUITO PROXIMO AO VENCIMENTO: ≥75% de vida usada E <365 dias restantes
    - PROXIMO AO VENCIMENTO: 25-75% de vida usada E <365 dias restantes
    - PRAZO ACEITAVEL: <75% de vida usada OU >365 dias restantes
    - INDETERMINADO: dados insuficientes ou inválidos
    
    Parâmetros:
    - df: DataFrame com colunas de métricas de vencimento
    
    Retorna:
    - DataFrame com coluna 'categoria_vencimento' adicionada
    """
    print("[INFO] Categorizando vencimentos...")
    
    # Definir condições de categorização
    cond_vencido = df['dt_emissao'] > df['dt_validade']
    cond_muito_prox = (df['vida_usada_porcento'] >= 0.75) & (df['dias_restantes'] < 365)
    cond_prox = (df['vida_usada_porcento'] >= 0.25) & (df['vida_usada_porcento'] < 0.75) & (df['dias_restantes'] < 365)
    cond_aceitavel = (df['vida_usada_porcento'] < 0.75) | (df['dias_restantes'] > 365)
    
    # Condições para indeterminado
    cond_indeterminado_extra = (df['dias_restantes'] < -3650) | (df['vida_total'] == 0)
    
    # Classificação usando np.select
    df['categoria_vencimento'] = np.select(
        [cond_vencido, cond_muito_prox, cond_prox, cond_aceitavel],
        ['VENCIDO', 'MUITO PROXIMO AO VENCIMENTO', 'PROXIMO AO VENCIMENTO', 'PRAZO ACEITAVEL'],
        default='INDETERMINADO'
    )
    
    # Forçar INDETERMINADO para casos extras
    df.loc[cond_indeterminado_extra, 'categoria_vencimento'] = 'INDETERMINADO'
    
    # Estatísticas
    print("[OK] Categorização concluída")
    print("\nDistribuição de categorias:")
    print(df['categoria_vencimento'].value_counts())
    
    return df


def processar_vencimento_nfe(df):
    """
    Pipeline completo de processamento de vencimento de NFe.
    
    1. Limpa datas (fabricação, validade, emissão)
    2. Calcula métricas de vida útil
    3. Categoriza status de vencimento
    4. Particiona dados em tabela base e tabela de vencimento
    
    Parâmetros:
    - df: DataFrame com colunas de data
    
    Retorna:
    - tuple: (df_base, df_venc) - dados base sem métricas, dados de vencimento
    """
    print("="*60)
    print("[INICIO] Processamento de Vencimento")
    print("="*60)
    print(f"[INFO] Tamanho inicial: {len(df):,} registros\n")
    
    # Validar colunas necessárias
    colunas_necessarias = ['id_data_fabricacao', 'id_data_validade', 'data_emissao', 'chave_codigo']
    colunas_faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if colunas_faltantes:
        raise ValueError(f"Colunas faltantes: {colunas_faltantes}")
    
    # 1. Limpar datas
    print("[ETAPA 1/4] Limpando datas...")
    df['dt_fabricacao'] = limpar_datas(df['id_data_fabricacao'])
    df['dt_validade'] = limpar_datas(df['id_data_validade'])
    df['dt_emissao'] = limpar_datas(df['data_emissao'])
    print()
    
    # 2. Calcular métricas
    print("[ETAPA 2/4] Calculando métricas...")
    df = calcular_metricas_vencimento(df)
    print()
    
    # 3. Categorizar
    print("[ETAPA 3/4] Categorizando...")
    df = categorizar_vencimento(df)
    print()
    
    # 4. Particionar dados
    print("[ETAPA 4/4] Particionando dados...")
    
    # Validar chave_codigo
    if 'chave_codigo' not in df.columns:
        raise ValueError("Coluna 'chave_codigo' não existe no DataFrame")
    
    # Remover duplicatas por chave_codigo (manter primeira ocorrência)
    duplicatas = df['chave_codigo'].duplicated().sum()
    if duplicatas > 0:
        print(f"[AVISO] {duplicatas} duplicatas encontradas em 'chave_codigo'")
        print("[INFO] Mantendo primeira ocorrência por chave")
        df = df.drop_duplicates(subset=['chave_codigo'], keep='first')
    
    # Criar ID de vencimento (usar chave_codigo)
    df['id_venc'] = df['chave_codigo'].astype(str)
    
    # Colunas de vencimento
    colunas_venc = [
        'id_venc',
        'dt_fabricacao',
        'dt_validade',
        'dt_emissao',
        'vida_total',
        'vida_usada',
        'dias_restantes',
        'vida_usada_porcento',
        'categoria_vencimento'
    ]
    
    # Particionar
    df_venc = df[colunas_venc].copy()
    df_base = df.drop(
        columns=['dt_fabricacao', 'dt_validade', 'dt_emissao', 
                 'vida_total', 'vida_usada', 'dias_restantes', 
                 'vida_usada_porcento', 'categoria_vencimento', 'id_venc'],
        errors='ignore'
    ).copy()
    
    print(f"[OK] Particionamento concluído")
    print(f"  - df_base: {len(df_base):,} registros, {len(df_base.columns)} colunas")
    print(f"  - df_venc: {len(df_venc):,} registros, {len(df_venc.columns)} colunas")
    
    print("\n" + "="*60)
    print("[SUCESSO] Processamento de Vencimento Concluído")
    print("="*60)
    
    return df_base, df_venc


def salvar_dados_vencimento(df_venc, diretorio='data/processed', formato='csv'):
    """
    Salva tabela de vencimento processada em CSV.
    
    Parâmetros:
    - df_venc: DataFrame com dados de vencimento
    - diretorio: diretório de destino
    - formato: sempre 'csv' (parquet removido)
    
    Retorna:
    - Caminho do arquivo salvo
    """
    os.makedirs(diretorio, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo = f"nfe_vencimento_{timestamp}.csv"
    caminho = os.path.join(diretorio, arquivo)
    
    df_venc.to_csv(caminho, sep=';', index=False, encoding='utf-8')
    
    print(f"[OK] Dados de vencimento salvos em: {caminho}")
    return caminho


# Exemplo de uso
if __name__ == "__main__":
    import glob
    
    # Encontrar arquivo processado mais recente
    arquivos = glob.glob("data/processed/nfe_processado_*.csv")
    
    if not arquivos:
        print("[ERRO] Nenhum arquivo processado encontrado")
        print("[INFO] Execute primeiro: python scripts/processar_nfe.py")
        exit(1)
    
    arquivo_entrada = max(arquivos, key=os.path.getmtime)
    print(f"[INFO] Carregando: {arquivo_entrada}\n")
    
    # Carregar
    df = pd.read_csv(arquivo_entrada, sep=';', dtype=str)
    
    # Processar vencimento
    df_base, df_venc = processar_vencimento_nfe(df)
    
    # Salvar
    caminho_venc = salvar_dados_vencimento(df_venc, formato='csv')
    
    print("\n[SUCESSO] Processamento concluído!")
    print(f"Arquivos gerados:")
    print(f"  - CSV: {caminho_venc}")
