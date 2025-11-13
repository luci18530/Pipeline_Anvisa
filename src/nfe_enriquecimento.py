"""
Módulo de enriquecimento de dados com informações de município
Adiciona nome do município baseado no código IBGE
"""

import pandas as pd
import os
from datetime import datetime


# ============================================================
# CONFIGURAÇÕES
# ============================================================

SUPPORT_DIR = "support"
CODIGO_MUNICIPIO_FILE = os.path.join(SUPPORT_DIR, "codigomunicipio.xlsx")


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def verificar_arquivo_codigos():
    """Verifica se o arquivo de códigos de município existe"""
    if not os.path.exists(CODIGO_MUNICIPIO_FILE):
        raise FileNotFoundError(
            f"[ERRO] Arquivo de códigos de município não encontrado!\n"
            f"[INFO] Coloque o arquivo em: {CODIGO_MUNICIPIO_FILE}\n"
            f"[INFO] Arquivo esperado: codigomunicipio.xlsx"
        )
    return True


def carregar_codigos_municipio():
    """Carrega o arquivo Excel com códigos de município"""
    print(f"[INFO] Carregando códigos de município de: {CODIGO_MUNICIPIO_FILE}")
    
    try:
        df_codigos = pd.read_excel(CODIGO_MUNICIPIO_FILE)
        print(f"[OK] {len(df_codigos):,} códigos de município carregados")
        print(f"[INFO] Colunas: {', '.join(df_codigos.columns.tolist())}")
        
        return df_codigos
    except Exception as e:
        raise Exception(f"Erro ao carregar arquivo de códigos: {e}")


def enriquecer_com_municipios(df, df_codigos):
    """
    Enriquece DataFrame com informações de município
    
    Parâmetros:
        df (DataFrame): DataFrame com dados de NFe
        df_codigos (DataFrame): DataFrame com códigos de município
        
    Retorna:
        DataFrame: DataFrame enriquecido
    """
    print("="*60)
    print("[INICIO] Enriquecimento com Dados de Município")
    print("="*60 + "\n")
    
    df_trabalho = df.copy()
    
    # 1. Converter código para numérico
    print("[INFO] Convertendo 'codigo_municipio_destinatario' para numérico...")
    df_trabalho['codigo_municipio_destinatario'] = pd.to_numeric(
        df_trabalho['codigo_municipio_destinatario'],
        errors='coerce'
    ).astype('Int64')
    
    # 2. Mesclar dados de município
    print("[INFO] Mesclando dados de município...")
    registros_antes = len(df_trabalho)
    
    df_trabalho = df_trabalho.merge(
        df_codigos,
        left_on='codigo_municipio_destinatario',
        right_on='codigo_municipio_destinatario',
        how='left'
    )
    
    registros_depois = len(df_trabalho)
    print(f"[OK] Merge concluido: {registros_antes:,} -> {registros_depois:,} registros")
    
    # 3. Verificar matches
    municipios_preenchidos = df_trabalho['municipio'].notna().sum()
    pct_match = (municipios_preenchidos / registros_depois) * 100
    print(f"[INFO] Municípios preenchidos: {municipios_preenchidos:,} ({pct_match:.1f}%)")
    
    if pct_match < 95:
        print(f"[AVISO] Menos de 95% dos registros foram enriquecidos!")
        print(f"[AVISO] {registros_depois - municipios_preenchidos:,} registros sem município")
    
    # 4. Normalizar coluna de município
    print("[INFO] Normalizando nomes de município...")
    df_trabalho['municipio'] = (
        df_trabalho['municipio']
        .astype(str)
        .str.strip()
        .str.upper()
    )
    
    # 5. Reorganizar colunas
    print("[INFO] Reorganizando colunas...")
    
    # Remover código_municipio_destinatario duplicado (se existir)
    cols_duplicadas = df_trabalho.columns[df_trabalho.columns.duplicated()].tolist()
    if cols_duplicadas:
        print(f"[INFO] Removendo colunas duplicadas: {cols_duplicadas}")
        df_trabalho = df_trabalho.loc[:, ~df_trabalho.columns.duplicated()]
    
    # Mover 'municipio' para próximo a 'codigo_municipio_destinatario'
    if 'municipio' in df_trabalho.columns and 'codigo_municipio_destinatario' in df_trabalho.columns:
        cols = df_trabalho.columns.tolist()
        municipio_col = cols.pop(cols.index('municipio'))
        
        # Encontrar posição do código_municipio
        codigo_idx = cols.index('codigo_municipio_destinatario')
        cols.insert(codigo_idx + 1, municipio_col)
        
        df_trabalho = df_trabalho[cols]
        print("[OK] Coluna 'municipio' posicionada após código")
    
    print("="*60)
    print("[SUCESSO] Enriquecimento concluído")
    print("="*60)
    
    return df_trabalho


def processar_enriquecimento_nfe(arquivo_entrada):
    """
    Processa enriquecimento completo de NFe com dados de município
    
    Parâmetros:
        arquivo_entrada (str): Caminho do arquivo limpo processado
        
    Retorna:
        tuple: (df_enriquecido, caminho_arquivo_salvo)
    """
    print("="*60)
    print("Pipeline de Enriquecimento de Dados de NFe")
    print("="*60 + "\n")
    
    # Verificar arquivo de códigos
    print("[VALIDANDO] Arquivo de códigos de município...")
    verificar_arquivo_codigos()
    print("[OK] Arquivo encontrado!\n")
    
    # Carregar dados
    print("[INFO] Carregando dados de NFe...")
    df = pd.read_csv(arquivo_entrada, sep=';')
    print(f"[OK] {len(df):,} registros carregados\n")
    
    # Carregar códigos de município
    df_codigos = carregar_codigos_municipio()
    print()
    
    # Enriquecer com município
    df_enriquecido = enriquecer_com_municipios(df, df_codigos)
    
    # Estatísticas
    print("\n" + "="*60)
    print("Estatísticas de Enriquecimento")
    print("="*60)
    print(f"Total de registros: {len(df_enriquecido):,}")
    if 'municipio' in df_enriquecido.columns:
        municipios_unicos = df_enriquecido['municipio'].nunique()
        print(f"Municípios únicos: {municipios_unicos:,}")
        
        # Top 5 municípios
        print("\nTop 5 Municípios:")
        top_municipios = df_enriquecido['municipio'].value_counts().head(5)
        for mun, count in top_municipios.items():
            pct = (count / len(df_enriquecido)) * 100
            print(f"  {count:>6} ({pct:>5.2f}%) - {mun}")
    
    # Salvar dados
    print("\n" + "="*60)
    print("Salvando Dados Enriquecidos")
    print("="*60 + "\n")
    
    os.makedirs("data/processed", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    caminho_csv = os.path.join("data/processed", f"nfe_enriquecido_{timestamp}.csv")
    df_enriquecido.to_csv(caminho_csv, sep=';', index=False, encoding='utf-8-sig')
    print(f"[OK] Dados enriquecidos salvos em: {caminho_csv}")
    
    print("\n" + "="*60)
    print("[SUCESSO] Pipeline concluído com sucesso!")
    print("="*60)
    print(f"\nArquivos gerados:")
    print(f"  - CSV: {caminho_csv}")
    
    return df_enriquecido, caminho_csv


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    import glob
    
    # Encontrar arquivo limpo mais recente
    arquivos = glob.glob("data/processed/nfe_limpo_*.csv")
    
    if not arquivos:
        print("[ERRO] Nenhum arquivo limpo encontrado!")
        print("[INFO] Execute primeiro: python scripts/processar_limpeza.py")
        exit(1)
    
    arquivo_entrada = max(arquivos, key=os.path.getmtime)
    
    # Processar enriquecimento
    df_enriquecido, caminho_saida = processar_enriquecimento_nfe(arquivo_entrada)
    
    # Exibir amostra
    print("\n" + "="*60)
    print("Amostra de Dados Enriquecidos (primeiras 10)")
    print("="*60)
    if 'municipio' in df_enriquecido.columns:
        cols_amostra = ['codigo_municipio_destinatario', 'municipio', 'descricao_produto']
        print(df_enriquecido[cols_amostra].head(10).to_string(index=False))
    else:
        print(df_enriquecido.head(10).to_string(index=False))
