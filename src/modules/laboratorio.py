# -*- coding: utf-8 -*-
"""
Modulo para processamento e normalizacao da coluna 'LABORATORIO'.
Remove siglas empresariais e padroniza nomes de laboratorios.
"""
import pandas as pd


def processar_laboratorio(df):
    """
    Processa e normaliza a coluna LABORATORIO.
    
    Remove siglas empresariais comuns (LTDA, SA, EIRELI, EPP, etc.)
    e padroniza o formato dos nomes de laboratorios.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'LABORATORIO'
        
    Returns:
        pandas.DataFrame: DataFrame com coluna LABORATORIO normalizada
            e LABORATORIO_ORIGINAL criada como backup
    """
    print("\n" + "=" * 80)
    print("PROCESSAMENTO DE LABORATORIO")
    print("=" * 80)
    
    if 'LABORATORIO' not in df.columns:
        print("[AVISO] Coluna 'LABORATORIO' nao encontrada. Pulando processamento.")
        return df
    
    # Criar backup antes da normalizacao
    if 'LABORATORIO_ORIGINAL' not in df.columns:
        print("Criando backup 'LABORATORIO_ORIGINAL'...")
        df['LABORATORIO_ORIGINAL'] = df['LABORATORIO'].str.upper()
    
    # Contar valores unicos antes
    unicos_antes = df['LABORATORIO'].nunique()
    print(f"Laboratorios unicos antes da normalizacao: {unicos_antes:,}")
    
    # Aplicar limpeza de siglas empresariais
    print("Removendo siglas empresariais e padronizando...")
    df['LABORATORIO'] = (
        df['LABORATORIO']
        .astype(str)
        .str.upper()
        .str.replace(r'\bLTDA\b', '', regex=True)
        .str.replace(r'\bLT\b', '', regex=True)
        .str.replace(r'\.', '', regex=True)
        .str.replace(r'\bEIRELI\b', '', regex=True)
        .str.replace(r'\bEPP\b', '', regex=True)
        .str.replace(r'\bS\.?A\.?\b', '', regex=True)     # cobre "SA", "S.A.", "S. A."
        .str.replace(r'\bS\s*A\b', '', regex=True)         # cobre " S A" com espacos
        .str.replace(r'\s+', ' ', regex=True)              # normaliza espacos
        .str.strip()
    )
    
    # Contar valores unicos depois
    unicos_depois = df['LABORATORIO'].nunique()
    reducao = unicos_antes - unicos_depois
    percentual = (reducao / unicos_antes * 100) if unicos_antes > 0 else 0
    
    print(f"\n[OK] Processamento de LABORATORIO concluido!")
    print(f"Laboratorios unicos apos normalizacao: {unicos_depois:,}")
    print(f"Reducao: {reducao:,} laboratorios ({percentual:.1f}%)")
    
    return df


if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")
