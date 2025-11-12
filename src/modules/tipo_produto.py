# -*- coding: utf-8 -*-
"""
Modulo para categorizacao de tipo de produto farmaceutico.
Identifica a forma farmaceutica com base em palavras-chave na apresentacao.
"""
import pandas as pd


def categorizar_produto(apresentacao: str) -> str:
    """
    Retorna a categoria da forma farmaceutica com base em palavras-chave na apresentacao.
    Mantida a ordem de prioridade das regras originais, mas com estrutura centralizada.
    
    Args:
        apresentacao (str): Texto da apresentacao normalizada
        
    Returns:
        str: Categoria identificada (FRASCO, COMPRIMIDO/CAPSULA, BISNAGA, etc.)
    
    Categorias possiveis:
        - FRASCO
        - AMPOLA/FRASCO-AMPOLA
        - DISPOSITIVOS
        - COMPRIMIDO/CAPSULA
        - BISNAGA
        - BOLSA
        - SACHE/PO
        - OUTROS
    """
    if not isinstance(apresentacao, str):
        return "OUTROS"

    s = apresentacao.upper()

    # Mapeamento: categoria -> lista de palavras-chave (em ordem de prioridade)
    # A ordem das regras importa - primeira correspondencia e retornada
    regras = [
        ("FRASCO", ['GOT', 'XAMP']),
        ("AMPOLA/FRASCO-AMPOLA", ['AMP', 'AMPOLA', 'FA']),
        ("DISPOSITIVOS", ['PREENCHIDA', 'ADS', 'EMPL', 'STT']),
        ("COMPRIMIDO/CAPSULA", ['COMPRIMIDO', 'TABLE', 'CAPSULA', 'CAPSULAS', 'CP', 'MGPAS', 
                               'PASTILHA', 'OVULO', 'DRAGEA', 'COMPR', 'CAPGEL']),
        ("BISNAGA", ['CREME', 'BG', 'POM', 'POMADA', 'GEL', 'BISNAGA', 'UNGUENTO']),
        ("BOLSA", ['BOLSA']),
        ("FRASCO", ['SOLUCAO', 'TUBO', 'FR ', 'FRASCO', 'SUS', 'XPE', 'GOT', 'EMUL', 'LOCAO']),
        ("DISPOSITIVOS", ['SERINGA', 'SER', 'CANETA', 'INALADOR', 'SPRAY',
                         'ADES', 'IMPL', 'APLIC', 'PEN', 'CARP']),
        ("SACHE/PO", ['PO ', 'PO', 'SACHES', 'GRAN']),
        ("FRASCO", ['UNGENTO', 'UNG', 'POTE', 'TALQUEIRA', 'PT']),
        ("FRASCO", ['ML']),
        ("COMPRIMIDO/CAPSULA", ['BL']),
    ]

    # Percorre regras na ordem declarada
    for categoria, termos in regras:
        if any(termo in s for termo in termos):
            return categoria

    # Caso nenhuma regra seja correspondida
    return "OUTROS"


def processar_tipo_produto(df):
    """
    Processa categorizacao de tipo de produto para todo o DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna APRESENTACAO_NORMALIZADA
        
    Returns:
        pandas.DataFrame: DataFrame com nova coluna TIPO DE PRODUTO criada
    """
    print("\n" + "=" * 80)
    print("CATEGORIZACAO DE TIPO DE PRODUTO")
    print("=" * 80)
    
    if 'APRESENTACAO_NORMALIZADA' not in df.columns:
        print("[AVISO] Coluna 'APRESENTACAO_NORMALIZADA' nao encontrada. Pulando processamento.")
        return df
    
    # Categorizar tipo de produto
    print("Categorizando tipo de produto baseado na apresentacao...")
    df['TIPO DE PRODUTO'] = df['APRESENTACAO_NORMALIZADA'].apply(categorizar_produto)
    
    # Estatisticas
    print(f"\n[OK] Categorizacao concluida!")
    print(f"\nDistribuicao de categorias:")
    print(df['TIPO DE PRODUTO'].value_counts())
    print(f"\nTotal de categorias unicas: {df['TIPO DE PRODUTO'].nunique()}")
    
    return df


if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")
