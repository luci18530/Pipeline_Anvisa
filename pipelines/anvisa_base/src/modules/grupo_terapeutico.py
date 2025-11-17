# -*- coding: utf-8 -*-
"""
Modulo para processamento de GRUPO TERAPEUTICO.
Faz normalizacao de codigos ATC e join com base externa de grupos terapeuticos.
"""
import pandas as pd
import re
import gdown
import os


def normalizar_sigla_atc(sigla: str) -> str:
    """
    Normaliza codigos ATC:
    - Mantem prefixo (ex: C07, J01)
    - Adiciona zero se houver apenas 1 digito final (N05A9 -> N05A09)
    - Remove zero isolado (A03A0 -> A03A)
    - NUNCA deixa zero sozinho (C07A0 -> C07A)
    
    Args:
        sigla (str): Codigo ATC para normalizar
        
    Returns:
        str: Codigo ATC normalizado
    
    Exemplos:
        >>> normalizar_sigla_atc("A02B4")
        'A02B04'
        >>> normalizar_sigla_atc("N05A9")
        'N05A09'
        >>> normalizar_sigla_atc("A03A0")
        'A03A'
    """
    if not isinstance(sigla, str):
        return sigla

    s = sigla.strip().upper()

    # Corrige casos de 1 digito no final (A02B4 -> A02B04, N05A9 -> N05A09)
    s = re.sub(r'([A-Z]\d{2}[A-Z])(\d)\b', r'\g<1>0\2', s)

    # Remove zero isolado ou duplo no fim (A03A0 -> A03A, C07A0 -> C07A)
    s = re.sub(r'0+\b', '', s)

    return s


def baixar_grupos_terapeuticos(file_id: str = "1G0pXhxVCw04f8JXhl1dB22qNPgekDb_aogVgLgMVQz8",
                               output_path: str = "grupos_terapeuticos.xlsx",
                               force_download: bool = False) -> pd.DataFrame:
    """
    Baixa planilha de grupos terapeuticos do Google Sheets.
    
    Args:
        file_id (str): ID do arquivo do Google Sheets
        output_path (str): Caminho para salvar o arquivo baixado
        force_download (bool): Se True, baixa mesmo se o arquivo ja existir
        
    Returns:
        pd.DataFrame: DataFrame com os grupos terapeuticos
    """
    # Verifica se arquivo ja existe e nao e para forcar download
    if os.path.exists(output_path) and not force_download:
        print(f"[OK] Arquivo '{output_path}' ja existe. Usando versao local.")
        return pd.read_excel(output_path)
    
    # Monta a URL de exportacao direta (formato XLSX)
    url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
    
    print(f"Baixando grupos terapeuticos do Google Sheets...")
    print(f"URL: {url}")
    
    # Faz o download
    gdown.download(url, output_path, quiet=False)
    
    # Le o Excel baixado
    df_grupos = pd.read_excel(output_path)
    
    print(f"[OK] Arquivo baixado: {output_path}")
    print(f"Total de registros: {len(df_grupos):,}")
    
    return df_grupos


def criar_debug_grupos_merge(df, df_grupos, output_dir: str = "."):
    """
    Cria arquivo de debug com join inverso (df_grupos -> df).
    Util para verificar quais classes terapeuticas nao tem correspondencia.
    
    Args:
        df (pd.DataFrame): DataFrame principal
        df_grupos (pd.DataFrame): DataFrame de grupos terapeuticos
        output_dir (str): Diretorio para salvar arquivos de debug
        
    Returns:
        tuple: (df_grupos_merge, nao_casaram) - DataFrames de debug
    """
    # Criar pasta output se nao existir
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n[DEBUG] Criando arquivo de join inverso para analise...")
    
    # Faz o merge para trazer PRINCIPIO_ATIVO e DESCRICAO
    df_grupos_merge = pd.merge(
        df_grupos,
        df[['CLASSE TERAPEUTICA', 'PRINCIPIO ATIVO', 'PRODUTO', 'STATUS', 'TIPO DE PRODUTO']],
        left_on='CLASSE_TERAPEUTICA_CONSOLIDADA',
        right_on='CLASSE TERAPEUTICA',
        how='left',
        indicator=True
    )
    
    # Remove duplicatas
    df_grupos_merge = df_grupos_merge.drop_duplicates(
        subset=['CLASSE_TERAPEUTICA_AJUSTADA', 'PRINCIPIO ATIVO', 'PRODUTO', 'STATUS', 'TIPO DE PRODUTO']
    )
    
    # Identifica quais nao deram match
    nao_casaram = df_grupos_merge[df_grupos_merge['_merge'] == 'left_only']
    print(f"{len(nao_casaram)} classes nao encontradas no DataFrame principal.\n")
    
    if len(nao_casaram) > 0:
        print("Classes sem correspondencia:")
        print(nao_casaram[['CLASSE_TERAPEUTICA_CONSOLIDADA', 'CLASSE_TERAPEUTICA_AJUSTADA']].drop_duplicates())
    
    # Exporta resultados
    output_path = os.path.join(output_dir, "df_grupos_com_principio_ativo.xlsx")
    nao_match_path = os.path.join(output_dir, "df_grupos_sem_match.xlsx")
    
    df_grupos_merge.to_excel(output_path, index=False)
    nao_casaram.to_excel(nao_match_path, index=False)
    
    print(f"[OK] Arquivos de debug salvos:")
    print(f"  - {output_path}")
    print(f"  - {nao_match_path}")
    
    return df_grupos_merge, nao_casaram


def mapear_grupos_terapeuticos(df, df_grupos, criar_debug: bool = False):
    """
    Mapeia grupos terapeuticos usando dicionarios (metodo rapido).
    
    Args:
        df (pd.DataFrame): DataFrame principal
        df_grupos (pd.DataFrame): DataFrame de grupos terapeuticos
        criar_debug (bool): Se True, cria arquivos de debug
        
    Returns:
        pd.DataFrame: DataFrame com colunas de grupo terapeutico mapeadas
    """
    print("\n" + "=" * 80)
    print("MAPEAMENTO DE GRUPOS TERAPEUTICOS")
    print("=" * 80)
    
    # Renomear coluna com acento se existir
    if 'CLASSE TERAPÊUTICA' in df.columns:
        df = df.rename(columns={'CLASSE TERAPÊUTICA': 'CLASSE TERAPEUTICA'})
        print("[INFO] Coluna 'CLASSE TERAPÊUTICA' renomeada para 'CLASSE TERAPEUTICA'.")
    
    # Normaliza codigos ATC primeiro
    print("Normalizando codigos ATC...")
    df['CLASSE_TERAPEUTICA_NORMALIZADA'] = df['CLASSE TERAPEUTICA'].map(normalizar_sigla_atc)
    
    # Cria dicionarios de lookup (mapeamentos diretos) - muito mais rapido que merge
    print("Criando dicionarios de mapeamento...")
    dic_ajustada = dict(zip(
        df_grupos['CLASSE_TERAPEUTICA_CONSOLIDADA'], 
        df_grupos['CLASSE_TERAPEUTICA_AJUSTADA']
    ))
    dic_grupo = dict(zip(
        df_grupos['CLASSE_TERAPEUTICA_CONSOLIDADA'], 
        df_grupos['GRUPO TERAPEUTICO']
    ))
    
    # Escolhe automaticamente a coluna certa para mapear
    col_chave = 'CLASSE_TERAPEUTICA_NORMALIZADA' if 'CLASSE_TERAPEUTICA_NORMALIZADA' in df.columns else 'CLASSE TERAPEUTICA'
    
    print(f"Aplicando mapeamento usando coluna: '{col_chave}'...")
    # Aplica mapeamento direto (instantaneo)
    df['CLASSE_TERAPEUTICA_AJUSTADA'] = df[col_chave].map(dic_ajustada)
    df['GRUPO TERAPEUTICO'] = df[col_chave].map(dic_grupo)
    
    # Identifica quem nao teve correspondencia
    mask_nao = df['CLASSE_TERAPEUTICA_AJUSTADA'].isna()
    df_nao_casados = (
        df.loc[mask_nao, [col_chave]]
        .drop_duplicates()
        .sort_values(col_chave)
    )
    
    total_nao = mask_nao.sum()
    percentual_nao = (total_nao / len(df) * 100) if len(df) > 0 else 0
    
    print(f"\n[OK] Mapeamento concluido instantaneamente!")
    print(f"Total de registros: {len(df):,}")
    print(f"Registros SEM correspondencia: {total_nao:,} ({percentual_nao:.1f}%)")
    print(f"Registros COM correspondencia: {len(df) - total_nao:,} ({100 - percentual_nao:.1f}%)")
    
    if len(df_nao_casados) > 0:
        print(f"\nClasses terapeuticas unicas sem correspondencia: {len(df_nao_casados):,}")
        print("\nPrimeiras classes sem match:")
        print(df_nao_casados.head(10))
        
        # Exporta nao casados
        def limpar_texto(col):
            return col.astype(str).str.replace(r'[\x00-\x1F\x7F-\x9F]', '', regex=True)
        
        df_nao_casados_limpo = df_nao_casados.apply(limpar_texto)
        output_no_match = "output/dfpro_sem_match_grupos.xlsx"
        # Criar pasta output se nao existir
        os.makedirs(os.path.dirname(output_no_match), exist_ok=True)
        df_nao_casados_limpo.to_excel(output_no_match, index=False)
        print(f"\n[AVISO] Planilha com nao correspondidos salva em: {output_no_match}")
    
    # Cria arquivo de debug se solicitado
    if criar_debug:
        criar_debug_grupos_merge(df, df_grupos, output_dir="output")
    
    # Limpa colunas temporarias e renomeia
    print("\nFinalizando processamento...")
    if 'CLASSE_TERAPEUTICA_NORMALIZADA' in df.columns:
        df = df.drop(columns=['CLASSE_TERAPEUTICA_NORMALIZADA'])
    
    if 'CLASSE_TERAPEUTICA_AJUSTADA' in df.columns:
        df = df.rename(columns={'CLASSE_TERAPEUTICA_AJUSTADA': 'CLASSE TERAPEUTICA'})
    
    print("[OK] Colunas de classe terapeutica atualizadas.")
    
    return df


def processar_grupo_terapeutico(df, 
                                file_id: str = "1G0pXhxVCw04f8JXhl1dB22qNPgekDb_aogVgLgMVQz8",
                                criar_debug: bool = False,
                                force_download: bool = False):
    """
    Funcao principal para processar grupo terapeutico.
    
    Args:
        df (pd.DataFrame): DataFrame principal
        file_id (str): ID do arquivo do Google Sheets com grupos terapeuticos
        criar_debug (bool): Se True, cria arquivos de debug para analise
        force_download (bool): Se True, baixa arquivo mesmo se ja existir
        
    Returns:
        pd.DataFrame: DataFrame com grupo terapeutico mapeado
    """
    print("\n" + "=" * 80)
    print("PROCESSAMENTO DE GRUPO TERAPEUTICO")
    print("=" * 80)
    
    # Baixar base de grupos terapeuticos
    df_grupos = baixar_grupos_terapeuticos(
        file_id=file_id, 
        force_download=force_download
    )
    
    # Mapear grupos terapeuticos
    df = mapear_grupos_terapeuticos(df, df_grupos, criar_debug=criar_debug)
    
    print("\n[OK] Processamento de GRUPO TERAPEUTICO concluido!")
    
    return df


if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")
