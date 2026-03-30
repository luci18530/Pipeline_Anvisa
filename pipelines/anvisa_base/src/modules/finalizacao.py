# -*- coding: utf-8 -*-
"""
Modulo para padronizacao final de colunas e exportacao de dados.
Gera arquivos para uso em outros pipelines e analise manual.
"""
import pandas as pd
import json
import os
from datetime import datetime
import re


# ==============================================================================
#      DEFINIÇÃO DAS COLUNAS DE EXPORTAÇÃO
# ==============================================================================

# Colunas para renomear mantendo historico (originais)
RENOMEAR_PARA_ORIGINAL = {
    'DESCRICAO': 'DESCRICAO_ORIGINAL',
    'LABORATORIO': 'LABORATORIO_ORIGINAL',
    'APRESENTACAO': 'APRESENTACAO_ORIGINAL',
    'CLASSE_TERAPEUTICA': 'CLASSE_TERAPEUTICA_ORIGINAL',
    'PRINCIPIO_ATIVO': 'PRINCIPIO_ATIVO_ORIGINAL'
}

# Colunas intermediarias para remover
COLUNAS_REMOVER = [
    'DESCRICAO_CORRIGIDA',
    'APRESENTACAO_NORMALIZADA',
    'UNIDADES_RULE'
]

# Colunas consolidadas para renomear (versoes finais)
RENOMEAR_PARA_FINAIS = {
    'PRINCIPIO_ATIVO_CONSOLIDADO': 'PRINCIPIO ATIVO',
    'DESCRICAO_CORRIGIDA_CONSOLIDADA': 'PRODUTO',
    'APRESENTACAO_NORMALIZADA_CONSOLIDADA': 'APRESENTACAO',
    'LABORATORIO_CONSOLIDADO': 'LABORATORIO',
    'CLASSE_TERAPEUTICA_AJUSTADA': 'CLASSE TERAPEUTICA'
}

# Colunas para exportacao completa (ordem correta)
COLUNAS_EXPORTAR_COMPLETA = [
    'ID_CMED_PRODUTO',
    'GRUPO ANATOMICO',
    'PRINCIPIO ATIVO',
    'PRODUTO',
    'STATUS',
    'APRESENTACAO',
    'TIPO DE PRODUTO',
    'QUANTIDADE UNIDADES',
    'QUANTIDADE MG',
    'QUANTIDADE ML',
    'QUANTIDADE UI',
    'LABORATORIO',
    'CLASSE TERAPEUTICA',
    'GRUPO TERAPEUTICO',
    'GGREM',
    'EAN_1',
    'EAN_2',
    'EAN_3',
    'REGISTRO'
]

# Colunas para exportacao de analise manual (ordem para revisao)
COLUNAS_EXPORTAR_ANALISE = [
    'PRODUTO',
    'PRINCIPIO ATIVO',
    'CLASSE TERAPEUTICA',
    'GRUPO TERAPEUTICO',
    'GRUPO ANATOMICO',
    'APRESENTACAO',
    'STATUS',
    'QUANTIDADE UNIDADES',
    'QUANTIDADE MG',
    'QUANTIDADE ML',
    'QUANTIDADE UI',
    'EAN_1',
    'EAN_2',
    'EAN_3',
    'TIPO DE PRODUTO',
    'REGISTRO',
    'GGREM',
    'LABORATORIO'
]


# ==============================================================================
#      FUNÇÕES DE PADRONIZAÇÃO
# ==============================================================================

def padronizar_nomes_colunas(df):
    """
    Garante consistencia dos nomes de colunas (uppercase e sem espacos extras).
    
    Args:
        df (pd.DataFrame): DataFrame a padronizar
        
    Returns:
        pd.DataFrame: DataFrame com colunas padronizadas
    """
    print("Padronizando nomes de colunas...")
    df.columns = df.columns.str.strip().str.upper()
    print(f"[OK] {len(df.columns)} colunas padronizadas.")
    return df


def renomear_colunas_originais(df):
    """
    Renomeia colunas originais para manter historico.
    
    Args:
        df (pd.DataFrame): DataFrame a processar
        
    Returns:
        pd.DataFrame: DataFrame com colunas originais renomeadas
    """
    print("\nRenomeando colunas originais para historico...")
    colunas_renomeadas = []
    
    for col_antiga, col_nova in RENOMEAR_PARA_ORIGINAL.items():
        if col_antiga not in df.columns:
            continue

        if col_nova in df.columns:
            print(f"  [INFO] Backup '{col_nova}' ja existe. Mantendo coluna '{col_antiga}' atual.")
            continue

        df = df.rename(columns={col_antiga: col_nova})
        colunas_renomeadas.append(f"{col_antiga} -> {col_nova}")
    
    if colunas_renomeadas:
        print(f"[OK] {len(colunas_renomeadas)} colunas renomeadas:")
        for renomeacao in colunas_renomeadas:
            print(f"  - {renomeacao}")
    else:
        print("[INFO] Nenhuma coluna original encontrada para renomear.")
    
    return df


def remover_colunas_intermediarias(df):
    """
    Remove colunas intermediarias que nao serao mais usadas.
    
    Args:
        df (pd.DataFrame): DataFrame a processar
        
    Returns:
        pd.DataFrame: DataFrame com colunas intermediarias removidas
    """
    print("\nRemovendo colunas intermediarias...")
    colunas_removidas = []
    
    for col in COLUNAS_REMOVER:
        if col in df.columns:
            colunas_removidas.append(col)
    
    df = df.drop(columns=COLUNAS_REMOVER, errors='ignore')
    
    if colunas_removidas:
        print(f"[OK] {len(colunas_removidas)} colunas removidas:")
        for col in colunas_removidas:
            print(f"  - {col}")
    else:
        print("[INFO] Nenhuma coluna intermediaria encontrada para remover.")
    
    return df


def renomear_colunas_finais(df):
    """
    Renomeia colunas consolidadas para versoes finais padronizadas.
    
    Args:
        df (pd.DataFrame): DataFrame a processar
        
    Returns:
        pd.DataFrame: DataFrame com colunas finais renomeadas
    """
    print("\nRenomeando colunas consolidadas para versoes finais...")
    colunas_renomeadas = []
    
    for col_antiga, col_nova in RENOMEAR_PARA_FINAIS.items():
        if col_antiga in df.columns:
            df = df.rename(columns={col_antiga: col_nova})
            colunas_renomeadas.append(f"{col_antiga} -> {col_nova}")
    
    if colunas_renomeadas:
        print(f"[OK] {len(colunas_renomeadas)} colunas renomeadas:")
        for renomeacao in colunas_renomeadas:
            print(f"  - {renomeacao}")
    else:
        print("[INFO] Nenhuma coluna consolidada encontrada para renomear.")
    
    return df


def aplicar_padronizacao_final(df):
    """
    Aplica todas as padronizacoes de colunas.
    
    Args:
        df (pd.DataFrame): DataFrame a padronizar
        
    Returns:
        pd.DataFrame: DataFrame com todas as padronizacoes aplicadas
    """
    print("\n" + "=" * 80)
    print("PADRONIZACAO FINAL DE COLUNAS")
    print("=" * 80)
    
    # Etapa 1: Padronizar nomes de colunas
    df = padronizar_nomes_colunas(df)
    
    # Etapa 2: Renomear colunas originais
    df = renomear_colunas_originais(df)
    
    # Etapa 3: Remover colunas intermediarias
    df = remover_colunas_intermediarias(df)
    
    # Etapa 4: Renomear colunas finais
    df = renomear_colunas_finais(df)
    
    print("\n[OK] Padronizacao de colunas concluida!")
    print(f"Total de colunas no DataFrame: {len(df.columns)}")
    print("\nColunas finais:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    return df


def aplicar_sanitizacao_final_produto(df):
    """
    Sanitizacao defensiva da coluna PRODUTO para remover residuos conhecidos
    que podem reaparecer em etapas posteriores.
    """
    if 'PRODUTO' not in df.columns:
        return df

    regras_criticas = [
        (r'\(\)\s*\+\s*BETAMETASONA\s*\+\s*MALEATO DE DEXCLORFENIRAMINA', 'BETAMETASONA + MALEATO DE DEXCLORFENIRAMINA'),
        (r'0,?25\s*\+\s*BROMETRO\s*\+\s*IPRATROPIO\s*\+\s*MG', 'BROMETO DE IPRATROPIO'),
        (r'25000\s*\+\s*NISTATINA\s*\+\s*UI/G', 'NISTATINA'),
        (r'40\s*\+\s*ALBENDAZOL\s*\+\s*MG/ML\s*\+\s*ORAL', 'ALBENDAZOL'),
        (r'ACETOTRIA\s*\+\s*GRAM\s*\+\s*NEO\s*\+\s*NIST\s*\+\s*SULF', 'ACETONIDO DE TRIANCINOLONA + GRAMICIDINA + SULFATO DE NEOMICINA + NISTATINA'),
        (r'ALFAINTERFERONA 2B\s*\(RECOMBINANTE\)', 'ALFAINTERFERONA 2B'),
        (r'AMBROXOL\s*\+\s*CLOR', 'CLORIDRATO DE AMBROXOL'),
        (r'\bCIPROFLOXACINO\s*\+\s*CLOR\b', 'CLORIDRATO DE CIPROFLOXACINO'),
        (r'\bC\s*E\s*DALACIN\s*\+\s*DALACIN\s*\+\s*V\b', 'DALACIN C + DALACIN V'),
        (r'\bDALACIN\s*C\s*E\s*DALACIN\s*V\b', 'DALACIN C + DALACIN V'),
        (r'\bCEFALEXINA\s*\+\s*MONOIDRATADA\b', 'CEFALEXINA'),
        (r'\bCEFTAZIDIMA\s*\+\s*PENTAIDRATADA\b', 'CEFTAZIDIMA'),
        (r'\bCLORIDRATO DE ONDANSETRONA\s*DIHIDRATADO\b', 'CLORIDRATO DE ONDANSETRONA'),
        (r'\s*\+\s*(?:MONO|DI|TRI|TETRA|PENTA|HEMI|HEPTA|SESQUI)?\s*(?:I?HIDRATAD|IDRATAD)[AO]\b', ''),
        (r'^NISTATINA\s*\+\s*UI/G$', 'NISTATINA'),
        (r'^ALBENDAZOL\s*\+\s*MG/ML\s*\+\s*ORAL$', 'ALBENDAZOL'),
    ]

    serie = df['PRODUTO'].astype(str)
    alteracoes = 0
    for padrao, substituicao in regras_criticas:
        mask = serie.str.contains(padrao, regex=True, na=False)
        qtd = int(mask.sum())
        if qtd > 0:
            alteracoes += qtd
            serie = serie.str.replace(padrao, substituicao, regex=True)

    if alteracoes > 0:
        serie = (
            serie
            .str.replace(r'\s*\+\s*', ' + ', regex=True)
            .str.replace(r'\s{2,}', ' ', regex=True)
            .str.strip()
        )
        df['PRODUTO'] = serie
        print(f"[OK] Sanitizacao final de PRODUTO aplicada: {alteracoes:,} ajustes.")
    else:
        print("[INFO] Sanitizacao final de PRODUTO: nenhum ajuste necessario.")

    return df


# ==============================================================================
#      FUNÇÕES DE EXPORTAÇÃO
# ==============================================================================

def exportar_para_pipeline(df, output_path="output/anvisa/baseANVISA.csv", dtype_path="output/anvisa/baseANVISA_dtypes.json"):
    """
    Exporta dados para uso em outro pipeline (CSV ';' + tipos).
    
    Args:
        df (pd.DataFrame): DataFrame a exportar
        output_path (str): Caminho do arquivo CSV de saida
        dtype_path (str): Caminho do arquivo JSON com tipos
        
    Returns:
        tuple: (output_path, dtype_path) - caminhos dos arquivos criados
    """
    print("\n" + "=" * 80)
    print("EXPORTACAO PARA PIPELINE")
    print("=" * 80)
    
    # Criar pasta output se nao existir
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Salva CSV com separador ';' (padrão do projeto)
    print(f"\nSalvando dados em: {output_path}")
    df.to_csv(output_path, index=False, sep=';', encoding='utf-8')
    print(f"[OK] Arquivo CSV salvo: {output_path}")
    print(f"  - Registros: {len(df):,}")
    print(f"  - Colunas: {len(df.columns)}")
    print(f"  - Tamanho: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
    
    # Salva tipos de dados
    print(f"\nSalvando tipos de dados em: {dtype_path}")
    dtypes_dict = {col: str(dtype) for col, dtype in df.dtypes.items()}
    with open(dtype_path, "w", encoding='utf-8') as f:
        json.dump(dtypes_dict, f, indent=2, ensure_ascii=False)
    print(f"[OK] Tipos de dados salvos: {dtype_path}")
    
    return output_path, dtype_path


def exportar_completo(df, output_path="output/anvisa/dfprodutos.csv"):
    """
    Exporta dataset completo sem remover duplicatas.
    
    Args:
        df (pd.DataFrame): DataFrame a exportar
        output_path (str): Caminho do arquivo de saida
        
    Returns:
        str: Caminho do arquivo criado
    """
    print("\n" + "=" * 80)
    print("EXPORTACAO COMPLETA")
    print("=" * 80)
    
    # Criar pasta output se nao existir
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Seleciona colunas existentes
    colunas_existentes = [c for c in COLUNAS_EXPORTAR_COMPLETA if c in df.columns]
    colunas_faltantes = [c for c in COLUNAS_EXPORTAR_COMPLETA if c not in df.columns]
    
    if colunas_faltantes:
        print(f"\n[AVISO] {len(colunas_faltantes)} colunas nao encontradas:")
        for col in colunas_faltantes:
            print(f"  - {col}")
    
    # Cria copia com colunas existentes
    df_exportar = df[colunas_existentes].copy()
    
    # Salva CSV
    print(f"\nSalvando dados completos em: {output_path}")
    df_exportar.to_csv(output_path, index=False, encoding='utf-8')
    print(f"[OK] Arquivo CSV salvo: {output_path}")
    print(f"  - Registros: {len(df_exportar):,}")
    print(f"  - Colunas: {len(df_exportar.columns)}")
    print(f"  - Tamanho: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
    
    return output_path


def exportar_para_analise_manual(df, output_path="output/anvisa/dfpro_correcao_manual.xlsx"):
    """
    Exporta dados para analise manual no Google Sheets/Excel (sem duplicatas).
    
    Args:
        df (pd.DataFrame): DataFrame a exportar
        output_path (str): Caminho do arquivo de saida
        
    Returns:
        str: Caminho do arquivo criado
    """
    print("\n" + "=" * 80)
    print("EXPORTACAO PARA ANALISE MANUAL")
    print("=" * 80)
    
    # Criar pasta output se nao existir
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Seleciona colunas existentes
    colunas_existentes = [c for c in COLUNAS_EXPORTAR_ANALISE if c in df.columns]
    colunas_faltantes = [c for c in COLUNAS_EXPORTAR_ANALISE if c not in df.columns]
    
    if colunas_faltantes:
        print(f"\n[AVISO] {len(colunas_faltantes)} colunas nao encontradas:")
        for col in colunas_faltantes:
            print(f"  - {col}")
    
    print(f"\n[OK] Usando {len(colunas_existentes)} colunas para exportacao.")
    
    # Cria copia com colunas existentes
    df_exportar = df[colunas_existentes].copy()
    
    # Remove duplicatas
    print("\nRemovendo duplicatas...")
    linhas_antes = len(df_exportar)
    df_exportar = df_exportar.drop_duplicates(subset=colunas_existentes)
    linhas_depois = len(df_exportar)
    linhas_removidas = linhas_antes - linhas_depois
    
    print(f"[OK] Duplicatas removidas: {linhas_removidas:,}")
    print(f"Registros unicos: {linhas_depois:,}")
    
    # Salva Excel
    print(f"\nSalvando dados para analise em: {output_path}")
    saved_path = output_path
    try:
        df_exportar.to_excel(output_path, index=False, engine='openpyxl')
    except PermissionError:
        base, ext = os.path.splitext(output_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_path = f"{base}_{timestamp}{ext}"
        print(
            f"[AVISO] Arquivo bloqueado/sem permissao em '{output_path}'. "
            f"Salvando em '{saved_path}'."
        )
        df_exportar.to_excel(saved_path, index=False, engine='openpyxl')

    print(f"[OK] Arquivo Excel salvo: {saved_path}")
    print(f"  - Registros: {len(df_exportar):,}")
    print(f"  - Colunas: {len(df_exportar.columns)}")
    print(f"  - Tamanho: {os.path.getsize(saved_path) / 1024 / 1024:.2f} MB")
    
    print(f"\nColunas incluidas na exportacao:")
    for i, col in enumerate(colunas_existentes, 1):
        print(f"  {i}. {col}")
    
    return saved_path


def processar_finalizacao(df):
    """
    Funcao principal para finalizar o pipeline.
    Aplica padronizacao e gera todos os arquivos de exportacao.
    
    Args:
        df (pd.DataFrame): DataFrame processado
        
    Returns:
        pd.DataFrame: DataFrame finalizado
    """
    print("\n" + "=" * 80)
    print("FINALIZACAO DO PIPELINE")
    print("=" * 80)
    
    # Aplicar padronizacao final
    df_finalizado = aplicar_padronizacao_final(df)

    # Sanitizacao defensiva de residuos criticos no nome de produto
    df_finalizado = aplicar_sanitizacao_final_produto(df_finalizado)
    
    # Exportar para pipeline (CSV ';' + tipos)
    exportar_para_pipeline(df_finalizado)
    
    # Exportar completo (CSV)
    exportar_completo(df_finalizado)
    
    # Exportar para analise manual (Excel sem duplicatas)
    exportar_para_analise_manual(df_finalizado)
    
    print("\n" + "=" * 80)
    print("[OK] FINALIZACAO CONCLUIDA COM SUCESSO!")
    print("=" * 80)
    print("\nArquivos gerados:")
    print("  1. baseANVISA.csv - Para uso em pipeline (CSV ';')")
    print("  2. baseANVISA_dtypes.json - Tipos de dados")
    print("  3. dfprodutos.csv - Dataset completo")
    print("  4. dfpro_correcao_manual.xlsx - Para analise manual (sem duplicatas)")
    
    return df_finalizado


if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")
