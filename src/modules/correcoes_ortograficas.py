# -*- coding: utf-8 -*-
"""
Modulo para correcoes ortograficas e quimicas em nomes de substancias.
Inclui normalizacao de combinacoes e correcoes comuns.
"""
import pandas as pd
import re


# ==============================================================================
#      DICIONÁRIO DE CORREÇÕES ORTOGRÁFICAS E QUÍMICAS
# ==============================================================================

CORRECOES_COMUNS = {
    r'\bGETAMICINA\b': 'GENTAMICINA',
    r'\bAZITRIMICINA\b': 'AZITROMICINA',
    r'\s*\+\s*TRI\s*HIDRATADA\b': '',
    r'\s*\+\s*DI\s*HIDRATADA\b': '',
    r'\bSIDENAFILA\b': 'SILDENAFILA',
    r'\bPROPANOLOL\b': 'PROPRANOLOL',
    r'\bDIPROPRIONATO\b': 'DIPROPIONATO',
    r'^SOLUCAO(?:\sFISIOLOGICA\sDE)?\sRINGER\sCOM\sLACTATO(?:\sDE\sSODIO)?$': 'SOLUCAO RINGER COM LACTATO',
    r'\s\+\s\+\s': ' ',  # Remove ' + + '
    r'\s\+\s?G$': '',  # Remove ' + G' ou '+G' no final da string
    r'^AC ACETILSALIC$': 'ACIDO ACETILSALICILICO',
    r'^VALERATO \+ BETAMETASONA$': 'VALERATO DE BETAMETASONA',
    
    # Falta de '+' (Regex para inserir o sinal)
    r'\b(CALCIO)\s(COLECALCIFEROL)\b': r'\1 + \2',
    r'\b(BETAMETASONA)\s(SULFATO)\b': r'\1 + \2',
    r'\b(PIRIDOXINA \+ CIANOCOBALAMINA)\s(FOSFATO)\b': r'\1 + \2',
    r'\b(ADIFENINA)\s(CLORIDRATO DE PROMETAZINA)\b': r'\1 + \2',
    r'\b(NEOMICINA)\s(CLORIDRATO DE LIDOCAINA)\b': r'\1 + \2',
    r'\b(ISONIAZIDA)\s(RIFAMPICINA)\b': r'\1 + \2',
    r'\b(RIFAMPICINA)\s(ISONIAZIDA)\s(PIRAZINAMIDA)\s(ETAMBUTOL)\b': r'\1 + \2 + \3 + \4',
    r'\b(TENOFOVIR DESOPROXILA)\s(LAMIVUDINA)\b': r'\1 + \2',
    'PARACETAMOL + CARISOPRODOL + DICLOFENACO SODICO + CAFEINA': 'PARACETAMOL + CAFEINA + CARISOPRODOL + DICLOFENACO SODICO',
    
    # Adicionar o sinal de "+" faltante em uma associacao
    'SULFATO DE GENTAMICINA FOSFATO DISSODICO DE BETAMETASONA': 'SULFATO DE GENTAMICINA + FOSFATO DISSODICO DE BETAMETASONA',
    
    # Ajustar a nomenclatura de "SODICO" para "DE SODIO"
    'MONTELUCASTE SODICO': 'MONTELUCASTE DE SODIO',
    'CANDESARTANA + CILEXETILA': 'CANDESARTANA CILEXETILA',
    r'\bTAZOBACTAM\sSODICO\b': 'TAZOBACTAM',
    r'\bRABEPRAZOL\sSODICO\b': 'RABEPRAZOL',
    r'\bACICLOVIR\sSODICO\b': 'ACICLOVIR',
    r'\bAVIBACTAM\sSODICO\b': 'AVIBACTAM',
    r'\bCROMOGLICATO\sDISSODICO\b': 'CROMOGLICATO',
    r'\bNAPROXENO\sSODICO\b': 'NAPROXENO',
    r'\bPANTOPRAZOL\sSODICO\b': 'PANTOPRAZOL',
}

# Casos que DEVEM ter '+'
PADROES_PLUS_ADICIONAR = [
    (r'AMOXICILINA\s+CLAVULANATO', 'AMOXICILINA + CLAVULANATO'),
    (r'BENZILPENICILINA\s+PROCAINA\s+BENZILPENICILINA\s+POTASSICA',
     'BENZILPENICILINA PROCAINA + BENZILPENICILINA POTASSICA'),
    (r'FOSFATO\s+DISSODICO\s+DE\s+BETAMETASONA\s+DIPROPIONATO\s+DE\s+BETAMETASONA',
     'FOSFATO DISSODICO DE BETAMETASONA + DIPROPIONATO DE BETAMETASONA'),
]

# Casos que NAO devem ter '+'
PADROES_PLUS_REMOVER = [
    (r'ALGESTONA\s*\+\s*ACETOFENIDA', 'ALGESTONA ACETOFENIDA'),
]

# Palavras que bloqueiam a padronizacao alfabetica
BLOQUEIOS_ORDENACAO = ['FURP', 'LQFEX', 'ISOFARMA', 'FRACAO']


# ==============================================================================
#      FUNÇÕES DE CORREÇÃO
# ==============================================================================

def corrigir_descricoes(df, col='PRODUTO'):
    """
    Corrige erros ortograficos e de formatacao em nomes de substancias:
    - Insere '+' entre substancias compostas quando faltando.
    - Remove '+' desnecessario em combinacoes incorretas.
    - Corrige erros ortograficos e nomes comuns.
    
    Args:
        df (pandas.DataFrame): DataFrame a processar
        col (str): Nome da coluna a corrigir ('PRODUTO' ou 'PRINCÍPIO ATIVO')
        
    Returns:
        pandas.DataFrame: DataFrame com correcoes aplicadas
    """
    print(f"\nAplicando correcoes ortograficas e quimicas em '{col}'...")
    
    # Aplicar dicionario de correcoes comuns
    for erro, certo in CORRECOES_COMUNS.items():
        df[col] = df[col].str.replace(erro, certo, flags=re.IGNORECASE, regex=True)
    
    # Aplicar adicoes de '+'
    for padrao, subst in PADROES_PLUS_ADICIONAR:
        df[col] = df[col].str.replace(padrao, subst, flags=re.IGNORECASE, regex=True)
    
    # Aplicar remocoes de '+'
    for padrao, subst in PADROES_PLUS_REMOVER:
        df[col] = df[col].str.replace(padrao, subst, flags=re.IGNORECASE, regex=True)
    
    # Normalizacao final de espacos e sinais de '+'
    df[col] = (
        df[col]
        .str.replace(r'\s*\+\s*', ' + ', regex=True)   # normaliza espacos
        .str.replace(r'\+{2,}', '+', regex=True)        # remove '++'
        .str.replace(r'\s{2,}', ' ', regex=True)        # remove espacos duplos
        .str.strip()
    )
    
    print(f"[OK] Correcoes ortograficas e quimicas aplicadas em '{col}'.")
    return df


def padronizar_combinacoes(texto):
    """
    Padroniza combinacoes quimicas em ordem alfabetica.
    Remove duplicatas e ordena componentes separados por '+'.
    
    TRAVAS: Nao altera se contiver: FURP, LQFEX, ISOFARMA, FRACAO
    
    Args:
        texto (str): Texto a padronizar
        
    Returns:
        str: Texto padronizado ou original se bloqueado
    """
    if pd.isna(texto):
        return texto
    
    texto_upper = str(texto).upper()
    
    # TRAVAS: se contiver termos proibidos, nao altera
    if any(b in texto_upper for b in BLOQUEIOS_ORDENACAO):
        return texto  # ignora linhas com palavras bloqueadas
    
    # Divide pelos '+' e limpa espacos extras
    partes = [p.strip() for p in texto.split('+') if p.strip()]
    
    # So aplica se houver mais de um componente
    if len(partes) > 1:
        # Remove duplicatas e ordena alfabeticamente (case-insensitive)
        partes_ordenadas = sorted(set(partes), key=lambda x: x.lower())
        return ' + '.join(partes_ordenadas)
    
    return texto


def aplicar_padronizacao_combinacoes(df, coluna):
    """
    Aplica padronizacao alfabetica de combinacoes em uma coluna.
    
    Args:
        df (pandas.DataFrame): DataFrame a processar
        coluna (str): Nome da coluna a padronizar
        
    Returns:
        pandas.DataFrame: DataFrame com padronizacao aplicada
    """
    if coluna not in df.columns:
        print(f"[AVISO] Coluna '{coluna}' nao encontrada. Pulando padronizacao.")
        return df
    
    print(f"\nPadronizando combinacoes em ordem alfabetica em '{coluna}'...")
    
    antes = df[coluna].copy()
    df[coluna] = df[coluna].apply(padronizar_combinacoes)
    alteradas = (antes != df[coluna]).sum()
    
    print(f"[OK] {alteradas:,} linhas padronizadas em '{coluna}'.")
    print(f"    (ordem alfabetica ajustada, exceto travadas por {', '.join(BLOQUEIOS_ORDENACAO)})")
    
    return df


def remover_procedimento_medico_tabelado(df, coluna='PRODUTO'):
    """
    Remove registros com 'PROCEDIMENTO MEDICO TABELADO PELO GOVERNO'.
    
    Args:
        df (pandas.DataFrame): DataFrame a processar
        coluna (str): Nome da coluna a verificar
        
    Returns:
        pandas.DataFrame: DataFrame filtrado
    """
    if coluna not in df.columns:
        return df
    
    linhas_antes = len(df)
    df = df[df[coluna].str.upper() != 'PROCEDIMENTO MEDICO TABELADO PELO GOVERNO'].copy()
    linhas_removidas = linhas_antes - len(df)
    
    if linhas_removidas > 0:
        print(f"[OK] Removidas {linhas_removidas:,} linhas com 'PROCEDIMENTO MEDICO TABELADO' em '{coluna}'.")
    
    return df


def processar_correcoes_ortograficas(df, colunas=['PRODUTO', 'PRINCÍPIO ATIVO']):
    """
    Funcao principal para processar correcoes ortograficas e padronizacao.
    
    Args:
        df (pandas.DataFrame): DataFrame a processar
        colunas (list): Lista de colunas a processar
        
    Returns:
        pandas.DataFrame: DataFrame com todas as correcoes aplicadas
    """
    print("\n" + "=" * 80)
    print("CORRECOES ORTOGRAFICAS E PADRONIZACAO DE COMBINACOES")
    print("=" * 80)
    
    for coluna in colunas:
        if coluna not in df.columns:
            print(f"\n[AVISO] Coluna '{coluna}' nao encontrada. Pulando.")
            continue
        
        # Aplicar correcoes ortograficas
        df = corrigir_descricoes(df, col=coluna)
        
        # Aplicar padronizacao de combinacoes
        df = aplicar_padronizacao_combinacoes(df, coluna=coluna)
    
    # Remover procedimento medico tabelado (apenas de PRODUTO)
    if 'PRODUTO' in colunas:
        df = remover_procedimento_medico_tabelado(df, coluna='PRODUTO')
    
    print("\n[OK] Correcoes ortograficas e padronizacao concluidas!")
    
    return df


if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")
