"""
Módulo de limpeza e padronização de descrições de produtos
Adaptado do pipeline Colab para ambiente local
"""

import pandas as pd
import numpy as np
import re
import os
from datetime import datetime


# ============================================================
# LISTA DE SUBSTITUIÇÕES
# ============================================================

SUBSTITUICOES = [
    ('"', ''),
    ('30000000000', '30'), ('600000000', '60'),
    ('5000030', '50000 UI 30'), ('100000 / ', '100000 UI /'),
    ('1200000 ', '1200000 UI'), ('1200000 UI', '[ 1200000 UI ]'),
    ('CARBOLITIUM CR', 'CARBOLITIUM'), ('CARBONATO DE LITIO CR', 'CARBONATO DE LITIO'),
    (' P / ', ' PARA '), (' C / ', ' COM '), (' D AGUA ', " D'AGUA "),
    ('CLOR BUPROPIONA', 'CLORIDRATO DE BUPROPIONA'), ('CLOR TERBINAFINA', 'CLORIDRATO DE TERBINAFINA'),
    ('CLOR PROPAFENONA', 'CLORIDRATO DE PROPAFENONA'), ('CLOR DE SODIO', 'CLORETO DE SODIO'),
    ('CLOR SODIO', 'CLORETO DE SODIO'), ('CLOR DE POTASSIO', 'CLORETO DE POTASSIO'),
    ('CLOR POTASSIO', 'CLORETO DE POTASSIO'), ('DICLOFENACO SODIO', 'DICLOFENACO SODICO'),
    ('PREDNISOLONA FOSFATO SODIO', 'FOSFATO SODICO DE PREDNISOLONA'), ('CLORIDRATO FEXOFENADINA', 'CLORIDRATO DE FEXOFENADINA'),
    (' C 1 ', ''), ('_', ''),
    ('A A S', 'AAS'), ('RINGER C ', 'RINGER COM '),
    (' COMP ', ' COMPRIMIDOS '), (' CPRG ', ' COMPRIMIDOS '), (' COMPR ', ' COMPRIMIDOS '), (' CPR ', ' COMPRIMIDOS '),
    (' BL AL AL X 30 C 1', ' BL AL AL X 30 COMPRIMIDOS'), (' BL AL AL X 30', ' BL AL AL X 30 COMPRIMIDOS'), (' BL AL AL X 42', ' BL AL AL X 42 COMPRIMIDOS'),
    (' BL AL AL X 28', ' BL AL AL X 28 COMPRIMIDOS'), (' BL AL PP X 60', ' BL AL PP X 60 COMPRIMIDOS'), (' BL AL PP X 30', ' BL AL PP X 30 COMPRIMIDOS'),
    (' FRS / AMP ', ' FRASCO / AMPOLA '), (' AMP ', ' AMPOLAS '), (' AMPOLAS AMP ', ' AMPOLAS '), (' FRAS ', ' FRASCOS '), (' FRX ', ' FRASCOS '),
    (' BL 6 X 10 EA ', ' 60 COMPRIMIDOS '), (' BL 10 X 6 EA ', ' 60 COMPRIMIDOS '),
    (' TABL BLI X 42', ' 42 COMPRIMIDOS'), (' BL 7 X 8 ', ' 56 COMPRIMIDOS '),
    (' SOL OFT ', ' SOLUCAO OFTALMICA '), (' SOL OFTA ', ' SOLUCAO OFTALMICA '),
    (' CR DERM ', ' CREME DERMATOLOGICO '), ('LOSARTANA PO ', 'LOSARTANA POTASSICA '),
    ('KOLLAGENASE C ', 'KOLLAGENASE COM '),
    (' PMD ', ' POMADA '), ('44 E ', '44E'), (' CAPS ', ' CAPSULAS '),
    (' 30 TABL ', ' 30 COMPRIMIDOS '),
    (r'\b(\d+)\s+\1\b', r'\1'),  # remove numeros duplicados
    (r'\[', ''), (r']', ''),
    (r' 2 BL X 15 COM$', ' 30 COMPRIMIDOS'),
    (r'^,+', ''), (r'^:+', ''),
    (r'\bCAP GEL\b', ' CAPSULAS GELATINOSAS '), (r'\bCAPS GEL\b', ' CAPSULAS GELATINOSAS '), (r'\bCPS\s*GEL\s*$', 'CAPSULAS GELATINOSAS'), (r'\bCA\s*GEL\s*$', 'CAPSULAS GELATINOSAS'),
    (r'\?', ''),
    (r'%,', ''), (r';;', ''),
    (r';', ''), (r'%;', ''),
    (r'\bCOMPRIMIDOS\s+REV\s+4\s*BLX\s*15\b', '60 COMPRIMIDOS REVESTIDOS'),
    (r'\b4\s*BL\s*X\s*15\s*COMPRIMIDOS\s*REV\b', '60 COMPRIMIDOS REVESTIDOS'),
    (r'\bCOMPRIMIDOS\s+REV\s+4\s*BL\s*X\s*15\b', '60 COMPRIMIDOS REVESTIDOS'),
    (r'\bCOMPRIMIDOS\s+REV\s+2\s*BL\s*X\s*15\b', '30 COMPRIMIDOS REVESTIDOS'),
    (r'\bCOMPRIMIDOS\s+REV\s+2\s*BL\s*X\s*4\b', '8 COMPRIMIDOS REVESTIDOS'),
    (r'\bCOMPRIMIDOS\s+REV\s+CT\s*BL\s*3\s*X\s*10\b', '30 COMPRIMIDOS REVESTIDOS'),
    (r'\bCOMPRIMIDOS\s+REV\s+3\s*BL\s*X\s*10\b', '30 COMPRIMIDOS REVESTIDOS'),
    (r'\bCOMPRIMIDOS\s+REV\s+2\s*BL\s*X\s*15\b', '30 COMPRIMIDOS REVESTIDOS'),
    (r'\bCOMPRIMIDOS\s+REV\s+1\s*X\s*28\b', '28 COMPRIMIDOS REVESTIDOS'),
    (r'\b4\s*BL\s*X\s*7\s*COMPRIMIDOS\s*REV\b', '28 COMPRIMIDOS REVESTIDOS'),
    (r'\bCOMPRIMIDOS\s+5\s*BL\s*X\s*10\b', '50 COMPRIMIDOS'),
    (r'\bCOMPRIMIDOS\s+BL\s*X\s*60\b', '60 COMPRIMIDOS'),
    (r'\b4\s*BL\s*X\s*7\s*COMPRIMIDOS\b', '28 COMPRIMIDOS'),
    (r'\b8\s*BL\s*X\s*7\s*COMPRIMIDOS\b', '56 COMPRIMIDOS'),
    (r'\b2\s*BL\s*X\s*15\s*COMPRIMIDOS\b', '30 COMPRIMIDOS'),
    (r'\b0\s*AMP\s*X\s*5\s*ML\b', '100 AMPOLAS X 5 ML'),
    (r'\bCOMP$', 'COMPRIMIDOS'), (r'\bCPR$', 'COMPRIMIDOS'), (r'\bCPS$', 'COMPRIMIDOS'),
    (r'\b50\s*BL\s*X\s*10\s*COMPRIMIDOS\s*CX\s*COM\s*500\s*COMPRIMIDOS\b', '500 COMPRIMIDOS'), (r'\b2\s*BL\s*6\s*COMPRIMIDOS\b', '12 COMPRIMIDOS'),
    (r'\b1\s*BL\s*X\s*30\s*COMPRIMIDOS\s*REV\b', '30 COMPRIMIDOS REVESTIDOS'), (r'\b3\s*BL\s*X\s*10\s*COMPRIMIDOS\s*REV\b', '30 COMPRIMIDOS REVESTIDOS'),
    (r'\b6\s*BL\s*X\s*10\s*COMPRIMIDOS\s*REV\b', '60 COMPRIMIDOS REVESTIDOS'), (r'\b1\s*BL\s*X\s*12\s*COMPRIMIDOS\b', '12 COMPRIMIDOS'),
    (r'\b2\s*BL\s*X\s*15\s*COMPRIMIDOS\b', '30 COMPRIMIDOS'), (r'\b2\s*BL\s*X\s*10\s*COMPRIMIDOS\b', '20 COMPRIMIDOS'),
    (r'\b3\s*BL\s*X\s*10\s*COMPRIMIDOS\b', '30 COMPRIMIDOS'), (r'\b6\s*BL\s*X\s*7\s*COMPRIMIDOS\b', '42 COMPRIMIDOS'),
    (r'\bCOMPRIMIDOS\s+REV\s+1\s*BL\s*X\s*28\b', '28 COMPRIMIDOS REVESTIDOS'), (r'\bCOMPRIMIDOS\s+REV\s+2\s*BL\s*X\s*14\b', '28 COMPRIMIDOS REVESTIDOS'),
    (r'\b50\s*BL\s*X\s*10\s*COMPRIMIDOS\b', '500 COMPRIMIDOS'), (r'\b2\s*BL\s*X\s*15\s*CAP\b', '30 CAPSULAS'),
    (r'\b2\s*BL\s*X\s*15\s*CAPSULAS\b', '30 CAPSULAS'), (r'\b1\s*BL\s*X\s*12\b', '12 COMPRIMIDOS'),
    (r'\b7\s*BL\s*8\s*CAP\b', '56 CAPSULAS'),
    (r'\b7\s*BL\s*8\s*CAPSULAS\b', '56 CAPSULAS'), (r'\bF\s*AMP\b', 'FRASCO AMPOLA'),
    (r'\bCOM\s+REV\b', 'COMPRIMIDOS REVESTIDOS'), (r'\b2\s*BLST\s*X\s*7\s*CPD\b', '14 COMPRIMIDOS'),
    (r'\b1\s*BLST\s*X\s*7\s*CPD\b', '7 COMPRIMIDOS'), (r'\bDG\s*6\s*X\s*10\s*BLST\b', '60 COMPRIMIDOS'),
    (r'\bDG\s*3\s*X\s*10\s*BLST\b', '30 COMPRIMIDOS'), (r'\bCX\s*2\s*BLST\s*X\s*15\b', '30 COMPRIMIDOS'),
    (r'\bFCT\s*6\s*X\s*10\s*BLST\b', '60 COMPRIMIDOS'), (r'\b14\s*BLST\s*8\s*CAPS\b', '112 CAPSULAS'),
    (r'\bC\s*OMP\b', 'COMPRIMIDOS'), (r'\bMG\s*B\s*1\s*COM\b', 'MG COM'),
    (r'\b30\s*GERMED\b', '30 COMPRIMIDOS'), (r'\b25\s*AP\b', '25 AMPOLAS'),
    (r'\b0\s*CP\b', '30 COMPRIMIDOS'), (r'\b0\s*COMPRIMIDOS\b', '30 COMPRIMIDOS'), (r'\b3\s*0\s*CP\b', '30 COMPRIMIDOS'),
    (r'\bCPHD\s+ACIDO\s+F\s*3\s*K\s*2\s*0\s*CA\s*3\s*,\s*5\s*CX\s*C\s*4\b', 'CPHD ACIDO CAIXA COM 4 UNIDADES'),
    (r'\bCOM\s*10\s*0\s*AMP\b', 'COM 100 AMPOLAS'), (r'\bCOM\s*960\s*0\s*COMPRIMIDOS\b', 'COM 960 COMPRIMIDOS'),
    (r'\bCOM\s*100\s*0\s*COMPRIMIDOS\b', 'COM 100 COMPRIMIDOS'), (r'\b30\s*0\s*COMPRIMIDOS\b', '30 COMPRIMIDOS'),
    (r'\bVITAMINA D\s*50\s*000\s*COMPRIMIDOS\b', 'VITAMINA D 50000 UI COM 4 COMPRIMIDOS'),
    (r'\bVITAMINA D\s*10\s*000\s*COMPRIMIDOS\b', 'VITAMINA D 10000 UI COM 4 COMPRIMIDOS'),
    (r'\bVITAMINA D\s*5\s*000\s*COMPRIMIDOS\b', 'VITAMINA D 5000 UI COM 4 COMPRIMIDOS'),
    (r'\bSER\s*COM\b', 'SERINGA CONTENDO'), (r'\b2\s*0\s*CP\b', '20 COMPRIMIDOS'), (r'\b2\s*0\s*COMPRIMIDOS\b', '20 COMPRIMIDOS'),
    (r'\bLOT\s*MZF\s*0\s*X\s*97\s*VENC\s*01\s*/\s*05\s*/\s*24\s*P\s*M\s*C\s*885\s*92\b', ''),
    (r'\b3 0 COMPRIMIDOS\b', '30 COMPRIMIDOS'),
    (r'\b0\s+COMPRIMIDOS\b', '30 COMPRIMIDOS'), (r'\b0\s+EUROFARMA\b', '60 COMPRIMIDOS'),
    (r'\b12\s*0\s*AMP\b', '120 AMPOLAS'),
    (r'\b3\s*000\s*CAPS\b', '30 CAPSULAS'), (r'\bCOM\s*C\s*0\s*CPR\b', 'COM 28 COMPRIMIDOS'),
    (r'\bMESACOL\s*800\s*MG\s*COM\s*0\s*COMP\b', 'MESACOL 800 MG COM 30 COMPRIMIDOS'),
    (r'\bLONGACTIL\s+SOL\s+INJ\s+5\s+MG\s*/\s*ML\s+0\s+AMP\s+CRIST\s+C\s*1\b', 'LONGACTIL SOLUCAO INJETAVEL 5 MG / ML COM 10 AMPOLAS'),
    (r'\b1200\s*000\s*AMPOLA\b', '1200000 UI AMPOLA'), (r'\b5\s*0\s*ENV\b', '50 ENVELOPES'),
    (r'\bESPASMO\s*DIMETILIV\s*20\s*ML\s*EMS\s*LOT\s*0\s*X\s*0219\s*VENC\s*01\s*/\s*02\s*/\s*21\s*P\s*M\s*C\s*20\s*99\b', 'ESPASMO DIMETILIV 20 ML EMS'),
    (r'\b2\s*,\s*0\s*MGCOMPRIMIDOS\b', '2 , 0 MG COMPRIMIDOS'), (r'\b50\s*0\s*COMPRIMIDOS\b', '500 COMPRIMIDOS'),
    (r'\bCREON\s*300\s*MG\s*25\s*000\s*FR\s*30\s*CAP\b', 'CREON 300 MG 25000 UI 30 CAPSULAS'),
    (r'\bCPHD\s*ACIDO\s*F\s*3\s*K\s*2\s*0\s*CA\s*3\s*5\s*FR\s*5\s*L\s*CX\s*C\s*4\s*FARMA\b', 'CPHD ACIDO CAIXA COM 4 UNIDADES'),
    (r'\bCPHD\s*ACIDO\s*F\s*3\s*K\s*2\s*0\s*CA\s*3\s*,\s*5\s*CX\s*C\s*4\b', 'CPHD ACIDO CAIXA COM 4 UNIDADES'),
    (r'\bCPHD\s*ACIDO\s*F\s*2\s*K\s*2\s*0\s*CA\s*3\s*0\s*FR\s*5\s*L\s*CX\s*C\s*4\b', 'CPHD ACIDO CAIXA COM 4 UNIDADES'),
    (r'\b10\s*0\s*AMP\b', '100 AMPOLAS'), (r'\b25\s*MG\s*1\s*ML\s*INJ\s*0\s*AMP\b', '25 MG POR 1 ML INJETAVEL 100 AMPOLAS'),
    (r'\b1\s*00\s*AMP\b', '100 AMPOLAS'), (r'\bEPINEFRINA\s*COM\s*00\s*AMP\b', 'EPINEFRINA COM 100 AMPOLAS'),
    (r'\bALTA\s*D\s*7\s*000\s*CPS\s*COM\s*4\s*(CPS|COMPRIMIDOS)\b', 'ALTA D 7000 UI COM 4 CAPSULAS'),
    (r'\bCOM\s*1\s*0\s*CP\b', 'COM 10 COMPRIMIDOS'), (r'\bCAIXA\s+COM\s+1\s*000\s*COMP\b', 'CAIXA COM 1000 COMPRIMIDOS'),
    (r'\bSELENE\s+ENV\s+COM\s+21\s+DRG\b', 'SELENE 21 COMPRIMIDOS'),
    (r'\bSELENE\s+ENV\s+COM\s+21\s+CP\b', 'SELENE 21 COMPRIMIDOS'),
    (r'\bSELENE\s+ENVELOPE\s+COM\s+21\s+CPRS\b', 'SELENE 21 COMPRIMIDOS'),
    (r'\bSELENE\s+ENVELOPE\s+COM\s+3\s*X\s*21\s*CP\b', 'SELENE 63 COMPRIMIDOS'),
    (r'\bSELENE\s+ENV\s+COM\s+21\s+COMPRIMIDOS\b', 'SELENE 63 COMPRIMIDOS'),
    (r'\bSELENE\s+ENV\s+21\s+COMPRIMIDOS\b', 'SELENE 21 COMPRIMIDOS'),
    (r'\bCOMPRIMIDOS\s+EFERV\s+X\s+16\b', '16 COMPRIMIDOS EFERVESCENTES'),
    (r'\b60\s+CP\b', '60 COMPRIMIDOS'),
    (r'\bCMP\b', 'COMPRIMIDOS'), (r'\bFRS\s+AMPOLAS\b', 'FRASCO / AMPOLA'),
    (r'\b80\s+FR\b', '80 FRASCOS'), (r'\bCX\s+COM\s+750\s+MULTI\b', 'CAIXA COM 750 UNIDADES / MARCA : MULTI'),
    (r'\bCX\s+COM\s+750\s+PRATI\b', 'CAIXA COM 750 UNIDADES / MARCA : PRATI'),
    (r'\bCX\s+COM\s+750\s+GEOLA\b', 'CAIXA COM 750 UNIDADES / MARCA : GEOLA'),
    (r'\bCX\s+COM\b', 'CAIXA COM'),
    (r'\bPMD$', 'POMADA'),
    (r'\bCREM\s+VAG\b', 'CREME VAGINAL'), (r'\bCRE\s+VAG\b', 'CREME VAGINAL'),
    (r'\bCR\s+VAG\b', 'CREME VAGINAL'), (r'\bCREM\s+DERM\b', 'CREME DERMATOLOGICO'),
    (r'\bCREM\b', 'CREME'), (r'\bBISN\b', 'BISNAGA'),
    (r'\b0\s*COMPR\b', '30 COMPRIMIDOS'), (r'\b0\s*COMPRIMIDOS\b', '30 COMPRIMIDOS'),
    (r'\b0\s+CP\b', '30 COMPRIMIDOS'), (r'\b3\s*0\s*COMPR\b', '30 COMPRIMIDOS'),
    (r'\bSER\s*COM\b', 'SERINGA CONTENDO'), (r'\bCOM\s+3\s*0\s+COMPRIMIDOS\b', 'COM 30 COMPRIMIDOS'),
    (r'\b3\s+0\s+COMPRIMIDOS\b', '30 COMPRIMIDOS'), (r'\b(\d{3,})\s+30\s+COMPRIMIDOS\b', r'\1 COMPRIMIDOS'),
    (r'\b(1|2|3|4|5|6|7|8|9|10)\s+COM\s+30\s+COMPRIMIDOS\b', 'COM 30 COMPRIMIDOS'),
    (r'\b3\s*[Xx×]\s*10\s*CPS\b', '30 COMPRIMIDOS'), (r'\b1\s*[Xx×]\s*60\s*BTL\b', '60 COMPRIMIDOS'),
    (r'\b1\s*[Xx×]\s*60\s*EA\b', '60 COMPRIMIDOS'), (r'\bTASIGNA.*?\b112\s*CS\b', '112 CAPSULAS GELATINOSAS'),
    (r'\bTASIGNA\s+200\s+MG\s+COM\s+4\s+CARTUCHOS\s+COM\s+28\s+CPS\s+112\s+CAPSULAS\b', 'TASIGNA 200 MG COM 112 CAPSULAS GELATINOSAS'),
    (r'\bVILAN\s+200\s*/\s*25\s*MCG\b', 'VILAN 200 + 25 MCG'), (r'\bELLIPTA\s+200\s*/\s*25\s*MCG\b', 'ELLIPTA 200 + 25 MCG'),
    (r'\b(\d+)\s*/\s*(\d+)\s*MG\b', r'\1 + \2 MG'), (r'\bDRAG\b', 'COMPRIMIDOS'),
    (r'\bCPS\s+GEL\s+DURA\b', 'CAPSULAS GELATINOSAS DURAS'), (r'\bCPS\s+GEL\s+DUR\b', 'CAPSULAS GELATINOSAS DURAS'),
    (r'\bCAPSULA\s+GEL\s+DUR\b', 'CAPSULAS GELATINOSAS DURAS'), (r'\bCAP\s+DURA\s+BLX\s+60\b', '60 CAPSULAS DURAS'),
    (r'\bCPS\s+1\s*[Xx×]\s*28\b', '28 COMPRIMIDOS'), (r'\bCPS\s+1\s*[Xx×]\s*30\b', '30 COMPRIMIDOS'),
    (r'\bSOL\s+CLORETO\s+DE\s+SODIO\b', 'SOLUCAO DE CLORETO DE SODIO'), (r'\bCOMPRIMIDOS\s+REV\s+1\s*[Xx×]\s*30\s*ETICO\b', '30 COMPRIMIDOS REVESTIDOS - ETICO'),
    (r'\bCOMPRIMIDOS\s+REV\s+1\s*[Xx×]\s*30\s*GMD\b', '30 COMPRIMIDOS REVESTIDOS - GMD'), (r'\bCOMPRIMIDOS\s+REV\s+1\s*[Xx×]\s*30ETICO\b', '30 COMPRIMIDOS REVESTIDOS - ETICO'),
    (r'\bMG\s+COMPRIMIDOS\s+REVESTIDOS\s*[Xx×]\s*30\b', '30 COMPRIMIDOS REVESTIDOS'), (r'\bMG\s+COMPRIMIDOS\s+1\s*[Xx×]\s*30\b', '30 COMPRIMIDOS'),
    (r'\bMG\s+COMPRIMIDOS\s+REV\s+1\s*[Xx×]\s*30\b', '30 COMPRIMIDOS REVESTIDOS'), (r'\b4\s*BL\s*[Xx×]\s*15\s*COM\b', '60 COMPRIMIDOS'),
    (r'\bMG\s*2\s*BL\s*[Xx×]\s*15\s*C\b', '30 COMPRIMIDOS'), (r'\bMG\s*3\s*BLT\s*[Xx×]\s*10\s*COMPRIMIDOS\b', '30 COMPRIMIDOS'),
    (r'\bMG\s+COMPRIMIDOS\s+1\s*[Xx×]\s*14\b', '14 COMPRIMIDOS'), (r'\bMG\s+COMPRIMIDOS\s+1\s*[Xx×]\s*2\b', '2 COMPRIMIDOS'),
    (r'\bENV\s+1\s*[Xx×]\s*(\d+)\s+COMPRIMIDOS\b', r'\1 COMPRIMIDOS'),
]


# ============================================================
# PADRÕES DE ESPAÇAMENTO
# ============================================================

PADROES_ESPACO = [
    (re.compile(r'(\d)([A-Za-z])'), r'\1 \2'),
    (re.compile(r'([A-Za-z])(\d)'), r'\1 \2'),
    (re.compile(r'([A-Za-z])([^\w\s])'), r'\1 \2'),
    (re.compile(r'([^\w\s])([A-Za-z])'), r'\1 \2'),
    (re.compile(r'(\d)([^\w\s])'), r'\1 \2'),
    (re.compile(r'([^\w\s])(\d)'), r'\1 \2'),
    (re.compile(r'([a-zA-Z])([+;.,\-/\\])'), r'\1 \2'),
    (re.compile(r'([+;.,\-/\\])([a-zA-Z])'), r'\1 \2'),
]

REMOVER_ESPECIAIS = re.compile(r'\(N\)|["#\$\'\(\)\*\-@\.]')


# ============================================================
# PALAVRAS INDESEJADAS
# ============================================================

PALAVRAS_PARA_REMOVER = {'G', ':', ',', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'ITEM', 'BR', 'CATMAT'}


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def espacamento_num_letra(texto):
    """Adiciona espaço entre número e letra"""
    if not isinstance(texto, str):
        return texto
    return re.sub(r'(?<=\d)(?=[A-Za-z])', ' ', texto)


def adicionar_espaco_letra_num(descricao):
    """Adiciona espaço entre letra e número"""
    if not isinstance(descricao, str):
        return descricao
    return re.sub(r'([a-zA-Z])(\d)', r'\1 \2', descricao)


def processar_primeira_palavra(series):
    """Remove palavras indesejadas no início da descrição"""
    series = series.astype(str).str.strip()
    palavras = series.str.split()
    return palavras.map(
        lambda x: ' '.join(x[1:]) if x and x[0].upper() in PALAVRAS_PARA_REMOVER else ' '.join(x)
    )


# ============================================================
# FUNÇÃO PRINCIPAL DE LIMPEZA
# ============================================================

def limpar_descricoes(df):
    """
    Limpa e padroniza as descrições de produtos
    
    Parâmetros:
        df (DataFrame): DataFrame com coluna 'descricao_produto'
        
    Retorna:
        DataFrame: DataFrame com descrições limpas
    """
    print("="*60)
    print("[INICIO] Limpeza de Descrições")
    print("="*60)
    
    df_trabalho = df.copy()
    
    # 1. Converter para string
    print("[INFO] Convertendo para string...")
    desc = df_trabalho['descricao_produto'].astype(str)
    
    # 2. Aplicar padrões de espaçamento
    print("[INFO] Aplicando padrões de espaçamento...")
    for pat, sub in PADROES_ESPACO:
        desc = desc.str.replace(pat, sub, regex=True)
    
    # 3. Remover caracteres especiais e normalizar espaços
    print("[INFO] Removendo caracteres especiais...")
    desc = (
        desc
        .str.replace(REMOVER_ESPECIAIS, '', regex=True)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )
    
    # 4. Remover números ou '+' no início
    print("[INFO] Limpando início das strings...")
    desc = desc.str.replace(r'^\+*\d+\s+', '', regex=True)
    
    # 5. Aplicar substituições
    print(f"[INFO] Aplicando {len(SUBSTITUICOES)} substituições...")
    sub_dict = {pad: rep for pad, rep in SUBSTITUICOES}
    desc = desc.replace(sub_dict, regex=True)
    
    # 6. Espaçamento adicional entre números e letras
    print("[INFO] Ajustando espaçamento número-letra...")
    desc = desc.apply(espacamento_num_letra)
    desc = desc.apply(adicionar_espaco_letra_num)
    
    # 7. Remover palavras indesejadas no início
    print("[INFO] Removendo termos indesejados no início...")
    desc = processar_primeira_palavra(desc)
    
    # 8. Normalização final
    print("[INFO] Normalização final...")
    desc = desc.str.strip().str.upper()
    
    # 9. Atualizar DataFrame
    df_trabalho['descricao_produto'] = desc
    
    # 10. Limpar coluna quantidade
    print("[INFO] Limpando coluna 'quantidade'...")
    if 'quantidade' in df_trabalho.columns:
        df_trabalho['quantidade'] = (
            df_trabalho['quantidade']
            .astype(str)
            .str.replace(',', '.', regex=False)
            .pipe(pd.to_numeric, errors='coerce')
            .fillna(0)
            .astype(int)
        )
    
    print("="*60)
    print("[SUCESSO] Limpeza concluída")
    print("="*60)
    
    return df_trabalho


def salvar_dados_limpos(df, diretorio="data/processed"):
    """
    Salva dados limpos em CSV
    
    Parâmetros:
        df (DataFrame): DataFrame com dados limpos
        diretorio (str): Diretório de saída
        
    Retorna:
        str: Caminho do arquivo salvo
    """
    print("\n" + "="*60)
    print("Salvando Dados Limpos")
    print("="*60 + "\n")
    
    os.makedirs(diretorio, exist_ok=True)
    
    # Gerar timestamp para nome único
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Salvar CSV
    caminho_csv = os.path.join(diretorio, f"nfe_limpo_{timestamp}.csv")
    df.to_csv(caminho_csv, sep=';', index=False, encoding='utf-8-sig')
    print(f"[OK] Dados limpos salvos em: {caminho_csv}")
    
    return caminho_csv


def processar_limpeza_nfe(arquivo_entrada):
    """
    Processa limpeza completa de NFe
    
    Parâmetros:
        arquivo_entrada (str): Caminho do arquivo de vencimento processado
        
    Retorna:
        tuple: (df_limpo, caminho_arquivo_salvo)
    """
    print("="*60)
    print("Pipeline de Limpeza de Descrições de NFe")
    print("="*60 + "\n")
    
    print(f"[INFO] Arquivo de entrada: {arquivo_entrada}\n")
    
    # Carregar dados
    print("[INFO] Carregando dados...")
    df = pd.read_csv(arquivo_entrada, sep=';')
    print(f"[OK] {len(df):,} registros carregados\n")
    
    # Limpar descrições
    df_limpo = limpar_descricoes(df)
    
    # Estatísticas
    print("\n" + "="*60)
    print("Estatísticas de Limpeza")
    print("="*60)
    print(f"Total de registros: {len(df_limpo):,}")
    print(f"Descrições únicas (antes): {df['descricao_produto'].nunique():,}")
    print(f"Descrições únicas (depois): {df_limpo['descricao_produto'].nunique():,}")
    reducao = df['descricao_produto'].nunique() - df_limpo['descricao_produto'].nunique()
    pct_reducao = (reducao / df['descricao_produto'].nunique()) * 100 if df['descricao_produto'].nunique() > 0 else 0
    print(f"Redução de variações: {reducao:,} ({pct_reducao:.1f}%)")
    
    # Salvar
    caminho_csv = salvar_dados_limpos(df_limpo)
    
    print("\n" + "="*60)
    print("[SUCESSO] Pipeline concluído com sucesso!")
    print("="*60)
    print(f"\nArquivos gerados:")
    print(f"  - CSV: {caminho_csv}")
    
    return df_limpo, caminho_csv


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    import glob
    
    # Encontrar arquivo processado mais recente (carregamento)
    arquivos = glob.glob("data/processed/nfe_processado_*.csv")
    
    if not arquivos:
        print("[ERRO] Nenhum arquivo processado encontrado!")
        print("[INFO] Execute primeiro: python scripts/processar_nfe.py")
        exit(1)
    
    arquivo_entrada = max(arquivos, key=os.path.getmtime)
    
    # Processar limpeza
    df_limpo, caminho_saida = processar_limpeza_nfe(arquivo_entrada)
    
    # Exibir amostra
    print("\n" + "="*60)
    print("Amostra de Descrições Limpas (primeiras 10)")
    print("="*60)
    print(df_limpo[['descricao_produto']].head(10).to_string(index=False))
