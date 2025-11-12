# -*- coding: utf-8 -*-
"""
Modulo para normalizacao da coluna 'APRESENTACAO'.
Inclui funcoes complexas para formatacao de dosagens e unidades farmaceuticas.
"""
import pandas as pd
import re
from tqdm.auto import tqdm


# ==============================================================================
#      CONSTANTES E PADROES
# ==============================================================================

UNIDADES_BASE = ["MG", "G", "MCG", "ML", "L", "UI", "MEQ", "MMOL", "%"]

PADRONIZACOES = {
    r'\bAGU DESC COM SIST SEG\b': '',
    r'\bCOM SIST SEG\b': '',
    r'\bCOM SIST SEGURANCA\b': '',
    r'\b0 05 MG\b': '50 MCG',
    r'\b0 075 MG\b': '75 MCG',
    r'\bBISN COM 20 G\b': 'BG X 20 G',
    r'\b5 631\b': '5,631',
    r'\bX 14 14 28\b': 'X 56',
    r'\bX 20 10 40\b': 'X 70',
    r'\bX 28 14 42\b': 'X 84',
    r'\bX 56 28 42\b': 'X 126',
    r'\bX 28 24 4\b': 'X 56',
    r'\bX 84 72 12\b': 'X 168',
    r'\bCRE\b': 'CREME',
    r'\b1 G\b': '1000 MG',
    r'\b24 H\b': '',
    r'\bCREM\b': 'CREME',
    r'\bSOL\b': 'SOLUCAO',
    r'\bCOM\b': 'COMPRIMIDOS',
    r'\bCPD\b': 'COMPRIMIDOS',
    r'\bCOMP\b': 'COMPRIMIDOS',
    r'\bCAP\b': 'CAPSULAS',
    r'\bCP\b': 'COPO',
    r'\bCOP\b': 'COPO',
    r'\bDER\b': 'DERM',
    r'\bSEM ACUCAR\b': '',
    r'\b7 LUVAS\b': '',
    r'\b14 DEDEIRAS\b': '',
    r'\b2 DEDEIRAS\b': '',
    r'\b4 COM PLACEBO\b': '',
    r'\b7 PLACEBOS\b': '',
    r'\b21 PLACEBOS\b': '',
    r'\b12 PLACEBO\b': '',
    r'\b4 PLACEBO\b': '',
    r'\b12 PLACEBOS\b': '',
    r'\b4 PLACEBOS\b': '',
    r'\b6 PLACEBOS\b': '',
    r'\b8 PLACEBOS\b': '',
    r'\b04\b': '4',
    r'\bS ACUCAR\b': '',
    r'\bREMOVERRRRRR\b': '',
    r'\b50 COP\b': '',
    r'\b24 COP\b': '',
    r'\b96 COP\b': '',
    r'\b48 COP\b': '',
    r'\b25 COP\b': '',
    r'\bCOM 2 MMOL\b': 'CONTENDO 2 MMOL',
    r'\b1 PORTA COMPRIMIDO\b': '',
    r'\bAGU DISPOSITIVO DE SEGURANCA\b': '',
    r'\bSEM AGU\b': '',
    r'\bNBSP 01\b': '',
    r'\bEST AMP\b': 'AMP',
    r'\bOF\b': 'OFT',
    r'\bFRGOT\b': 'FR',
    r'\bSTR\b': 'STRIP',
    r'\bCAPS\b': 'CAPSULAS',
    r'\bBOMB\b': 'BOMBO',
    r'\bHOSP\b': 'HOSPITALAR',
    r'\bPREENC\b': 'PREENCHIDAS',
    r'\bPREENCH\b': 'PREENCHIDAS',
    r'\bMICROG\b': 'MICROGRANULADO',
    r'\bMCGRAN\b': 'MICROGRANULADO',
    r'\bDRG\b': 'COMPRIMIDOS',
    r'\bSAC\b': 'SACHES',
    r'\bPOM\b': 'POMADA',
    r'\bALX\b': 'AL X',
    r'\bSACH\b': 'SACHES',
    r'\bINAL\b': 'INALADOR',
    r'\bMGGRAN\b': 'MICROGRANULADO',
    r'\bORODISPERS\b': 'ORODISPERSIVEL',
    r'\bORODISP\b': 'ORODISPERSIVEL',
    r'\bSPR\b': 'SPRAY',
    r'\bACION\b': 'ACIONAMENTOS',
    r'\bOR\b': 'ORAL',
    r'\bSUS\b': 'SUSP',
    r'\bCAPI\b': 'CAPILAR',
    r'\bCAPIL\b': 'CAPILAR',
    r'\bCAPILA\b': 'CAPILAR',
    r'\bBOLS\b': 'BOLSA',
    r'\bNAS\b': 'NASAL',
    r'\bAPL\b': 'APLIC',
    r'\bGCREM\b': 'G CREME',
    r'\bSACHE\b': 'SACHES',
    r'\bXAMP\b': 'XAMP',
    r'\bSHAMP\b': 'XAMP',
    r'\bPAST\b': 'PASTILHA',
    r'\bPAS\b': 'PASTILHA',
    r'\bTB\b': 'TUBO',
    r'\bCAR\b': 'CARP',
    r'\bMGRAN\b': 'MICROGRANULADO',
    r'\bAGU\b': 'AGULHAS',
    r'\bAG\b': 'AGULHAS',
    r'\bCR\b': 'CREME',
    r'\bGIN\b': 'GINEC',
    r'\bPRENC\b': 'PREENCHIDAS',
    r'\bTRANSX\b': 'TRANS X',
    r'\bATOMIZACOES\b': 'ACIONAMENTOS',
}

# Padrao regex para blocos de dosagem
PADRAO_BLOCO = re.compile(
    r'((?:\d+\s+){1,40})'
    r'(' + '|'.join(UNIDADES_BASE) + r')'
    r'(?:\s+(' + '|'.join(UNIDADES_BASE) + r'))?'
)


# ==============================================================================
#      FUNCOES AUXILIARES
# ==============================================================================

def _join_unit(u1: str, u2: str | None) -> str:
    """Concatena as duas unidades (ex: MG e ML -> MG/ML)."""
    return f"{u1}/{u2}" if u2 else u1


def _collapse_spaces(s: str) -> str:
    """Remove espacos duplicados."""
    return re.sub(r'\s+', ' ', s).strip()


def _split_digits_letters(s: str) -> str:
    """Separa digitos de letras."""
    s = re.sub(r'(\d)([A-Z])', r'\1 \2', s)
    s = re.sub(r'([A-Z])(\d)', r'\1 \2', s)
    return s


def _fmt_decimal(intpart: str, frac: str|None) -> str:
    """Formata numero decimal com virgula."""
    intpart = "0" if not intpart.isdigit() else str(int(intpart))
    if not frac or set(frac) == {"0"}:
        return intpart
    return f"{intpart},{frac}"


def _parse_values_po_g(nums: list[str]) -> list[str]:
    """
    Parser para blocos em G quando a apresentacao e de PO (po_mode=True).
    Somente digitos.
    """
    nums = [x for x in nums if x.isdigit()]
    n = len(nums)
    out = []

    def make_decimal(a: str, b: str|None) -> str:
        a_fmt = "0" if not a.isdigit() else str(int(a))
        if not b or set(b) == {"0"}:
            return a_fmt
        return f"{a_fmt},{b}"

    i = 0
    while i < n:
        a = nums[i]
        b = nums[i+1] if i + 1 < n and nums[i+1].isdigit() else None
        if b is None:
            out.append(make_decimal(a, None))
            i += 1
        else:
            out.append(make_decimal(a, b))
            i += 2
    return out


def _parse_values_bolsa(nums: list[str]) -> list[str]:
    """
    Regras especiais para MG/ML quando houver BOLSA/BOLS na apresentacao:
    - N par de tokens: (n0,n1), (n2,n3), ...
    - N impar: formar pares da direita p/ esquerda; o primeiro token (se sobrar) fica inteiro.
    """
    nums = [x for x in nums if x.isdigit()]
    n = len(nums)
    out = []
    if n == 0:
        return out
    
    def make_decimal(a: str, b: str|None) -> str:
        a_fmt = "0" if not a.isdigit() else str(int(a))
        if not b or set(b) == {"0"}:
            return a_fmt
        return f"{a_fmt},{b}"

    if n % 2 == 0:
        # esquerda -> direita
        for i in range(0, n, 2):
            out.append(make_decimal(nums[i], nums[i+1]))
        return out
    else:
        # direita -> esquerda, preservando ordem final
        acc = []
        i = n - 1
        while i > 0:
            acc.append(make_decimal(nums[i-1], nums[i]))
            i -= 2
        if i == 0:
            left = make_decimal(nums[0], None)
            out = [left] + list(reversed(acc))
        else:
            out = list(reversed(acc))
        return out


def _parse_values(nums: list[str], unit1: str, dual_unit: bool, composite: bool, 
                 unit2: str|None=None, bolsa_mode: bool=False, po_mode: bool=False) -> list[str]:
    """
    Faz parsing de valores numericos considerando unidades e contexto.
    """
    # 1) Regras especiais primeiro
    if bolsa_mode and dual_unit and unit1 == "MG" and unit2 == "ML":
        return _parse_values_bolsa(nums)
    if po_mode and not dual_unit and unit1 == "G":
        return _parse_values_po_g(nums)

    # 2) Fluxo original
    if unit1 == "UI":
        if len(nums) > 1 and any(len(n) >= 4 for n in nums):
            return nums
        else:
            return [''.join(nums)]

    out = []
    i, n = 0, len(nums)
    is_mgml = (dual_unit and unit1 == "MG" and unit2 == "ML")
    is_mgg  = (dual_unit and unit1 == "MG" and unit2 == "G")

    while i < n:
        a = nums[i]; i += 1
        if not a.isdigit():
            continue

        frac = None

        if composite and dual_unit:
            if is_mgml:
                if i < n and nums[i].isdigit():
                    b = nums[i]
                    if len(a) == 1 and 1 <= len(b) <= 3:
                        frac = b[:2]; i += 1
                    elif len(a) >= 2 and 1 <= len(b) <= 2 and b != "10":
                        frac = b; i += 1
            elif is_mgg:
                if i < n and nums[i].isdigit():
                    b = nums[i]
                    if 1 <= len(b) <= 2:
                        frac = b; i += 1
        else:
            if not dual_unit and unit1 == "MG" and len(a) == 1 and i < n and nums[i].isdigit() and len(nums[i]) == 3:
                frac = nums[i]; i += 1
            elif not dual_unit and unit1 == "MG" and composite and i < n and nums[i] == "10" and len(a) >= 2:
                out.append(_fmt_decimal(a, None))
                continue
            elif a.strip("0") == "":
                if i < n and nums[i].isdigit():
                    cand = nums[i]
                    if composite and not dual_unit and unit1 == "G" and 3 <= len(cand) <= 4:
                        out.append(str(int(cand))); i += 1
                        continue
                    if 1 <= len(cand) <= 5:
                        frac = cand; i += 1
            else:
                if i < n and nums[i].isdigit() and 1 <= len(nums[i]) <= 2:
                    if nums[i] == "0" and (i + 1 < n and nums[i+1].isdigit()):
                        frac = "0"; i += 1
                    else:
                        frac = nums[i]; i += 1

        out.append(_fmt_decimal(a, frac))

    return out


def _format_block(nums_raw: str, u1: str, u2: str|None, composite: bool, 
                 bolsa_mode: bool, po_mode: bool) -> tuple[list[str], str]:
    """Formata um bloco de numeros com suas unidades."""
    nums = re.findall(r'\d+', nums_raw)
    unit = _join_unit(u1, u2)
    values = _parse_values(nums, u1, dual_unit=bool(u2), composite=composite, 
                          unit2=u2, bolsa_mode=bolsa_mode, po_mode=po_mode)
    return values, unit


def _merge_adjacent_same_unit(s: str, matches: list[re.Match], composite: bool, 
                             bolsa_mode: bool, po_mode: bool) -> str:
    """Mescla blocos adjacentes com mesma unidade."""
    result = []
    pos = 0
    k = 0
    while k < len(matches):
        m = matches[k]
        result.append(s[pos:m.start()])

        values_all, u = _format_block(m.group(1), m.group(2), m.group(3), 
                                     composite, bolsa_mode, po_mode)
        end_span = m.end()
        k_next = k + 1

        if composite:
            while k_next < len(matches):
                m2 = matches[k_next]
                gap = s[end_span:m2.start()]
                if gap.strip() != "":
                    break
                values2, u2 = _format_block(m2.group(1), m2.group(2), m2.group(3), 
                                           composite, bolsa_mode, po_mode)
                if u2 != u:
                    break
                values_all.extend(values2)
                end_span = m2.end()
                k_next += 1

        if len(values_all) == 1:
            result.append(f"{values_all[0]} {u}")
        else:
            result.append(f"({ ' + '.join(values_all) }) {u}")

        pos = end_span
        k = k_next

    result.append(s[pos:])
    return _collapse_spaces(''.join(result))


# ==============================================================================
#      FUNCAO PRINCIPAL DE NORMALIZACAO
# ==============================================================================

def normalizar_apresentacao(texto: str, substancia_composta: bool=False) -> str:
    """
    Normaliza apresentacao farmaceutica com tratamento inteligente de dosagens.
    
    Args:
        texto (str): Texto da apresentacao
        substancia_composta (bool): Flag para substancias compostas
        
    Returns:
        str: Texto normalizado
    """
    if not isinstance(texto, str) or not texto.strip():
        return texto

    s = texto.upper()
    s = _split_digits_letters(s)
    s = _collapse_spaces(s)
    
    # Aplica padronizacoes
    for padrao, substituto in PADRONIZACOES.items():
        s = re.sub(padrao, substituto, s)
    
    # Remove termos irrelevantes
    s = re.sub(r'\b(PVC|ACLAR|TRANS|PVDC|NBSP|MOLE|DURA|SABOR|REV|PEAD|SBR|GUARANA|EVOH|TRNS|PLASC|SISTEMA|SEGURANCA|HD|PLACEBO|PP|LIMAO|MORANGO|CONECTOR|ABACAXI|BRANCO|LAMIN|DESCART|MENTA|FRAC|CALEND|BCO|LEIT|CGT|NATURAL|PCTFE|LARANJA|TUTTI|FRUTTI|PEBD|TRANSP|INC|AMB|POLIET|OPC|PE|PAP|ACUCAR|FLUROTEC|TAMPA|VALV|POLF|FLEX|TRANSL|LIB|RETARD|CAMOMILA|MEL|E|PROL|DESSEC|LEITOSO|DE|UVA|FRAMBOESA|EQP|PET|TRANSLUCIDO|TANGERINA|DESSECANTE|BAUNILHA|CEREJA|FRUTAS|VERMELHAS|BANANA|AMBX|MOD|TRANSF|PLAS|AL|POLIOF|P|PESSEGO|OPACO|PINA|COLADA|PLANS|MAST|HDPE|RESPIMAT|DAMASCO|MAMAO|CASSIS|ABAXAXI|VD|EFEV|AMEIXA|SALADA|ALUMINIO|FLOW|PACK|APOS|RECONSTITUICAO|FLEXPRO|FLTR|C|PLAST|EFERV|SUBL|DUR|RESERVATORIO|HF|ENCAP|OPA|OPAC|ALU|III|AP|ADAPTADOR|BIOFINA|EXTEMP|BJ|EPI|COLUT|LENTA|GRAD|LARANJ|LAM|KRAFT|DP|LAR|TRANSD|REC|SC|CANELA|MACA|CAMARA|TRIPLA|ESTERIL|TRIP|BIP|DUPLO|AD|TP|BR|POLIESTER|PAPEL|LONG|CONTROL|SECO|PL|CAPAC|SUB|LING|ACD|DREN|EQ|FLEXPEN|ADU|PED|DUPLA|CAM|PROG|DEPOT|II|FLEXTOUCH|VC|HORTELA|MULTIPLA|MULTI|MULT|COCO|COC|ACO|INOX|CAM|ESPATULA|MONODOSE|PENFILL|PROTECAO|CTG|EXTENSOR|APOIO|DOSIF|ORODISPERSIVEL|RETRATIL|IAR|NOVOFINE|EPOXI|FENOLICO|OURO|TRILAMINADA|EUCALIPTO|REFRESCANTE|TONICA|ADAPT|MENTOL|ACIDO|ACETILSALICILICO|ANIS|DESC|DES|REST|HOSPITALAR|ESTEREIS|ESPAC|JET|USO|PROFISSIONAL|ULTRASAFE|PASSIVE|EXTENSORES|GANGAN|UNOPEN|INCOLOR|BRANC|CONTI|MARACUJA|SIST|FECH|PLASTICA|CONT|REMOVIVEL|CONTROLADA|PROLONG|IA|DESINT|LENT|CTFE|LIOF|INF|CT|POTASSIO|ACEROLA|DISKUS|PEHD|PEQUENO|FECHADO|SUCRALOSE|PAPAYA|MATRICIAL|EXT|PREP|CALENDARIO|MOLA|OCUMETRO|GOT|PEBDL|REVES|REVE|MINIMICROESFERAS|GENGIBRE|ROMA|ADVANCE|DIL|MARROM|BACTERIOSTATICO|CONTR|REVCT|BLAL|RETARDAD|HOSPITALAR|I|TIPO|POLIETILENO|PES|MEDIDA|MED|MEDIDOR|PLASTRANS|TRANP|PLASP|GOM|POLI|DIET|REMOV|CHOCOLATE|COLA|TRADICIONAL|FILME|POLIEST|BOCAL|DISSOL|INST|CIL|EXP|POLIPROPILENO|TAM|GRANDE|SIS|PAN|ITRAQ|IMEDU|INERTE|CARTOLINA|ENVOL|VER|PLAC|VDC|OROD|ESTOJO|TRP|COMPART|CRISTAL|MIP|LEI|VDE|HPDE|PALNS)\b', '', s)

    # Normaliza unidades compostas
    s = re.sub(r'\bMGML\b', 'MG/ML', s)
    s = re.sub(r'\bMCGML\b', 'MCG/ML', s)
    s = re.sub(r'\bUIML\b', 'UI/ML', s)
    s = re.sub(r'\bGML\b', 'G/ML', s)

    # Aplica formatacao/mescla por blocos
    matches = list(PADRAO_BLOCO.finditer(s))
    if not matches:
        return s

    # Deteccoes robustas
    bolsa_mode = bool(re.search(r'\bBOLSA\b|\bBOLS\b', s))
    po_mode    = bool(re.search(r'\bPO\b', s))
    out = _merge_adjacent_same_unit(s, matches, composite=substancia_composta, 
                                   bolsa_mode=bolsa_mode, po_mode=po_mode)

    # Normaliza pares de unidade restantes
    out = re.sub(r'\b(MG)\s+(ML)\b', r'\1/\2', out)
    out = re.sub(r'\b(G)\s+(G)\b', r'\1/\2', out)
    out = re.sub(r'\b(MCG)\s+(ML)\b', r'\1/\2', out)
    out = re.sub(r'\b(MG)\s+(G)\b', r'\1/\2', out)
    out = re.sub(r'\b(ML)\s+(ML)\b', r'\1/\2', out)
    out = re.sub(r'\bAL\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCAPSULAS GEL\b', 'CAPSULAS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bTRANS\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bEMB HOSPITALAR\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOPO MED\b', 'COPO', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOPO SAB\b', 'COPO', out, flags=re.IGNORECASE)
    out = re.sub(r'\bSEM\b\s*$', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bEMB\b\s*$', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bSIST\b\s*$', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bDISPOSITIVO\b\s*$', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bPRE ENCHIDAS\b', 'PREENCHIDAS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bPRE ENCH\b', 'PREENCHIDAS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bPORT\b.*$', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\b(\d+)\s+\1\b', r'\1', out)
    out = re.sub(r'\bHOSPITALAR\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\b3 A SERIE\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\b2 A SERIE\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\b1 A SERIE\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\b3 O SERIE\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\b2 O SERIE\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\b1 O SERIE\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\b2 PLACEBOS\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bOMCILON A M\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOMPRIMIDOS SOLUCAO\b', 'COMPRIMIDOS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOMPRIMIDOS ORAL\b', 'COMPRIMIDOS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOMPRIMIDOS ORODISPERSIVEIS\b', 'COMPRIMIDOS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOMPRIMIDOS DISP\b', 'COMPRIMIDOS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOMPRIMIDOS DISPLAY\b', 'COMPRIMIDOS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bBL PA\b', 'BL', out, flags=re.IGNORECASE)
    out = re.sub(r'\bBL BL\b', 'BL', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCX BL\b', 'BL', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCART BL\b', 'BL', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOMPRIMIDOS BOLSA\b', 'COMPRIMIDOS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOMPRIMIDOS SUSP\b', 'COMPRIMIDOS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bAGULHAS COMPRIMIDOS SEG\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCOPO\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bMLSIST\b', 'ML', out, flags=re.IGNORECASE)
    out = re.sub(r'\bPREENCHIDA\b', 'PREENCHIDAS', out, flags=re.IGNORECASE)
    out = re.sub(r'\bFA FA\b', 'FA', out, flags=re.IGNORECASE)
    out = re.sub(r'\bOMCILON A ORABASE\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\(\s*\)', '', out)
    out = re.sub(r'\bSER DOS\b', 'SER DOSAD', out, flags=re.IGNORECASE)
    out = re.sub(r'\bORODISPERSIVEL\b', 'ORODISPERSIVEIS', out, flags=re.IGNORECASE)
    out = re.sub(r'\s{2,}', ' ', out).strip()
    
    return _collapse_spaces(out)


def limpar_apresentacao_final(texto: str) -> str:
    """
    Limpeza final da apresentacao preservando dados relevantes.
    
    Args:
        texto (str): Texto da apresentacao
        
    Returns:
        str: Texto limpo
    """
    if not isinstance(texto, str) or not texto.strip():
        return texto

    out = texto

    # Remove apenas parenteses contendo 'EMB'
    out = re.sub(r'\([^)]*\bEMB\b[^)]*\)', '', out, flags=re.IGNORECASE)

    # Corrige 'BL + X' -> 'BL X'
    out = re.sub(r'\bBL\s*\+\s*', 'BL ', out, flags=re.IGNORECASE)

    # Simplifica 'FA + FA' -> 'FA'
    out = re.sub(r'\b(FA)\s*\+\s*\1\b', r'\1', out, flags=re.IGNORECASE)

    # Simplifica '+ +' -> '+'
    out = re.sub(r'\+\s*\+', '+', out)

    # Remove '+' final
    out = re.sub(r'\+$', '', out)

    # Remove '.' final
    out = re.sub(r'\.\s*$', '', out)

    # Substitui '/' por espaco, exceto em dosagens MG/ML, G/ML, MCG/ML, etc.
    out = re.sub(
        r'(?<!\bMG)(?<!\bG)(?<!\bMCG)(?<!\bKG)/(?!(ML|L|G|MG|MCG|KG)\b)',
        ' ',
        out,
        flags=re.IGNORECASE
    )

    # Remove lixo "&;01"
    out = re.sub(r'&;01', '', out)

    # Corrige "MLSABOR" → "ML"
    out = re.sub(r'\bMLSABOR\b', 'ML', out, flags=re.IGNORECASE)

    # Remove " S AGULHAS"
    out = re.sub(r'\s*S\s+AGULHAS\b', '', out, flags=re.IGNORECASE)

    # Remove "(500 ML)" se ja houver "500 ML" fora dos parenteses
    out = re.sub(r'\(\s*(\d+\s*ML)\s*\)(?=.*\b\1\b)', '', out, flags=re.IGNORECASE)

    # Remove parentese de abertura se for o ultimo caractere
    out = re.sub(r'\($', '', out)

    # Remove "+ ACESSORIO" somente se estiver no final da string
    out = re.sub(r'\+\s*ACESSORIO\s*$', '', out, flags=re.IGNORECASE)

    # Remove literal "(COMPRIMIDOS 500 ML)"
    out = re.sub(r'\(\s*COMPRIMIDOS\s*500\s*ML\s*\)', '', out, flags=re.IGNORECASE)

    # Remove palavras especificas
    out = re.sub(r'\bPVCTRANS\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bCAMA\b', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\bMICROGRANULADO\b', '', out, flags=re.IGNORECASE)

    # Remove literal "( + )"
    out = re.sub(r'\(\s*\+\s*\)', '', out)

    # Remove literais especificos de grupo/adq
    out = re.sub(r'\(\s*ADQ\.?\s*RES\.?\s*572\s*05\s*4\s*2002\s*\)', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\(\s*GRUPO\s*O\s*\)', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\(\s*GRUPO\s*A\s*\)', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\(\s*BRUPO\s*B\s*\)', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\(\s*BRUPO\s*AB\s*\)', '', out, flags=re.IGNORECASE)

    # Remove "( EMBALAGEM )" no final
    out = re.sub(r'\(\s*EMBALAGEM\s*\)\s*$', '', out, flags=re.IGNORECASE)

    # Remove '+' no final
    out = re.sub(r'\+\s*$', '', out)

    # Remove "+ KIT INFUS" no final
    out = re.sub(r'\+\s*KIT\s*INFUS\s*$', '', out, flags=re.IGNORECASE)

    # Remove "+ COL DOS" no final
    out = re.sub(r'\+\s*COL\s*DOS\s*$', '', out, flags=re.IGNORECASE)

    # Remove ")" no final
    out = re.sub(r'\)\s*$', '', out)

    # Remove "+ 1 APLIC" no final
    out = re.sub(r'\+\s*1\s*APLIC\s*$', '', out, flags=re.IGNORECASE)

    # Remove "+ 1 CAN APLIC" no final
    out = re.sub(r'\+\s*1\s*CAN\s*APLIC\s*$', '', out, flags=re.IGNORECASE)

    # Remove "X 1 APLIC" no final
    out = re.sub(r'X\s*1\s*APLIC\s*$', '', out, flags=re.IGNORECASE)

    # Remove "+ DOSADOR" no final
    out = re.sub(r'\+\s*DOSADOR\s*$', '', out, flags=re.IGNORECASE)

    # Substitui padroes tipo "BL 250 120 X" → "BL X"
    out = re.sub(r'\bBL\s*\d+\s*\d+\s*X\b', 'BL X', out, flags=re.IGNORECASE)

    # Corrige "BL L X" → "BL X"
    out = re.sub(r'\bBL\s*L\s*X\b', 'BL X', out, flags=re.IGNORECASE)

    # Remove literal "( EST )"
    out = re.sub(r'\(\s*EST\s*\)', '', out, flags=re.IGNORECASE)

    # Remove "COMPRIMIDOS FILTRO" no final
    out = re.sub(r'COMPRIMIDOS\s*FILTRO\s*$', '', out, flags=re.IGNORECASE)

    # Remove "+ (SAB." no final
    out = re.sub(r'\+\s*\(SAB\.\s*$', '', out, flags=re.IGNORECASE)

    # Corrige "PRE - ENCHIDAS" → "PREENCHIDAS"
    out = re.sub(r'\bPRE\s*-\s*ENCHIDAS\b', 'PREENCHIDAS', out, flags=re.IGNORECASE)

    # Remove dois pontos consecutivos ".."
    out = re.sub(r'\.\.+', '', out)
    out = re.sub(r'-', '', out)
    out = re.sub(r'\(\s*-\s*\)', '', out)
    out = re.sub(r'(\d)\.(?=\d)', r'\1,', out)
    out = re.sub(r'\(\s*SR\s*\)', '', out, flags=re.IGNORECASE)
    out = re.sub(r'\(\s*\)', '', out)
    # Remove '+' no final
    out = re.sub(r'\+\s*$', '', out)
    out = re.sub(r'\s+', ' ', out).strip()

    return out


def expandir_cx_bl(texto: str) -> str:
    """
    Detecta padroes como 'CX 250 BL X 4' e transforma em 'BL X 1000'
    (multiplicando os valores numericos automaticamente).
    
    Args:
        texto (str): Texto da apresentacao
        
    Returns:
        str: Texto com multiplicacoes expandidas
    """
    if not isinstance(texto, str):
        return texto

    def substituir(match):
        cx_valor = int(match.group(1))
        mult_valor = int(match.group(2))
        resultado = cx_valor * mult_valor
        return f'BL X {resultado}'

    return re.sub(r'\bCX\s*(\d+)\s*BL\s*X\s*(\d+)\b', substituir, texto, flags=re.IGNORECASE)


# ==============================================================================
#      FUNCAO PRINCIPAL PARA PROCESSAR O DATAFRAME
# ==============================================================================

def processar_apresentacao(df):
    """
    Processa a coluna APRESENTACAO com normalizacao completa.
    
    Args:
        df (pandas.DataFrame): DataFrame com colunas APRESENTACAO e SUBSTANCIA_COMPOSTA
        
    Returns:
        pandas.DataFrame: DataFrame com APRESENTACAO_NORMALIZADA criada
    """
    print("\n" + "=" * 80)
    print("ETAPA 3: PROCESSAMENTO DA APRESENTACAO")
    print("=" * 80)
    
    if 'APRESENTACAO' not in df.columns:
        print("[AVISO] Coluna 'APRESENTACAO' nao encontrada. Pulando processamento.")
        return df
    
    # Ajustar espacos ao redor de '+'
    print("Ajustando espacos ao redor de '+'...")
    df['APRESENTACAO'] = df['APRESENTACAO'].str.replace(r'\s*\+\s*', ' + ', regex=True)
    
    # Normalizacao principal
    print("Aplicando normalizacao de apresentacao...")
    tqdm.pandas(desc="Normalizando apresentacoes")
    
    def _normalizar_row(row):
        return normalizar_apresentacao(
            row.APRESENTACAO, 
            bool(row.SUBSTANCIA_COMPOSTA) if hasattr(row, 'SUBSTANCIA_COMPOSTA') else False
        )
    
    df.loc[:, 'APRESENTACAO_NORMALIZADA'] = df.progress_apply(_normalizar_row, axis=1)
    
    # Limpeza final vetorizada
    print("Aplicando limpeza final...")
    df.loc[:, 'APRESENTACAO_NORMALIZADA'] = (
        df['APRESENTACAO_NORMALIZADA']
        .astype(str)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )
    
    # Aplicar limpeza adicional
    df['APRESENTACAO_NORMALIZADA'] = df['APRESENTACAO_NORMALIZADA'].apply(limpar_apresentacao_final)
    
    # Expandir CX BL
    print("Expandindo padroes CX BL...")
    df['APRESENTACAO_NORMALIZADA'] = df['APRESENTACAO_NORMALIZADA'].apply(expandir_cx_bl)
    
    print(f"\n[OK] APRESENTACAO processada com sucesso!")
    print(f"Total de apresentacoes unicas: {df['APRESENTACAO_NORMALIZADA'].nunique():,}")
    
    return df


def criar_flag_substancia_composta(df):
    """
    Cria flag SUBSTANCIA_COMPOSTA para identificar medicamentos com multiplos principios ativos.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'PRINCIPIO ATIVO'
        
    Returns:
        pandas.DataFrame: DataFrame com nova coluna SUBSTANCIA_COMPOSTA
    """
    if 'PRINCIPIO ATIVO' in df.columns:
        df['SUBSTANCIA_COMPOSTA'] = df['PRINCIPIO ATIVO'].str.contains(r'\+', na=False)
        print(f"[OK] Flag SUBSTANCIA_COMPOSTA criada: {df['SUBSTANCIA_COMPOSTA'].sum():,} compostos identificados")
    else:
        df['SUBSTANCIA_COMPOSTA'] = False
        print("[AVISO] Coluna 'PRINCIPIO ATIVO' nao encontrada. Flag SUBSTANCIA_COMPOSTA definida como False")
    
    return df


if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")
