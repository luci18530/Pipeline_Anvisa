# -*- coding: utf-8 -*-
"""ETAPA 17.5: conversao economica unidade/caixa antes do sobrepreco.

A etapa cria colunas auditaveis para comparar o valor praticado com o teto
CMED na mesma escala economica. A regra e conservadora: quando a unidade da
NFe parece avulsa, mas o valor unitario ja esta perto do teto de caixa, a
conversao e bloqueada e a linha fica marcada para revisao.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.io_utils import ler_zip_csv, salvar_csv, salvar_zip_csv
from pipelines.nfe.src.paths import DATA_DIR

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa17_consolidado_final.zip"
OUTPUT_DIR = DATA_DIR / "processed"
OUTPUT_ZIP = OUTPUT_DIR / "df_etapa17_5_unidade_caixa.zip"
OUTPUT_RESUMO = OUTPUT_DIR / "df_etapa17_5_unidade_caixa_resumo.csv"
OUTPUT_AMOSTRAS = OUTPUT_DIR / "df_etapa17_5_unidade_caixa_amostras.csv"
CSV_NAME = "df_etapa17_5_unidade_caixa.csv"

CONFIANCA_MINIMA_USO = 0.75
LIMITE_VALOR_JA_PARECE_CAIXA = 0.10
FATOR_MAXIMO_CONSERVADOR = 1000

UNIDADES_CAIXA = {
    "CX",
    "CXA",
    "CAIXA",
    "CAIXAS",
    "CART",
    "CARTUCHO",
    "CARTUCHOS",
    "CT",
    "KIT",
    "KITS",
    "PCT",
    "PACOTE",
    "PACK",
    "BL",
    "BLISTER",
}

UNIDADES_AVULSAS = {
    "UN",
    "UND",
    "UNID",
    "UNIDADE",
    "UNIDADES",
    "COMP",
    "CMP",
    "COM",
    "COMPR",
    "COMPRIMIDO",
    "COMPRIMIDOS",
    "CP",
    "CPR",
    "CAP",
    "CAPS",
    "CAPSULA",
    "CAPSULAS",
    "DRG",
    "DRAGEA",
    "AMP",
    "AM",
    "AMPOLA",
    "AMPOLAS",
    "FA",
    "F/A",
    "FR",
    "FRA",
    "FRS",
    "FRASCO",
    "FRASCOS",
    "BIS",
    "BISNAGA",
    "BG",
    "ENV",
    "ENVELOPE",
    "SACHE",
    "SER",
    "SERINGA",
    "TUB",
    "TUBO",
    "TB",
    "FLAC",
    "VD",
}
UNIDADES_GENERICAS = {"UN", "UND", "UNID", "UNIDADE", "UNIDADES"}
UNIDADES_SOLIDAS = {"COMP", "CMP", "COM", "COMPR", "COMPRIMIDO", "COMPRIMIDOS", "CP", "CPR", "CAP", "CAPS", "CAPSULA", "CAPSULAS", "DRG", "DRAGEA"}
UNIDADES_INJETAVEIS = {"AMP", "AM", "AMPOLA", "AMPOLAS", "FA", "F/A"}
UNIDADES_CONTAINER = {"FR", "FRA", "FRS", "FRASCO", "FRASCOS", "BIS", "BISNAGA", "BG", "TUB", "TUBO", "TB", "VD"}

PADROES_FATOR_DESCRICAO = [
    re.compile(
        r"\b(?:C\s*/|C/|COM|CX\s*(?:COM|C/)?|CAIXA\s+COM|CT\s+COM)\s*"
        r"(\d{1,4})\s*"
        r"(?:COMPRIMIDOS?|CPR|COMP|CAPSULAS?|CAPS?|AMPOLAS?|FRASCOS?|"
        r"UNIDADES?|UND|DOSES?|SACHES?|ENVELOPES?)\b",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"\b(\d{1,4})\s*"
        r"(?:COMPRIMIDOS?|CPR|COMP|CAPSULAS?|CAPS?|AMPOLAS?|FRASCOS?|"
        r"UNIDADES?|UND|DOSES?|SACHES?|ENVELOPES?)\b",
        flags=re.IGNORECASE,
    ),
    re.compile(r"\b1\s*X\s*(\d{1,4})\b", flags=re.IGNORECASE),
]

PADRAO_FATOR_NAO_CAIXA = re.compile(
    r"\b(?:DOSES?|ACIONAMENTOS?|JATOS?)\b",
    flags=re.IGNORECASE,
)
PADRAO_SOLIDO = re.compile(
    r"\b(?:COMPRIMIDOS?|COMPR|COMP|CPR|CAPSULAS?|CAPS?|DRAGEAS?|DRG)\b",
    flags=re.IGNORECASE,
)
PADRAO_INJETAVEL = re.compile(
    r"\b(?:INJ|INJETAVEL|AMP|AMPOLA|AMPOLAS|FR\s*AMP|FA|DILUENTE)\b",
    flags=re.IGNORECASE,
)
PADRAO_LIQUIDO_ORAL = re.compile(
    r"\b(?:SUSPENSAO|SUSP|XAROPE|XPE|SOLUCAO\s+ORAL|SOL\s+ORAL|GOTAS?)\b",
    flags=re.IGNORECASE,
)
PADRAO_EMBALAGEM_EXPLICITA = re.compile(
    r"\b(?:CX|CAIXA|C\s*/|C/|BL\s*X|FR\s*X|AMP\s*X|FA\s*X|F/A\s*X|BIS\s*X)\s*\d+",
    flags=re.IGNORECASE,
)
PADRAO_MG_POR_ML_EXPLICITO = re.compile(
    r"(\d+(?:[,.]\d+)?)\s*MG\s*/\s*(\d+(?:[,.]\d+)?)?\s*ML\b",
    flags=re.IGNORECASE,
)
LIMITE_DIVERGENCIA_CONCENTRACAO = 0.25


def _serie_padrao(df: pd.DataFrame, coluna: str, default: object = pd.NA) -> pd.Series:
    if coluna in df.columns:
        return df[coluna]
    return pd.Series(default, index=df.index)


def _normalizar_unidade(valor: object) -> str:
    if pd.isna(valor):
        return ""
    texto = str(valor).strip().upper()
    texto = re.sub(r"\s+", " ", texto)
    texto = texto.replace(".", "").replace("-", "").replace("_", "")
    return texto


def _bool_series(serie: pd.Series) -> pd.Series:
    if serie.dtype == bool:
        return serie.fillna(False)
    return serie.astype(str).str.strip().str.upper().isin({"1", "TRUE", "SIM", "S", "YES"})


def extrair_fator_descricao(texto: object) -> float:
    """Extrai fator de embalagem de descricao/apresentacao com regex conservadora."""
    if pd.isna(texto):
        return np.nan

    texto_norm = str(texto).upper()
    candidatos: list[int] = []
    for padrao in PADROES_FATOR_DESCRICAO:
        for match in padrao.finditer(texto_norm):
            try:
                valor = int(match.group(1))
            except (TypeError, ValueError):
                continue
            if 1 < valor <= FATOR_MAXIMO_CONSERVADOR:
                candidatos.append(valor)

    if not candidatos:
        return np.nan

    # Em apresentacoes compostas, o maior fator tende a representar a caixa.
    return float(max(candidatos))


def _parse_decimal_br(valor: str | None) -> float:
    if valor is None or valor == "":
        return 1.0
    return float(valor.replace(",", "."))


def extrair_concentracao_mg_ml(texto: object) -> float:
    """Retorna concentracao em mg/ml quando o texto traz padrao explicito."""
    if pd.isna(texto):
        return np.nan

    match = PADRAO_MG_POR_ML_EXPLICITO.search(str(texto).upper())
    if not match:
        return np.nan

    try:
        mg = _parse_decimal_br(match.group(1))
        ml = _parse_decimal_br(match.group(2))
    except ValueError:
        return np.nan

    if mg <= 0 or ml <= 0:
        return np.nan
    return mg / ml


def carregar_dados() -> pd.DataFrame:
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(
            f"Arquivo {INPUT_ZIP.name} nao encontrado. Execute a Etapa 17 antes."
        )

    print("\n" + "=" * 80)
    print("CARREGANDO DADOS DA ETAPA 17 PARA CONVERSAO UNIDADE/CAIXA")
    print("=" * 80)
    return ler_zip_csv(INPUT_ZIP, sep=";", log_progresso=True)


def _preparar_fator(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    fator_cmed = pd.to_numeric(_serie_padrao(df, "QUANTIDADE UNIDADES"), errors="coerce")
    fator_ia = pd.to_numeric(_serie_padrao(df, "IA_QUANTIDADE UNIDADES"), errors="coerce")

    descricao_base = (
        _serie_padrao(df, "APRESENTACAO").fillna("").astype(str)
        + " "
        + _serie_padrao(df, "descricao_produto").fillna("").astype(str)
    )

    fator_descricao = pd.Series(np.nan, index=df.index, dtype="float64")
    precisa_fallback = ~(fator_cmed.gt(0) | fator_ia.gt(0))
    if precisa_fallback.any():
        fator_descricao.loc[precisa_fallback] = descricao_base.loc[precisa_fallback].map(
            extrair_fator_descricao
        )

    fator = pd.Series(np.nan, index=df.index, dtype="float64")
    origem = pd.Series("INDETERMINADO", index=df.index, dtype="object")
    confianca = pd.Series(0.0, index=df.index, dtype="float64")

    mask_cmed = fator_cmed.gt(0) & fator_cmed.le(FATOR_MAXIMO_CONSERVADOR)
    fator.loc[mask_cmed] = fator_cmed.loc[mask_cmed]
    origem.loc[mask_cmed] = "CMED_QUANTIDADE_UNIDADES"
    confianca.loc[mask_cmed] = np.where(fator_cmed.loc[mask_cmed].gt(1), 0.95, 0.85)

    mask_ia = fator.isna() & fator_ia.gt(0) & fator_ia.le(FATOR_MAXIMO_CONSERVADOR)
    fator.loc[mask_ia] = fator_ia.loc[mask_ia]
    origem.loc[mask_ia] = "IA_QUANTIDADE_UNIDADES"
    confianca.loc[mask_ia] = np.where(fator_ia.loc[mask_ia].gt(1), 0.75, 0.65)

    mask_desc = fator.isna() & fator_descricao.gt(1)
    fator.loc[mask_desc] = fator_descricao.loc[mask_desc]
    origem.loc[mask_desc] = "DESCRICAO_PRODUTO"
    confianca.loc[mask_desc] = 0.65

    return fator, origem, confianca


def aplicar_conversao_unidade_caixa(df_entrada: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas de conversao sem depender de arquivos."""
    df = df_entrada.copy()

    unidade_original = _serie_padrao(df, "unidade").copy()
    unidade_norm = unidade_original.map(_normalizar_unidade)
    unidade_token = unidade_norm.str.extract(r"([A-Z/]+)", expand=False).fillna("")

    mask_caixa = unidade_token.isin(UNIDADES_CAIXA)
    mask_avulsa = unidade_token.isin(UNIDADES_AVULSAS)
    mask_generica = unidade_token.isin(UNIDADES_GENERICAS)
    mask_solida = unidade_token.isin(UNIDADES_SOLIDAS)
    mask_injetavel = unidade_token.isin(UNIDADES_INJETAVEIS)
    mask_container = unidade_token.isin(UNIDADES_CONTAINER)
    mask_misto = unidade_norm.str.contains(r"\+|/", regex=True, na=False) & ~mask_avulsa

    df["unidade_original"] = unidade_original
    df["unidade_semantica"] = np.select(
        [mask_caixa, mask_avulsa, mask_misto],
        ["CAIXA", "UNIDADE_AVULSA", "MISTO"],
        default="INDETERMINADO",
    )
    df["confianca_unidade_semantica"] = np.select(
        [mask_caixa, mask_avulsa, mask_misto],
        [0.95, 0.85, 0.45],
        default=0.0,
    ).astype(float)

    fator, origem, confianca_fator = _preparar_fator(df)
    df["fator_unidades_por_caixa"] = fator
    df["origem_conversao_unidade"] = origem
    df["confianca_fator_caixa"] = confianca_fator

    quantidade = pd.to_numeric(_serie_padrao(df, "quantidade"), errors="coerce")
    valor_unitario = pd.to_numeric(_serie_padrao(df, "valor_unitario"), errors="coerce")
    teto = pd.to_numeric(_serie_padrao(df, "PRECO_MAXIMO_REFINADO"), errors="coerce")

    texto_apresentacao = (
        _serie_padrao(df, "APRESENTACAO").fillna("").astype(str)
        + " "
        + _serie_padrao(df, "descricao_produto").fillna("").astype(str)
    )
    fator_nao_caixa = fator.gt(1) & texto_apresentacao.str.contains(
        PADRAO_FATOR_NAO_CAIXA,
        na=False,
    )
    texto_descricao = _serie_padrao(df, "descricao_produto").fillna("").astype(str)
    texto_cmed = _serie_padrao(df, "APRESENTACAO").fillna("").astype(str)
    texto_categoria = (
        _serie_padrao(df, "TIPO DE PRODUTO").fillna("").astype(str)
        + " "
        + _serie_padrao(df, "CLASSE TERAPEUTICA").fillna("").astype(str)
        + " "
        + _serie_padrao(df, "GRUPO TERAPEUTICO").fillna("").astype(str)
        + " "
        + _serie_padrao(df, "GRUPO ANATOMICO").fillna("").astype(str)
    )
    desc_solido = texto_descricao.str.contains(PADRAO_SOLIDO, na=False)
    desc_injetavel = texto_descricao.str.contains(PADRAO_INJETAVEL, na=False)
    desc_liquido_oral = texto_descricao.str.contains(PADRAO_LIQUIDO_ORAL, na=False)
    cmed_solido = texto_cmed.str.contains(PADRAO_SOLIDO, na=False)
    cmed_injetavel = texto_cmed.str.contains(PADRAO_INJETAVEL, na=False)
    categoria_solida = texto_categoria.str.contains(PADRAO_SOLIDO, na=False)
    categoria_injetavel = texto_categoria.str.contains(PADRAO_INJETAVEL, na=False)
    categoria_container = texto_categoria.str.contains(
        r"\b(?:FRASCO|BISNAGA|SOLUCAO|SUSPENSAO|XAROPE|OFT|OTO|DERM)\b",
        flags=re.IGNORECASE,
        regex=True,
        na=False,
    )
    forma_incompativel = (
        (desc_injetavel & cmed_solido)
        | (desc_solido & cmed_injetavel)
        | (desc_liquido_oral & cmed_solido)
    )
    conc_desc = texto_descricao.map(extrair_concentracao_mg_ml)
    conc_cmed = texto_cmed.map(extrair_concentracao_mg_ml)
    conc_maior = pd.concat([conc_desc, conc_cmed], axis=1).max(axis=1)
    conc_menor = pd.concat([conc_desc, conc_cmed], axis=1).min(axis=1)
    dosagem_incompativel = (
        conc_desc.notna()
        & conc_cmed.notna()
        & conc_menor.gt(0)
        & ((conc_maior / conc_menor - 1) > LIMITE_DIVERGENCIA_CONCENTRACAO)
    )

    fator_valido = fator.gt(0) & ~fator_nao_caixa
    fator_para_calc = fator.where(fator_valido)
    fator_maior_que_um = fator_para_calc.gt(1)
    razao_original_teto = (valor_unitario / teto).replace([np.inf, -np.inf], np.nan)
    descricao_embalagem_explicita = texto_descricao.str.contains(
        PADRAO_EMBALAGEM_EXPLICITA,
        na=False,
    )
    quantidade_sugere_avulsa = quantidade.notna() & fator_para_calc.notna() & quantidade.ge(fator_para_calc)
    preco_sugere_avulsa = razao_original_teto.notna() & razao_original_teto.lt(LIMITE_VALOR_JA_PARECE_CAIXA)
    contexto_forma_compativel = (
        (mask_solida & (desc_solido | categoria_solida | cmed_solido))
        | (mask_injetavel & (desc_injetavel | categoria_injetavel | cmed_injetavel))
        | (mask_container & (desc_liquido_oral | categoria_container))
        | (mask_generica & desc_solido & (categoria_solida | cmed_solido))
        | (mask_generica & desc_injetavel & (categoria_injetavel | cmed_injetavel))
        | (mask_generica & desc_liquido_oral & categoria_container)
    )
    unidade_especifica_avulsa = mask_avulsa & ~mask_generica
    contexto_suporta_avulsa = (
        unidade_especifica_avulsa
        | contexto_forma_compativel
        | (mask_generica & quantidade_sugere_avulsa & preco_sugere_avulsa)
    )
    descricao_parece_apresentacao_inteira = (
        mask_generica
        & fator_maior_que_um
        & descricao_embalagem_explicita
        & quantidade.notna()
        & quantidade.lt(fator_para_calc)
    )
    contexto_insuficiente_unidade_generica = (
        mask_generica
        & fator_maior_que_um
        & ~contexto_suporta_avulsa
        & ~descricao_parece_apresentacao_inteira
    )
    contexto_produto = pd.Series("NAO_APLICAVEL", index=df.index, dtype="object")
    contexto_produto.loc[mask_caixa] = "UNIDADE_ORIGINAL_CAIXA"
    contexto_produto.loc[unidade_especifica_avulsa] = "UNIDADE_ESPECIFICA_AVULSA"
    contexto_produto.loc[mask_generica & contexto_forma_compativel] = "UNIDADE_GENERICA_COM_FORMA_COMPATIVEL"
    contexto_produto.loc[
        mask_generica & quantidade_sugere_avulsa & preco_sugere_avulsa
    ] = "UNIDADE_GENERICA_QUANTIDADE_PRECO_SUGEREM_AVULSA"
    contexto_produto.loc[descricao_parece_apresentacao_inteira] = "DESCRICAO_SUGERE_APRESENTACAO_INTEIRA"
    contexto_produto.loc[contexto_insuficiente_unidade_generica] = "CONTEXTO_INSUFICIENTE_UNIDADE_GENERICA"
    df["contexto_produto_unidade"] = contexto_produto
    df["confianca_contexto_produto"] = np.select(
        [
            mask_caixa,
            unidade_especifica_avulsa,
            mask_generica & contexto_forma_compativel,
            mask_generica & quantidade_sugere_avulsa & preco_sugere_avulsa,
            descricao_parece_apresentacao_inteira,
            contexto_insuficiente_unidade_generica,
        ],
        [0.95, 0.90, 0.80, 0.75, 0.85, 0.35],
        default=0.50,
    ).astype(float)
    df["sinais_contexto_produto"] = (
        "desc_solido=" + desc_solido.astype(str)
        + "|desc_injetavel=" + desc_injetavel.astype(str)
        + "|desc_liquido_oral=" + desc_liquido_oral.astype(str)
        + "|categoria_solida=" + categoria_solida.astype(str)
        + "|categoria_injetavel=" + categoria_injetavel.astype(str)
        + "|categoria_container=" + categoria_container.astype(str)
        + "|descricao_embalagem=" + descricao_embalagem_explicita.astype(str)
        + "|quantidade_sugere_avulsa=" + quantidade_sugere_avulsa.astype(str)
        + "|preco_sugere_avulsa=" + preco_sugere_avulsa.astype(str)
    )

    tipo_compra = pd.Series("INDETERMINADO", index=df.index, dtype="object")
    tipo_compra.loc[mask_caixa & fator_valido] = "CAIXA"
    tipo_compra.loc[mask_avulsa & fator_valido & fator_maior_que_um] = "FRACAO_DE_CAIXA"
    tipo_compra.loc[mask_avulsa & fator_valido & ~fator_maior_que_um] = "UNIDADE_AVULSA"
    tipo_compra.loc[mask_misto & fator_valido] = "MISTO"
    df["tipo_compra"] = tipo_compra

    converte_avulsa = mask_avulsa & fator_valido
    df["quantidade_caixa_equivalente"] = np.nan
    df.loc[mask_caixa & quantidade.notna(), "quantidade_caixa_equivalente"] = quantidade.loc[
        mask_caixa & quantidade.notna()
    ]
    df.loc[converte_avulsa & quantidade.notna(), "quantidade_caixa_equivalente"] = (
        quantidade.loc[converte_avulsa & quantidade.notna()]
        / fator_para_calc.loc[converte_avulsa & quantidade.notna()]
    )

    df["valor_unitario_caixa_equivalente"] = np.nan
    df.loc[mask_caixa & valor_unitario.notna(), "valor_unitario_caixa_equivalente"] = (
        valor_unitario.loc[mask_caixa & valor_unitario.notna()]
    )
    df.loc[converte_avulsa & valor_unitario.notna(), "valor_unitario_caixa_equivalente"] = (
        valor_unitario.loc[converte_avulsa & valor_unitario.notna()]
        * fator_para_calc.loc[converte_avulsa & valor_unitario.notna()]
    )

    df["teto_caixa_equivalente"] = teto

    confianca_unidade = pd.to_numeric(df["confianca_unidade_semantica"], errors="coerce").fillna(0)
    confianca_fator = pd.to_numeric(df["confianca_fator_caixa"], errors="coerce").fillna(0)
    df["confianca_conversao_unidade"] = np.minimum(confianca_unidade, confianca_fator)

    valor_parece_caixa = (
        converte_avulsa
        & fator_maior_que_um
        & valor_unitario.notna()
        & teto.notna()
        & teto.gt(0)
        & (valor_unitario / teto >= LIMITE_VALOR_JA_PARECE_CAIXA)
    )

    usar = (
        df["valor_unitario_caixa_equivalente"].notna()
        & df["teto_caixa_equivalente"].notna()
        & df["teto_caixa_equivalente"].gt(0)
        & (df["confianca_conversao_unidade"] >= CONFIANCA_MINIMA_USO)
        & ~valor_parece_caixa
        & ~forma_incompativel
        & ~dosagem_incompativel
        & ~descricao_parece_apresentacao_inteira
        & ~contexto_insuficiente_unidade_generica
    )
    df["usar_valor_unitario_caixa_equivalente"] = usar.astype(bool)

    obs = pd.Series("", index=df.index, dtype="object")
    obs.loc[descricao_parece_apresentacao_inteira] = "descricao_sugere_apresentacao_inteira"
    obs.loc[contexto_insuficiente_unidade_generica] = "contexto_insuficiente_unidade_generica"
    obs.loc[valor_parece_caixa] = "valor_unitario_ja_proximo_teto_caixa"
    obs.loc[fator_nao_caixa] = "fator_representa_dose_nao_caixa"
    obs.loc[forma_incompativel] = "forma_nfe_cmed_incompativel"
    obs.loc[dosagem_incompativel] = "dosagem_nfe_cmed_incompativel"
    obs.loc[fator.isna()] = "fator_indisponivel"
    obs.loc[~(mask_caixa | mask_avulsa | mask_misto)] = "unidade_indeterminada"
    obs.loc[usar] = "conversao_aplicavel_etapa18"
    df["observacao_conversao_unidade"] = obs

    return df


def gerar_resumos(df: pd.DataFrame, exportar: bool = True) -> None:
    print("\n" + "=" * 80)
    print("GERANDO RESUMOS DA ETAPA 17.5")
    print("=" * 80)

    partes = []
    for coluna in [
        "unidade_semantica",
        "tipo_compra",
        "origem_conversao_unidade",
        "usar_valor_unitario_caixa_equivalente",
        "observacao_conversao_unidade",
    ]:
        if coluna not in df.columns:
            continue
        resumo = (
            df[coluna]
            .fillna("NAO_INFORMADO")
            .value_counts(dropna=False)
            .rename_axis("valor")
            .reset_index(name="quantidade")
        )
        resumo.insert(0, "metrica", coluna)
        resumo["percentual"] = (resumo["quantidade"] / len(df) * 100).round(2)
        partes.append(resumo)

    resumo_final = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()

    colunas_amostra = [
        "descricao_produto",
        "unidade_original",
        "unidade_semantica",
        "tipo_compra",
        "quantidade",
        "valor_unitario",
        "PRECO_MAXIMO_REFINADO",
        "fator_unidades_por_caixa",
        "quantidade_caixa_equivalente",
        "valor_unitario_caixa_equivalente",
        "confianca_conversao_unidade",
        "contexto_produto_unidade",
        "confianca_contexto_produto",
        "sinais_contexto_produto",
        "usar_valor_unitario_caixa_equivalente",
        "observacao_conversao_unidade",
        "TIPO DE PRODUTO",
        "CLASSE TERAPEUTICA",
        "APRESENTACAO",
    ]
    colunas_amostra = [col for col in colunas_amostra if col in df.columns]
    amostras = pd.concat(
        [
            df[df["usar_valor_unitario_caixa_equivalente"]].head(100),
            df[df["observacao_conversao_unidade"] == "valor_unitario_ja_proximo_teto_caixa"].head(100),
            df[df["tipo_compra"] == "INDETERMINADO"].head(100),
        ],
        ignore_index=True,
    )
    amostras = amostras.loc[:, colunas_amostra] if colunas_amostra else amostras

    aplicadas = int(_bool_series(df["usar_valor_unitario_caixa_equivalente"]).sum())
    print(f"[OK] Linhas com conversao habilitada para Etapa 18: {aplicadas:,}")
    print(f"[OK] Linhas totais avaliadas: {len(df):,}")

    if exportar:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        salvar_csv(resumo_final, OUTPUT_RESUMO)
        salvar_csv(amostras, OUTPUT_AMOSTRAS)
        print(f"[OK] Resumo salvo em {OUTPUT_RESUMO.name}")
        print(f"[OK] Amostras salvas em {OUTPUT_AMOSTRAS.name}")


def exportar_dataframe(df: pd.DataFrame) -> None:
    print("\n" + "=" * 80)
    print("EXPORTANDO RESULTADO DA ETAPA 17.5")
    print("=" * 80)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    salvar_zip_csv(df, OUTPUT_ZIP, nome_csv=CSV_NAME)


def processar_conversao_unidade_caixa(
    df_entrada: pd.DataFrame | None = None,
    exportar: bool = True,
) -> pd.DataFrame:
    df_base = df_entrada if df_entrada is not None else carregar_dados()
    df_convertido = aplicar_conversao_unidade_caixa(df_base)
    gerar_resumos(df_convertido, exportar=exportar)
    if exportar:
        exportar_dataframe(df_convertido)
    else:
        print("[INFO] Exportacao desativada (modo pipeline rapido)")
    return df_convertido


def main() -> bool:
    try:
        processar_conversao_unidade_caixa()
        print("\n[SUCESSO] Etapa 17.5 concluida!")
        return True
    except Exception as exc:  # pragma: no cover
        print(f"\n[ERRO] Etapa 17.5 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
