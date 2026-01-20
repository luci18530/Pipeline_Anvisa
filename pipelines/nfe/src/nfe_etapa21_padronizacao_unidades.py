# -*- coding: utf-8 -*-
"""ETAPA 21: PADRONIZAÇÃO E INFERÊNCIA DE UNIDADES.

Objetivo:
    - Padronizar o campo ``unidade`` aplicando regras de negócio e heurísticas.
    - Garantir consistência para análises envolvendo quantidade e valor unitário.

Input esperado:
    data/processed/df_etapa20_classificacao_esfera.zip

Saídas:
    data/processed/df_etapa21_unidades_padronizadas.zip
    data/processed/df_etapa21_unidades_resumo.csv (top 30 unidades por fase)
    data/processed/df_etapa21_unidades_metricas.csv (estatísticas da etapa)
"""

from __future__ import annotations

import gc
import io
import os
import tempfile
import zipfile
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from paths import DATA_DIR

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa20_classificacao_esfera.zip"
OUTPUT_DIR = DATA_DIR / "processed"
OUTPUT_ZIP = OUTPUT_DIR / "df_etapa21_unidades_padronizadas.zip"
OUTPUT_RESUMO = OUTPUT_DIR / "df_etapa21_unidades_resumo.csv"
OUTPUT_METRICAS = OUTPUT_DIR / "df_etapa21_unidades_metricas.csv"
CSV_NAME = "df_etapa21_unidades_padronizadas.csv"

MAPA_UNIDADES: Dict[str, str] = {
    "CZ": "CAIXA", "CX": "CAIXA", "CX1": "CAIXA", "CX U": "CAIXA", "3/": "CAIXA", "GO": "CAIXA",
    "AERO": "CAIXA", "FRF": "CAIXA", "---": "CAIXA", "X": "CAIXA", "9": "CAIXA", "5": "CAIXA",
    "200": "CAIXA", "FARSCO": "CAIXA", "CRT": "CAIXA", "7": "CAIXA", "ND": "CAIXA", "EMB": "CAIXA",
    "PAP": "CAIXA", "25": "CAIXA", "FSC": "CAIXA", "BUND": "CAIXA", "UN/001": "CAIXA", "CPT": "CAIXA",
    "CX500": "CAIXA", "CX240": "CAIXA", "CX3": "CAIXA", "CX/": "CAIXA", "UNN": "CAIXA", "00": "CAIXA",
    "TB010": "CAIXA", "OF": "CAIXA", "VID": "CAIXA", "CXM": "CAIXA", "FMA": "CAIXA", "CX6": "CAIXA",
    "AER": "CAIXA", "MLK": "CAIXA", "FE": "CAIXA", "CDX": "CAIXA", "0X": "CAIXA", "CRE": "CAIXA",
    "SC": "CAIXA", "POM": "CAIXA", "LTO": "CAIXA", "GAR": "CAIXA", "DR": "CAIXA", "SACO": "CAIXA",
    "C": "CAIXA", "CX.": "CAIXA", "FL": "CAIXA", "F/AM": "CAIXA", "CAI": "CAIXA", "BB": "CAIXA",
    "FRT": "CAIXA", "CSS": "CAIXA", "COL": "CAIXA", "S.O": "CAIXA", "01": "CAIXA", "XP": "CAIXA",
    "3": "CAIXA", "CXD": "CAIXA", "FRAMP": "CAIXA", "UNITS": "CAIXA", "CX/16": "CAIXA", "10": "CAIXA",
    "CXVD": "CAIXA", "GR": "CAIXA", "SACH": "CAIXA", "CTL": "CAIXA", "BT": "CAIXA", "EB": "CAIXA",
    "CX 1": "CAIXA", "/": "CAIXA", "1X": "CAIXA", "PMA": "CAIXA", "PACK": "CAIXA", "CAT": "CAIXA",
    "CXP": "CAIXA", "UNID1": "CAIXA", "UNDFR": "CAIXA", "BSN": "CAIXA", "SP": "CAIXA", "CX40": "CAIXA",
    "PO": "CAIXA", "03": "CAIXA", "PTE": "CAIXA", "TAB": "CAIXA", "M": "CAIXA", "FCO": "CAIXA",
    "CX56": "CAIXA", "CX,": "CAIXA", "CT01F": "CAIXA", "VC": "CAIXA", "CT1": "CAIXA", "GOT": "CAIXA",
    "FR1": "CAIXA", "GL": "CAIXA", "FLA": "CAIXA", "INJ": "CAIXA", "CX60": "CAIXA", "CXC": "CAIXA",
    "CX120": "CAIXA", "CX14": "CAIXA", "EV": "CAIXA", "REF": "CAIXA", "UM": "CAIXA", "DO": "CAIXA",
    "BSG": "CAIXA", "ENV1": "CAIXA", "FD": "CAIXA", ",": "CAIXA", "LTS": "CAIXA", "FRASC": "CAIXA",
    "X1": "CAIXA", "CAN": "CAIXA", "PCT": "CAIXA", "CX28": "CAIXA", "SUSP": "CAIXA", "SOL": "CAIXA",
    "1ND": "CAIXA", "CXS": "CAIXA", "FR-AMP": "CAIXA", "F/": "CAIXA", "CX01": "CAIXA", "G": "CAIXA",
    "GT": "CAIXA", "XAR": "CAIXA", "SE": "CAIXA", "LITRO": "CAIXA", "BSA": "CAIXA", "CX4": "CAIXA",
    "UND.": "CAIXA", "PEC": "CAIXA", "SDF": "CAIXA", "POTE": "CAIXA", "EN": "CAIXA", "FRD": "CAIXA",
    "SPR": "CAIXA", "SUS": "CAIXA", "CAIX": "CAIXA", "CV": "CAIXA", "LTR": "CAIXA", "BO": "CAIXA",
    "CX5": "CAIXA", "FR/A": "CAIXA", "GAL": "CAIXA", "GTS": "CAIXA", "PAC": "CAIXA", "PCE": "CAIXA",
    "POT": "CAIXA", "40": "CAIXA", "CX2 UN": "CAIXA", "VDR": "CAIXA", "CX12": "CAIXA", "RL": "CAIXA",
    "KG": "CAIXA", "FC": "CAIXA", "VR": "CAIXA", "CDA": "CAIXA", "EA": "CAIXA", "CAX": "CAIXA",
    "U": "CAIXA", "MG": "CAIXA", "LT.": "CAIXA", "SER": "CAIXA", "LIT": "CAIXA", "0": "CAIXA",
    "BL": "CAIXA", "CX25": "CAIXA", "UD": "CAIXA", "CX10": "CAIXA", "BLS": "CAIXA", "BLT": "CAIXA",
    "L": "CAIXA", "FRC": "CAIXA", "CX200": "CAIXA", "CXA": "CAIXA", "CX30": "CAIXA", "CX20": "CAIXA",
    "CX50": "CAIXA", "TUB": "CAIXA", "CT": "CAIXA", "PT": "CAIXA", "CX100": "CAIXA", "PC": "CAIXA",
    "TB": "CAIXA", "UNID": "CAIXA", "UN1": "CAIXA", "LT": "CAIXA", "CX/AMP": "AMPOLA", "AMMP": "AMPOLA",
    "MP": "AMPOLA", "150": "AMPOLA", "ANP": "AMPOLA", "AMPOL": "AMPOLA", "CXX": "AMPOLA",
    "AMP.": "AMPOLA", "AP": "AMPOLA", "AMP": "AMPOLA", "AM": "AMPOLA", "39": "FRASCO",
    "XPE": "FRASCO", "UIN": "FRASCO", "FAR": "FRASCO", "FRANCO": "FRASCO", "FRAS": "FRASCO",
    "FR/AMP": "FRASCO", "FR.": "FRASCO", "F/A": "FRASCO", "FRS.": "FRASCO", "FAM": "FRASCO",
    "DS": "FRASCO", "F.A": "FRASCO", "F/A.": "FRASCO", "FAMP": "FRASCO", "F": "FRASCO",
    "VD.": "FRASCO", "FR/AM": "FRASCO", "VD": "FRASCO", "FR": "FRASCO", "FA": "FRASCO",
    "FRS": "FRASCO", "FRA": "FRASCO", "TU": "CARPULE", "TBO": "TUBOS", "TUBETE": "TUBOS",
    "SH": "ENVELOPES", "SACHE": "ENVELOPES", "SCH": "ENVELOPES", "SAC": "ENVELOPES", "FLC": "ENVELOPES",
    "BISNAG": "BISNAGA", "UN.": "BISNAGA", "GEL": "BISNAGA", "BIS.": "BISNAGA", "BISN.": "BISNAGA",
    "BS": "BISNAGA", "BI": "BISNAGA", "BISN": "BISNAGA", "BN": "BISNAGA", "BIS": "BISNAGA",
    "SERING": "SERINGAS", "SRG": "SERINGAS", "SR": "SERINGAS", "SER.": "SERINGAS", "]": "MISTO",
    "F/B": "MISTO", "CPD": "MISTO", "GE": "MISTO", "UNIDAD": "MISTO", "F A": "MISTO", "BNG": "MISTO",
    "1": "MISTO", "ML": "MISTO", "ENV": "MISTO", "UNI": "MISTO", "UN": "MISTO", "UND": "MISTO",
    "SUP": "UNIDADES", "UNDADE": "UNIDADES", "CR": "UNIDADES", "UF": "UNIDADES", "BOL": "UNIDADES",
    "BG": "UNIDADES", "EMS": "COMPRIMIDOS", "BLI": "COMPRIMIDOS", "CP REV": "COMPRIMIDOS",
    "CRP": "COMPRIMIDOS", "CCM": "COMPRIMIDOS", "VP": "COMPRIMIDOS", "CAP.": "COMPRIMIDOS",
    "CAPS.": "COMPRIMIDOS", "CAR": "COMPRIMIDOS", "UCOMP": "COMPRIMIDOS", "DRA": "COMPRIMIDOS",
    "HR": "COMPRIMIDOS", "CX-1": "COMPRIMIDOS", "MI": "COMPRIMIDOS", "CAPSUL": "COMPRIMIDOS",
    "CS": "COMPRIMIDOS", "CPM": "COMPRIMIDOS", "CA": "COMPRIMIDOS", "DRG": "COMPRIMIDOS",
    "CAPS": "COMPRIMIDOS", "COMP.": "COMPRIMIDOS", "CPR.": "COMPRIMIDOS", "CM": "COMPRIMIDOS",
    "CPS": "COMPRIMIDOS", "CO": "COMPRIMIDOS", "CAP": "COMPRIMIDOS", "UNB": "COMPRIMIDOS",
    "CPREV": "COMPRIMIDOS", "CMP": "COMPRIMIDOS", "CPR": "COMPRIMIDOS", "CP": "COMPRIMIDOS",
    "COMP": "COMPRIMIDOS", "COM": "COMPRIMIDOS", "TUBO": "MISTO", "KIT": "MISTO", "SERINGAS": "MISTO",
    "TUBOS": "MISTO", "POMADA": "BISNAGA", "ABL": "COMPRIMIDOS", "CARPULE": "UNIDADES",
    "BOLSA": "UNIDADES", "ENVELOPES": "UNIDADES", "LATA": "CAIXA", "GA": "CAIXA", "VL": "CAIXA",
    "GOTAS": "CAIXA", "CH": "CAIXA", "1 KG": "CAIXA", "LA": "CAIXA", "BID": "CAIXA", "SU": "FRASCO",
    "UN0001": "CAIXA", "LAT": "CAIXA", "PA": "COMPRIMIDOS", "UNT": "CAIXA", "CD": "CAIXA",
    "CX300": "CAIXA", "DP": "CAIXA", "CX1000": "CAIXA", "UNS": "UNIDADE", "CX/70": "CAIXA",
    "ENV.": "ENVELOPES", "11": "UNIDADE", "UN0": "UNIDADE", "IN": "CAIXA", "CAIXAS": "CAIXA",
    "100": "CAIXA", "FRACO": "FRASCO", "METRO": "UNIDADE", "UNIDADE": "UNIDADES", "RACK": "DELETAR",
    "KI": "DELETAR", "PL": "DELETAR", "1000UN": "DELETAR", "CJ": "DELETAR", "PACOT": "DELETAR",
    "DGR": "COMPRIMIDOS", "PAS": "COMPRIMIDOS",
}

UNIDADES_PARA_REMOVER = ["BD38", "DELETAR"]

MAPA_CONSOLIDACAO_UNIDADES: Dict[str, str] = {
    "COMPRIMIDOS": "UNIDADES",
    "BISNAGA": "UNIDADES",
    "AMPOLA": "UNIDADES",
    "FRASCO": "UNIDADES",
    "ENVELOPES": "UNIDADES",
    "SERINGAS": "UNIDADES",
    "TUBOS": "UNIDADES",
    "CARPULE": "UNIDADES",
    "UNIDADE": "UNIDADES",
}

MAPA_FINAL: Dict[str, str] = {"MIL": "CAIXA"}


def carregar_dataframe() -> pd.DataFrame:
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(
            f"Arquivo {INPUT_ZIP.name} não encontrado. Execute a Etapa 20 antes."
        )

    print("\n" + "=" * 80)
    print("CARREGANDO DADOS DA ETAPA 20 - MODO CHUNKED")
    print("=" * 80)

    with zipfile.ZipFile(INPUT_ZIP, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            raise ValueError("Nenhum CSV encontrado dentro do pacote da Etapa 20.")
        
        # Extrair para arquivo temporário e ler em chunks
        tmp_fd, tmp_path = tempfile.mkstemp(suffix='.csv', text=False)
        try:
            with os.fdopen(tmp_fd, 'wb') as tmp_file:
                with zf.open(csv_name) as csv_source:
                    tmp_file.write(csv_source.read())
            
            # Ler em chunks para evitar MemoryError
            chunks = []
            chunk_size = 100_000
            for i, chunk in enumerate(pd.read_csv(tmp_path, sep=";", low_memory=False, chunksize=chunk_size)):
                chunks.append(chunk)
                if (i + 1) % 10 == 0:
                    print(f"[INFO] Carregados {(i + 1) * chunk_size:,} registros...")
            
            df = pd.concat(chunks, ignore_index=True)
        finally:
            try:
                os.unlink(tmp_path)
            except:
                pass

    print(f"[OK] Registros carregados: {len(df):,}")
    return df


def preparar_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    # Modificar in-place para evitar MemoryError
    for coluna in ("valor_produtos", "valor_unitario", "quantidade"):
        df[coluna] = pd.to_numeric(df.get(coluna), errors="coerce")

    unidade_col = df.get("unidade")
    if unidade_col is None:
        df["unidade"] = ""
    else:
        df["unidade"] = unidade_col.astype(str).str.strip().str.upper()

    contagem = df["unidade"].value_counts(dropna=False)
    return df, contagem


def aplicar_correcao_unidade_180(df: pd.DataFrame) -> pd.DataFrame:
    # Processar in-place para economizar memória
    mask_180 = df["unidade"] == "180"
    if mask_180.any():
        print("Aplicando correção especial para unidade '180'...")
        fator_conversao = 60.0
        df.loc[mask_180, "quantidade"] = df.loc[mask_180, "quantidade"] / fator_conversao
        denominador = df.loc[mask_180, "quantidade"].replace(0, np.nan)
        df.loc[mask_180, "valor_unitario"] = (
            df.loc[mask_180, "valor_produtos"] / denominador
        )
        df.loc[mask_180, "unidade"] = "CAIXA"
    return df


def recalcular_valor_unitario_caixa(df: pd.DataFrame) -> pd.DataFrame:
    # Processar in-place para economizar memória
    mask_caixa = df["unidade"] == "CAIXA"
    if mask_caixa.any():
        print("Recalculando valor_unitario para registros com unidade 'CAIXA'...")
        denominador = df.loc[mask_caixa, "quantidade"].replace(0, np.nan)
        df.loc[mask_caixa, "valor_unitario"] = (
            df.loc[mask_caixa, "valor_produtos"] / denominador
        )
    return df


def padronizar_unidades(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    # Processar in-place para economizar memória
    df["unidade_padronizada"] = df["unidade"].map(MAPA_UNIDADES).fillna(df["unidade"])
    linhas_antes = len(df)
    # Filtrar sem criar cópia
    mask_manter = ~df["unidade_padronizada"].isin(UNIDADES_PARA_REMOVER)
    df = df[mask_manter]
    removidas = linhas_antes - len(df)
    df["unidade"] = df["unidade_padronizada"]
    df.drop(columns=["unidade_padronizada"], inplace=True)
    return df, removidas


def aplicar_heuristicas(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    # Processar in-place para economizar memória
    df["unidade"] = df["unidade"].map(MAPA_CONSOLIDACAO_UNIDADES).fillna(df["unidade"])

    quantidade = df["quantidade"].clip(lower=1e-6).fillna(1.0)
    valor_unitario = df["valor_unitario"].clip(lower=1e-6).fillna(1.0)
    score = 2 * (np.log10(quantidade) - np.log10(valor_unitario))
    score = score.replace([np.inf, -np.inf], 0.0)
    df["score"] = score.fillna(0.0)

    # Copiar apenas a série (muito menor que o DataFrame completo)
    unidades_originais = df["unidade"].copy()

    mask_ambigua_caixa = df["unidade"].isin(["MISTO", "CAIXA"])
    df.loc[mask_ambigua_caixa & (df["valor_unitario"] < 0.7), "unidade"] = "UNIDADES"
    df.loc[mask_ambigua_caixa & (df["score"] > 2), "unidade"] = "UNIDADES"
    df.loc[mask_ambigua_caixa & (df["quantidade"] > 13333), "unidade"] = "UNIDADES"
    df.loc[
        mask_ambigua_caixa & (df["valor_unitario"] < 5) & (df["quantidade"] >= 3500),
        "unidade",
    ] = "UNIDADES"
    df.loc[
        mask_ambigua_caixa & (df["valor_unitario"] < 4) & (df["quantidade"] >= 2600),
        "unidade",
    ] = "UNIDADES"
    df.loc[
        mask_ambigua_caixa & (df["valor_unitario"] < 3) & (df["quantidade"] >= 1900),
        "unidade",
    ] = "UNIDADES"
    df.loc[
        mask_ambigua_caixa & (df["valor_unitario"] < 2) & (df["quantidade"] >= 200),
        "unidade",
    ] = "UNIDADES"

    mask_ambigua_unidade = df["unidade"].isin(["MISTO", "UNIDADES"])
    df.loc[mask_ambigua_unidade & (df["score"] < 0.33), "unidade"] = "CAIXA"
    df.loc[mask_ambigua_unidade & (df["quantidade"] <= 3), "unidade"] = "CAIXA"
    df.loc[mask_ambigua_unidade & (df["valor_unitario"] > 1500), "unidade"] = "CAIXA"
    df.loc[
        mask_ambigua_unidade & (df["quantidade"] <= 4) & (df["valor_unitario"] > 3),
        "unidade",
    ] = "CAIXA"
    df.loc[
        mask_ambigua_unidade & (df["quantidade"] <= 5) & (df["valor_unitario"] > 5),
        "unidade",
    ] = "CAIXA"
    df.loc[
        mask_ambigua_unidade & (df["quantidade"] <= 6) & (df["valor_unitario"] > 10),
        "unidade",
    ] = "CAIXA"
    df.loc[
        mask_ambigua_unidade & (df["quantidade"] <= 7) & (df["valor_unitario"] > 30),
        "unidade",
    ] = "CAIXA"
    df.loc[
        mask_ambigua_unidade & (df["quantidade"] <= 8) & (df["valor_unitario"] > 50),
        "unidade",
    ] = "CAIXA"
    df.loc[
        mask_ambigua_unidade & (df["quantidade"] <= 9) & (df["valor_unitario"] > 75),
        "unidade",
    ] = "CAIXA"

    df.loc[df["score"] <= 1, "unidade"] = "CAIXA"
    df.loc[df["score"] > 2, "unidade"] = "UNIDADES"
    df.loc[df["quantidade"] <= 3, "unidade"] = "CAIXA"
    df.loc[df["unidade"].isin(["MISTO"]) & (df["score"] <= 2), "unidade"] = "CAIXA"

    df["unidade"] = df["unidade"].map(MAPA_FINAL).fillna(df["unidade"])

    mudancas = (unidades_originais != df["unidade"]).sum()
    df.drop(columns=["score"], inplace=True)
    return df, mudancas


def exportar_dataframe(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Usar arquivo temporário para evitar MemoryError
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp_file:
        tmp_path = tmp_file.name
        print("[INFO] Salvando CSV temporário...")
        df.to_csv(tmp_file, sep=";", index=False)
    
    try:
        print("[INFO] Comprimindo arquivo...")
        with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(tmp_path, CSV_NAME)
    finally:
        os.unlink(tmp_path)
    
    print(f"[OK] Arquivo salvo: {OUTPUT_ZIP.name}")


def _series_para_resumo(nome: str, serie: pd.Series) -> pd.DataFrame:
    if serie.empty:
        return pd.DataFrame({"etapa": [nome], "unidade": ["-"], "registros": [0]})
    top = (
        serie.head(30)
        .rename("registros")
        .reset_index()
        .rename(columns={"index": "unidade"})
    )
    top["etapa"] = nome
    return top[["etapa", "unidade", "registros"]]


def gerar_resumos(
    contagem_inicial: pd.Series,
    contagem_padronizada: pd.Series,
    contagem_final: pd.Series,
    removidas: int,
    mudancas: int,
) -> None:
    resumo = pd.concat(
        [
            _series_para_resumo("antes_padronizacao", contagem_inicial),
            _series_para_resumo("apos_mapeamento", contagem_padronizada),
            _series_para_resumo("apos_heuristica", contagem_final),
        ],
        ignore_index=True,
    )
    resumo.to_csv(OUTPUT_RESUMO, sep=";", index=False, encoding="utf-8")
    print(f"[OK] Resumo salvo: {OUTPUT_RESUMO.name}")

    metricas = pd.DataFrame(
        {
            "metricas": [
                "linhas_removidas_mapeamento",
                "linhas_alteradas_heuristicas",
            ],
            "valor": [removidas, mudancas],
        }
    )
    metricas.to_csv(OUTPUT_METRICAS, sep=";", index=False, encoding="utf-8")
    print(f"[OK] Métricas salvas: {OUTPUT_METRICAS.name}")


def main() -> bool:
    try:
        for nome in ("df_unidade", "df_heuristica", "df_final_unidade", "df_final_esfera"):
            if nome in globals():
                del globals()[nome]
        gc.collect()

        df = carregar_dataframe()
        df_prep, contagem_inicial = preparar_dataframe(df)
        df_corrigido = aplicar_correcao_unidade_180(df_prep)
        df_corrigido = recalcular_valor_unitario_caixa(df_corrigido)
        df_padronizado, removidas = padronizar_unidades(df_corrigido)
        contagem_padronizada = df_padronizado["unidade"].value_counts()

        df_final, mudancas = aplicar_heuristicas(df_padronizado)
        contagem_final = df_final["unidade"].value_counts()

        exportar_dataframe(df_final)
        gerar_resumos(contagem_inicial, contagem_padronizada, contagem_final, removidas, mudancas)

        print("\n[SUCESSO] Etapa 21 concluída!")
        return True
    except Exception as exc:  # pragma: no cover
        print(f"\n[ERRO] Etapa 21 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
