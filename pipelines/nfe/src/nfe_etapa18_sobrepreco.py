# -*- coding: utf-8 -*-
"""ETAPA 18: ANALISE DE SOBREPRECO

Calcula a razao entre o valor unitario praticado e o teto CMED, classifica
cada transacao em faixas de preco e exporta o DataFrame enriquecido.

Input preferencial: df_etapa17_5_unidade_caixa.zip
Fallback:           df_etapa17_consolidado_final.zip
Output:            df_etapa18_sobrepreco.zip
                   df_etapa18_sobrepreco_resumo.csv
                   df_etapa18_sobrepreco_stats.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.io_utils import ler_zip_csv, salvar_csv, salvar_zip_csv
from pipelines.nfe.src.paths import DATA_DIR

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa17_5_unidade_caixa.zip"
FALLBACK_INPUT_ZIP = DATA_DIR / "processed" / "df_etapa17_consolidado_final.zip"
OUTPUT_DIR = DATA_DIR / "processed"
OUTPUT_ZIP = OUTPUT_DIR / "df_etapa18_sobrepreco.zip"
OUTPUT_RESUMO = OUTPUT_DIR / "df_etapa18_sobrepreco_resumo.csv"
OUTPUT_STATS = OUTPUT_DIR / "df_etapa18_sobrepreco_stats.csv"
CSV_NAME = "df_etapa18_sobrepreco.csv"

CLASSES_VALOR = [
    "NAO CLASSIFICADO",
    "EXTREMAMENTE ABAIXO",
    "MUITO ABAIXO",
    "DENTRO DO TETO (NORMAL)",
    "ACIMA DO TETO",
    "MUITO ACIMA",
    "EXTREMAMENTE ACIMA",
]


def carregar_dados() -> pd.DataFrame:
    """Carrega o DataFrame preferindo a Etapa 17.5, com fallback para a Etapa 17."""
    caminho = INPUT_ZIP if INPUT_ZIP.exists() else FALLBACK_INPUT_ZIP
    if not caminho.exists():
        raise FileNotFoundError(
            f"Arquivo {INPUT_ZIP.name} nao encontrado. Execute a Etapa 17.5 antes."
        )

    print("\n" + "=" * 80)
    print(f"CARREGANDO DADOS PARA ETAPA 18: {caminho.name}")
    print("=" * 80)
    return ler_zip_csv(caminho, sep=";", log_progresso=True)


def _serie_bool(serie: pd.Series) -> pd.Series:
    if serie.dtype == bool:
        return serie.fillna(False)
    return serie.astype(str).str.strip().str.upper().isin({"1", "TRUE", "SIM", "S", "YES"})


def calcular_razao(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula TETO_DE_PRECO, RAZAO_VALOR_TETO e CLASSE_VALOR."""
    df_proc = df.copy()

    print("\n" + "=" * 80)
    print("CALCULANDO RAZAO VALOR/TETO")
    print("=" * 80)

    for coluna in (
        "valor_unitario",
        "valor_unitario_caixa_equivalente",
        "PRECO_MAXIMO_REFINADO",
        "teto_caixa_equivalente",
        "confianca_conversao_unidade",
    ):
        if coluna in df_proc.columns:
            df_proc[coluna] = pd.to_numeric(df_proc[coluna], errors="coerce")
        else:
            df_proc[coluna] = pd.NA

    if "usar_valor_unitario_caixa_equivalente" in df_proc.columns:
        usar_equivalente = _serie_bool(df_proc["usar_valor_unitario_caixa_equivalente"])
    else:
        usar_equivalente = pd.Series(False, index=df_proc.index)
    usar_equivalente = usar_equivalente & df_proc["valor_unitario_caixa_equivalente"].notna()

    df_proc["VALOR_UNITARIO_ANALISE"] = df_proc["valor_unitario"]
    df_proc.loc[usar_equivalente, "VALOR_UNITARIO_ANALISE"] = df_proc.loc[
        usar_equivalente, "valor_unitario_caixa_equivalente"
    ]

    df_proc["TETO_DE_PRECO"] = df_proc["PRECO_MAXIMO_REFINADO"]
    teto_equivalente_valido = df_proc["teto_caixa_equivalente"].notna()
    df_proc.loc[teto_equivalente_valido, "TETO_DE_PRECO"] = df_proc.loc[
        teto_equivalente_valido, "teto_caixa_equivalente"
    ]
    df_proc["USOU_CONVERSAO_UNIDADE_CAIXA"] = usar_equivalente.astype(bool)

    df_proc["RAZAO_VALOR_TETO"] = np.divide(
        df_proc["VALOR_UNITARIO_ANALISE"],
        df_proc["TETO_DE_PRECO"],
    )
    df_proc["RAZAO_VALOR_TETO"] = df_proc["RAZAO_VALOR_TETO"].replace(
        [np.inf, -np.inf],
        np.nan,
    )

    condicoes = [
        df_proc["RAZAO_VALOR_TETO"].isna(),
        df_proc["RAZAO_VALOR_TETO"] < 0.02,
        (df_proc["RAZAO_VALOR_TETO"] >= 0.02) & (df_proc["RAZAO_VALOR_TETO"] < 0.1),
        (df_proc["RAZAO_VALOR_TETO"] >= 0.1) & (df_proc["RAZAO_VALOR_TETO"] <= 1.0),
        (df_proc["RAZAO_VALOR_TETO"] > 1.0) & (df_proc["RAZAO_VALOR_TETO"] <= 2.0),
        (df_proc["RAZAO_VALOR_TETO"] > 2.0) & (df_proc["RAZAO_VALOR_TETO"] <= 5.0),
        df_proc["RAZAO_VALOR_TETO"] > 5.0,
    ]
    classes = np.select(condicoes, CLASSES_VALOR, default="NAO CLASSIFICADO")
    df_proc["CLASSE_VALOR"] = pd.Categorical(classes, categories=CLASSES_VALOR, ordered=True)

    linhas_validas = df_proc["RAZAO_VALOR_TETO"].notna().sum()
    print(f"[OK] {linhas_validas:,} linhas possuem razao valida.")
    print(
        "[OK] Conversao unidade/caixa usada em "
        f"{int(usar_equivalente.sum()):,} linhas da Etapa 18."
    )

    return df_proc


def gerar_resumos(df: pd.DataFrame, exportar: bool = True) -> None:
    """Gera arquivos auxiliares com contagens e estatisticas por classe."""
    print("\n" + "=" * 80)
    print("GERANDO RESUMOS ESTATISTICOS")
    print("=" * 80)

    resumo = (
        df["CLASSE_VALOR"].value_counts(dropna=False).rename_axis("CLASSE_VALOR").reset_index(
            name="quantidade"
        )
    )
    resumo["percentual"] = (resumo["quantidade"] / len(df) * 100).round(2)

    if "USOU_CONVERSAO_UNIDADE_CAIXA" in df.columns:
        resumo_conversao = (
            df["USOU_CONVERSAO_UNIDADE_CAIXA"]
            .value_counts(dropna=False)
            .rename_axis("USOU_CONVERSAO_UNIDADE_CAIXA")
            .reset_index(name="quantidade")
        )
        resumo_conversao["percentual"] = (
            resumo_conversao["quantidade"] / len(df) * 100
        ).round(2)
    else:
        resumo_conversao = pd.DataFrame()

    if exportar:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        salvar_csv(resumo, OUTPUT_RESUMO)
        print(f"[OK] Resumo salvo em {OUTPUT_RESUMO.name}")
        if not resumo_conversao.empty:
            salvar_csv(resumo_conversao, OUTPUT_DIR / "df_etapa18_sobrepreco_resumo_conversao.csv")

    stats_cols = [
        col
        for col in [
            "RAZAO_VALOR_TETO",
            "VALOR_UNITARIO_ANALISE",
            "valor_unitario",
            "valor_unitario_caixa_equivalente",
            "TETO_DE_PRECO",
        ]
        if col in df.columns
    ]
    if stats_cols:
        stats = df.groupby("CLASSE_VALOR", observed=True)[stats_cols].describe().round(4)
        stats = stats.reset_index()
        stats.columns = ["_".join(filter(None, map(str, col))).strip("_") for col in stats.columns]
        if exportar:
            salvar_csv(stats, OUTPUT_STATS)
            print(f"[OK] Estatisticas salvas em {OUTPUT_STATS.name}")
    else:
        print("[AVISO] Colunas numericas para estatisticas nao encontradas.")


def exportar_dataframe(df: pd.DataFrame) -> None:
    """Exporta o DataFrame enriquecido usando utilitarios centralizados."""
    print("\n" + "=" * 80)
    print("EXPORTANDO RESULTADO DA ETAPA 18")
    print("=" * 80)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    salvar_zip_csv(df, OUTPUT_ZIP, nome_csv=CSV_NAME)


def processar_sobrepreco(
    df_entrada: pd.DataFrame | None = None,
    exportar: bool = True,
) -> pd.DataFrame:
    """Processa analise de sobrepreco."""
    df_base = df_entrada if df_entrada is not None else carregar_dados()
    df_enriquecido = calcular_razao(df_base)
    gerar_resumos(df_enriquecido, exportar=exportar)
    if exportar:
        exportar_dataframe(df_enriquecido)
    else:
        print("[INFO] Exportacao desativada (modo pipeline rapido)")
    return df_enriquecido


def main() -> bool:
    try:
        processar_sobrepreco()
        print("\n[SUCESSO] Etapa 18 concluida!")
        return True
    except Exception as exc:  # pragma: no cover
        print(f"\n[ERRO] Etapa 18 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
