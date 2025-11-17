# -*- coding: utf-8 -*-
"""ETAPA 18: ANÁLISE DE SOBREPREÇO

Calcula a razão entre o valor unitário praticado e o teto (PRECO_MAXIMO_REFINADO),
classifica cada transação em faixas de preço e exporta o DataFrame enriquecido.

Input:  df_etapa17_consolidado_final.zip
Output: df_etapa18_sobrepreco.zip
        df_etapa18_sobrepreco_resumo.csv (contagens por classe)
        df_etapa18_sobrepreco_stats.csv (estatísticas por classe)
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from paths import DATA_DIR

# Caminhos
INPUT_ZIP = DATA_DIR / "processed" / "df_etapa17_consolidado_final.zip"
OUTPUT_DIR = DATA_DIR / "processed"
OUTPUT_ZIP = OUTPUT_DIR / "df_etapa18_sobrepreco.zip"
OUTPUT_RESUMO = OUTPUT_DIR / "df_etapa18_sobrepreco_resumo.csv"
OUTPUT_STATS = OUTPUT_DIR / "df_etapa18_sobrepreco_stats.csv"
CSV_NAME = "df_etapa18_sobrepreco.csv"

# Classes ordenadas para facilitar análises posteriores
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
    """Carrega o DataFrame consolidado da etapa 17."""
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(
            f"Arquivo {INPUT_ZIP.name} não encontrado. Execute a Etapa 17 antes."
        )

    print("\n" + "=" * 80)
    print("CARREGANDO DADOS DA ETAPA 17 (CONSOLIDADO)")
    print("=" * 80)

    with zipfile.ZipFile(INPUT_ZIP, "r") as zf:
        csv_name = next(
            (name for name in zf.namelist() if name.lower().endswith(".csv")), None
        )
        if not csv_name:
            raise ValueError("Nenhum CSV encontrado no arquivo consolidado.")

        with zf.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, sep=";", low_memory=False)

    print(f"[OK] Registros carregados: {len(df):,}")
    return df


def calcular_razao(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula TETO_DE_PRECO, RAZAO_VALOR_TETO e CLASSE_VALOR."""
    df_proc = df.copy()

    print("\n" + "=" * 80)
    print("CALCULANDO RAZÃO VALOR/TETO")
    print("=" * 80)

    # Garantir colunas numéricas
    for coluna in ("valor_unitario", "PRECO_MAXIMO_REFINADO"):
        if coluna in df_proc.columns:
            df_proc[coluna] = pd.to_numeric(df_proc[coluna], errors="coerce")
        else:
            df_proc[coluna] = pd.NA

    df_proc["TETO_DE_PRECO"] = df_proc["PRECO_MAXIMO_REFINADO"]

    df_proc["RAZAO_VALOR_TETO"] = np.divide(
        df_proc["valor_unitario"],
        df_proc["TETO_DE_PRECO"],
    )
    df_proc["RAZAO_VALOR_TETO"] = df_proc["RAZAO_VALOR_TETO"].replace([np.inf, -np.inf], np.nan)

    condicoes = [
        df_proc["RAZAO_VALOR_TETO"].isna(),
        df_proc["RAZAO_VALOR_TETO"] < 0.02,
        (df_proc["RAZAO_VALOR_TETO"] >= 0.02)
        & (df_proc["RAZAO_VALOR_TETO"] < 0.1),
        (df_proc["RAZAO_VALOR_TETO"] >= 0.1)
        & (df_proc["RAZAO_VALOR_TETO"] <= 1.0),
        (df_proc["RAZAO_VALOR_TETO"] > 1.0)
        & (df_proc["RAZAO_VALOR_TETO"] <= 2.0),
        (df_proc["RAZAO_VALOR_TETO"] > 2.0)
        & (df_proc["RAZAO_VALOR_TETO"] <= 5.0),
        df_proc["RAZAO_VALOR_TETO"] > 5.0,
    ]
    classes = np.select(condicoes, CLASSES_VALOR, default="NAO CLASSIFICADO")
    df_proc["CLASSE_VALOR"] = pd.Categorical(classes, categories=CLASSES_VALOR, ordered=True)

    linhas_validas = df_proc["RAZAO_VALOR_TETO"].notna().sum()
    print(f"[OK] {linhas_validas:,} linhas possuem razão válida.")

    return df_proc


def gerar_resumos(df: pd.DataFrame) -> None:
    """Gera arquivos auxiliares com contagens e estatísticas por classe."""
    print("\n" + "=" * 80)
    print("GERANDO RESUMOS ESTATÍSTICOS")
    print("=" * 80)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Contagem e percentual por classe
    resumo = (
        df["CLASSE_VALOR"].value_counts(dropna=False)
        .rename_axis("CLASSE_VALOR")
        .reset_index(name="quantidade")
    )
    resumo["percentual"] = (resumo["quantidade"] / len(df) * 100).round(2)
    resumo.to_csv(OUTPUT_RESUMO, sep=";", index=False, encoding="utf-8")
    print(f"[OK] Resumo salvo em {OUTPUT_RESUMO.name}")

    # Estatísticas por classe
    stats_cols = [col for col in ["RAZAO_VALOR_TETO", "valor_unitario", "TETO_DE_PRECO"] if col in df.columns]
    if stats_cols:
        stats = (
            df.groupby("CLASSE_VALOR", observed=True)[stats_cols]
            .describe()
            .round(4)
        )
        stats = stats.reset_index()
        stats.columns = ["_".join(filter(None, map(str, col))).strip("_") for col in stats.columns]
        stats.to_csv(OUTPUT_STATS, sep=";", index=False, encoding="utf-8")
        print(f"[OK] Estatísticas salvas em {OUTPUT_STATS.name}")
    else:
        print("[AVISO] Colunas numéricas para estatísticas não encontradas.")


def exportar_dataframe(df: pd.DataFrame) -> None:
    """Exporta o DataFrame enriquecido."""
    print("\n" + "=" * 80)
    print("EXPORTANDO RESULTADO DA ETAPA 18")
    print("=" * 80)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        buffer = io.StringIO()
        df.to_csv(buffer, sep=";", index=False, encoding="utf-8")
        zf.writestr(CSV_NAME, buffer.getvalue())

    tamanho_zip = OUTPUT_ZIP.stat().st_size / (1024 * 1024)
    print(f"[OK] Arquivo salvo: {OUTPUT_ZIP.name} ({tamanho_zip:.2f} MB)")


def main() -> bool:
    try:
        df = carregar_dados()
        df_enriquecido = calcular_razao(df)
        gerar_resumos(df_enriquecido)
        exportar_dataframe(df_enriquecido)
        print("\n[SUCESSO] Etapa 18 concluída!")
        return True
    except Exception as exc:  # pragma: no cover - logging informativo
        print(f"\n[ERRO] Etapa 18 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
