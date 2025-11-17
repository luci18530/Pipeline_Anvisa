# -*- coding: utf-8 -*-
"""ETAPA 19: AJUSTE INFLACIONÁRIO (IGP-DI)

Atualiza os valores monetários (valor_produtos e valor_unitario) para uma data base
comum usando os fatores multiplicativos do IGP-DI.

Input:  df_etapa18_sobrepreco.zip
Output: df_etapa19_valores_ajustados.zip
        df_etapa19_resumo_ajuste.csv
"""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd

from paths import DATA_DIR, SUPPORT_DIR

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa18_sobrepreco.zip"
OUTPUT_DIR = DATA_DIR / "processed"
OUTPUT_ZIP = OUTPUT_DIR / "df_etapa19_valores_ajustados.zip"
OUTPUT_RESUMO = OUTPUT_DIR / "df_etapa19_resumo_ajuste.csv"
CSV_NAME = "df_etapa19_valores_ajustados.csv"

FACTORS_FILE = SUPPORT_DIR / "ajusteinflacionario.xlsx"
FACTORS_URL = "https://drive.google.com/uc?id=1XbGURbH4Sn3LOyC5eIy-NI7sjTApKtzt"
DEFAULT_FACTOR_COLUMN = os.environ.get("ETAPA19_FATOR_COL", "Multiplicative FactorSET25")


def carregar_dataframe() -> pd.DataFrame:
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(
            f"Arquivo {INPUT_ZIP.name} não encontrado. Execute a Etapa 18 primeiro."
        )

    print("\n" + "=" * 80)
    print("CARREGANDO DADOS DA ETAPA 18")
    print("=" * 80)

    with zipfile.ZipFile(INPUT_ZIP, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            raise ValueError("Nenhum CSV encontrado no arquivo da Etapa 18.")
        with zf.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, sep=";", low_memory=False)

    print(f"[OK] Registros carregados: {len(df):,}")
    return df


def garantir_fatores_local() -> Path:
    if FACTORS_FILE.exists():
        return FACTORS_FILE

    print("[INFO] Arquivo de fatores não encontrado. Tentando baixar via gdown...")
    try:
        import gdown  # type: ignore
    except ImportError as exc:  # pragma: no cover - feedback ao usuário
        raise RuntimeError(
            "gdown não está instalado e o arquivo de fatores não está disponível. "
            "Instale gdown ou adicione o arquivo ajusteinflacionario.xlsx em support/."
        ) from exc

    FACTORS_FILE.parent.mkdir(parents=True, exist_ok=True)
    gdown.download(FACTORS_URL, str(FACTORS_FILE), quiet=False)
    return FACTORS_FILE


def carregar_fatores() -> pd.DataFrame:
    caminho = garantir_fatores_local()
    print("\n" + "=" * 80)
    print("CARREGANDO FATORES DE INFLAÇÃO (IGP-DI)")
    print("=" * 80)

    fatores = pd.read_excel(caminho)
    fatores = fatores.rename(columns={"ano": "ano_emissao", "mes": "mes_emissao"})
    fatores["ano_emissao"] = fatores["ano_emissao"].astype(int)
    fatores["mes_emissao"] = fatores["mes_emissao"].astype(int)
    print(f"[OK] Fatores disponíveis: {len(fatores):,}")
    return fatores


def aplicar_ajuste(df: pd.DataFrame, fatores: pd.DataFrame, fator_coluna: Optional[str]) -> pd.DataFrame:
    fator_col = fator_coluna or DEFAULT_FACTOR_COLUMN
    if fator_col not in fatores.columns:
        raise KeyError(
            f"Coluna '{fator_col}' não encontrada no arquivo de fatores. "
            "Verifique o nome disponível no Excel ou defina ETAPA19_FATOR_COL."
        )

    print("\n" + "=" * 80)
    print(f"APLICANDO FATOR DE AJUSTE: {fator_col}")
    print("=" * 80)

    df_proc = df.copy()
    df_proc["data_emissao"] = pd.to_datetime(df_proc.get("data_emissao"), errors="coerce")
    if "ano_emissao" not in df_proc.columns or "mes_emissao" not in df_proc.columns:
        df_proc["ano_emissao"] = df_proc["data_emissao"].dt.year
        df_proc["mes_emissao"] = df_proc["data_emissao"].dt.month

    df_proc = df_proc.merge(
        fatores[["ano_emissao", "mes_emissao", fator_col]],
        on=["ano_emissao", "mes_emissao"],
        how="left",
    )

    df_proc[fator_col] = pd.to_numeric(df_proc[fator_col], errors="coerce").fillna(1.0)

    for coluna in ("valor_produtos", "valor_unitario"):
        df_proc[coluna] = pd.to_numeric(df_proc.get(coluna), errors="coerce")

    df_proc["valor_produtos_ajustado"] = df_proc["valor_produtos"] * df_proc[fator_col]
    df_proc["valor_unitario_ajustado"] = df_proc["valor_unitario"] * df_proc[fator_col]

    # Limpa colunas auxiliares
    colunas_para_remover = [c for c in df_proc.columns if c.startswith("Multiplicative Factor")]
    df_proc = df_proc.drop(columns=colunas_para_remover, errors="ignore")
    # Remover colunas de ano/mes e colunas redundantes
    df_proc = df_proc.drop(columns=["ano_emissao", "mes_emissao"], errors="ignore")
    # PRECO_MAXIMO_REFINADO é idêntico a TETO_DE_PRECO; remover para simplificar o dataset
    df_proc = df_proc.drop(columns=["PRECO_MAXIMO_REFINADO"], errors="ignore")

    variacao = df_proc["valor_produtos_ajustado"].sum() - df_proc["valor_produtos"].sum()
    print(f"Impacto total do ajuste: R$ {variacao:,.2f}")

    return df_proc


def exportar(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        buffer = io.StringIO()
        df.to_csv(buffer, sep=";", index=False, encoding="utf-8")
        zf.writestr(CSV_NAME, buffer.getvalue())
    print(f"[OK] Arquivo salvo: {OUTPUT_ZIP.name}")


def gerar_resumo(df: pd.DataFrame) -> None:
    total_original = pd.to_numeric(df["valor_produtos"], errors="coerce").sum()
    total_ajustado = pd.to_numeric(df["valor_produtos_ajustado"], errors="coerce").sum()
    diferenca = total_ajustado - total_original
    variacao_pct = (diferenca / total_original) * 100 if total_original else float("nan")

    resumo = pd.DataFrame(
        {
            "metricas": [
                "Soma Original",
                "Soma Ajustada",
                "Diferença Absoluta",
                "Variação Percentual",
            ],
            "valor": [
                total_original,
                total_ajustado,
                diferenca,
                variacao_pct,
            ],
        }
    )
    resumo.to_csv(OUTPUT_RESUMO, sep=";", index=False, encoding="utf-8")
    print(f"[OK] Resumo salvo: {OUTPUT_RESUMO.name}")


def main() -> bool:
    try:
        df = carregar_dataframe()
        fatores = carregar_fatores()
        df_ajustado = aplicar_ajuste(df, fatores, DEFAULT_FACTOR_COLUMN)
        exportar(df_ajustado)
        gerar_resumo(df_ajustado)
        print("\n[SUCESSO] Etapa 19 concluída!")
        return True
    except Exception as exc:  # pragma: no cover - logs de runtime
        print(f"\n[ERRO] Etapa 19 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
