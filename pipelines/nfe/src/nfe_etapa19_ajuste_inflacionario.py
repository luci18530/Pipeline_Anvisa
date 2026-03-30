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
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd

from pipelines.nfe.src.paths import DATA_DIR, SUPPORT_DIR

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa18_sobrepreco.zip"
OUTPUT_DIR = DATA_DIR / "processed"
OUTPUT_ZIP = OUTPUT_DIR / "df_etapa19_valores_ajustados.zip"
OUTPUT_RESUMO = OUTPUT_DIR / "df_etapa19_resumo_ajuste.csv"
CSV_NAME = "df_etapa19_valores_ajustados.csv"

FACTORS_FILE = SUPPORT_DIR / "ajusteinflacionario.xlsx"
DEFAULT_FACTOR_COLUMN = os.environ.get("ETAPA19_FATOR_COL")
FACTOR_COLUMN_FALLBACKS = (
    "Fator",
    "Multiplicative FactorSET25",
    "Multiplicative FactorAUG25",
    "Multiplicative FactorDEZ24",
)


def carregar_dataframe() -> pd.DataFrame:
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(
            f"Arquivo {INPUT_ZIP.name} não encontrado. Execute a Etapa 18 primeiro."
        )

    print("\n" + "=" * 80)
    print("CARREGANDO DADOS DA ETAPA 18 - MODO CHUNKED")
    print("=" * 80)

    with zipfile.ZipFile(INPUT_ZIP, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            raise ValueError("Nenhum CSV encontrado no arquivo da Etapa 18.")
        
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


def garantir_fatores_local() -> Path:
    if FACTORS_FILE.exists():
        return FACTORS_FILE

    raise FileNotFoundError(
        f"Arquivo de fatores não encontrado: {FACTORS_FILE}. "
        "Adicione o arquivo ajusteinflacionario.xlsx em pipelines/nfe/support/."
    )


def carregar_fatores() -> pd.DataFrame:
    caminho = garantir_fatores_local()
    print("\n" + "=" * 80)
    print("CARREGANDO FATORES DE INFLAÇÃO (IGP-DI)")
    print("=" * 80)

    fatores = pd.read_excel(caminho)

    rename_map = {}
    for col in fatores.columns:
        nome = str(col).strip().lower()
        if nome in {"ano", "ano_emissao"}:
            rename_map[col] = "ano_emissao"
        elif nome in {"mes", "mês", "mes_emissao", "mês_emissao"}:
            rename_map[col] = "mes_emissao"
    fatores = fatores.rename(columns=rename_map)

    faltantes = sorted({"ano_emissao", "mes_emissao"} - set(fatores.columns))
    if faltantes:
        raise KeyError(
            "Arquivo de fatores sem colunas obrigatorias de data. "
            f"Faltando: {faltantes}. Colunas disponiveis: {list(fatores.columns)}"
        )

    fatores["ano_emissao"] = fatores["ano_emissao"].astype(int)
    fatores["mes_emissao"] = fatores["mes_emissao"].astype(int)
    print(f"[OK] Fatores disponíveis: {len(fatores):,}")
    return fatores


def resolver_coluna_fator(fatores: pd.DataFrame, fator_coluna: Optional[str]) -> str:
    candidatos = []
    if fator_coluna:
        candidatos.append(fator_coluna)
    if DEFAULT_FACTOR_COLUMN and DEFAULT_FACTOR_COLUMN not in candidatos:
        candidatos.append(DEFAULT_FACTOR_COLUMN)
    for fallback in FACTOR_COLUMN_FALLBACKS:
        if fallback not in candidatos:
            candidatos.append(fallback)

    for coluna in candidatos:
        if coluna in fatores.columns:
            return coluna

    colunas_data = {"ano_emissao", "mes_emissao", "ano", "mes", "mês"}
    colunas_candidatas = [c for c in fatores.columns if c not in colunas_data]
    if len(colunas_candidatas) == 1:
        return colunas_candidatas[0]

    raise KeyError(
        "Nenhuma coluna de fator encontrada no arquivo de fatores. "
        f"Colunas disponiveis: {list(fatores.columns)}. "
        "Defina ETAPA19_FATOR_COL para indicar explicitamente a coluna correta."
    )


def converter_fator_numerico(serie: pd.Series) -> pd.Series:
    direto = pd.to_numeric(serie, errors="coerce")
    texto_com_ponto = pd.to_numeric(
        serie.astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    return texto_com_ponto if texto_com_ponto.notna().sum() > direto.notna().sum() else direto


def aplicar_ajuste(df: pd.DataFrame, fatores: pd.DataFrame, fator_coluna: Optional[str]) -> pd.DataFrame:
    fator_col = resolver_coluna_fator(fatores, fator_coluna)
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

    df_proc[fator_col] = converter_fator_numerico(df_proc[fator_col]).fillna(1.0)

    for coluna in ("valor_produtos", "valor_unitario"):
        df_proc[coluna] = pd.to_numeric(df_proc.get(coluna), errors="coerce")

    df_proc["valor_produtos_ajustado"] = df_proc["valor_produtos"] * df_proc[fator_col]
    df_proc["valor_unitario_ajustado"] = df_proc["valor_unitario"] * df_proc[fator_col]

    # Limpa colunas auxiliares
    colunas_para_remover = [c for c in df_proc.columns if c.startswith("Multiplicative Factor")]
    if fator_col in df_proc.columns:
        colunas_para_remover.append(fator_col)
    df_proc = df_proc.drop(columns=colunas_para_remover, errors="ignore")
    # Remover colunas de ano/mes e colunas redundantes
    df_proc = df_proc.drop(columns=["ano_emissao", "mes_emissao"], errors="ignore")
    # PRECO_MAXIMO_REFINADO é idêntico a TETO_DE_PRECO; remover para simplificar o dataset
    df_proc = df_proc.drop(columns=["PRECO_MAXIMO_REFINADO"], errors="ignore")

    variacao = df_proc["valor_produtos_ajustado"].sum() - df_proc["valor_produtos"].sum()
    print(f"Impacto total do ajuste: R$ {variacao:,.2f}")

    return df_proc


def exportar_resultado(df: pd.DataFrame) -> None:
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


def processar_ajuste_inflacionario(
    df_entrada: pd.DataFrame | None = None,
    exportar: bool = True,
    fator_coluna: Optional[str] = None,
) -> pd.DataFrame:
    df_base = df_entrada if df_entrada is not None else carregar_dataframe()
    fatores = carregar_fatores()
    df_ajustado = aplicar_ajuste(df_base, fatores, fator_coluna)
    if exportar:
        exportar_resultado(df_ajustado)
        gerar_resumo(df_ajustado)
    return df_ajustado


def main() -> bool:
    try:
        processar_ajuste_inflacionario()
        print("\n[SUCESSO] Etapa 19 concluída!")
        return True
    except Exception as exc:  # pragma: no cover - logs de runtime
        print(f"\n[ERRO] Etapa 19 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)


