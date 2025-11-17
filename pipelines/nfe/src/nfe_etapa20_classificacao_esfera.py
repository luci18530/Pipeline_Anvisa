# -*- coding: utf-8 -*-
"""ETAPA 20: CLASSIFICAÇÃO POR ESFERA ADMINISTRATIVA

Cruza os destinatários com uma base de CNPJs para atribuir a esfera administrativa
(1 = Municipal, 2 = Estadual) e aplica regras de negócio para ajustes manuais.

Input:  df_etapa19_valores_ajustados.zip
Output: df_etapa20_classificacao_esfera.zip
        df_etapa20_distribuicao_esfera.csv
"""

from __future__ import annotations

import gc
import io
import zipfile

import numpy as np
import pandas as pd

from paths import DATA_DIR, SUPPORT_DIR

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa19_valores_ajustados.zip"
OUTPUT_DIR = DATA_DIR / "processed"
OUTPUT_ZIP = OUTPUT_DIR / "df_etapa20_classificacao_esfera.zip"
OUTPUT_RESUMO = OUTPUT_DIR / "df_etapa20_distribuicao_esfera.csv"
CSV_NAME = "df_etapa20_classificacao_esfera.csv"

ESFERA_FILE = SUPPORT_DIR / "classificacao_esfera.csv"
ESFERA_URL = "https://drive.google.com/uc?id=11mCabQH1SXvdg4p5hW8q9QeZpRYQN-ic"

EXCLUIR_NOME_FANTASIA = {
    "HOSPITAL DE GUARNICAO DE JOAO PESSOA",
    "BASE ADMINISTRATIVA DA GUARNICAO DE JOAO PESSOA",
}
EXCLUIR_RAZAO_SOCIAL = {
    "FUNDACAO PARQUE TECNOLOGICO DA PARAIBA",
    "INSTITUTO DOS CEGOS DA PARAIBA ADALGISA CUNHA",
    "ESPACO CIDADANIA E OPORTUNIDADES SOCIAIS",
}
ATUALIZAR_PARA_MUNICIPAL = {
    "nome_fantasia_destinatario": {
        "FARMADANTAS",
        "INTERVENCAO PUBLICA",
        "PREFEITURA MUNICIPAL DE TRIUNFO",
    },
    "razao_social_destinatario": {
        "INSTITUTO ACQUA - ACAO, CIDADANIA, QUALIDADE URBANA E AMBIENTAL",
        "MUNICIPIO DE QUEIMADAS",
        "MUNICIPIO DE SANTA LUZIA",
        "FUNDO MUNICIPAL DE SAUDE",
        "HELIOSMAN BIDO DA COSTA",
        "MARIA JOSENE DE ARRUDA ANDRADE",
        "DIAS COMERCIO DE PRODUTOS FARMACEUTICOS LTDA",
        "F. ECONOMICA LTDA",
        "VIDA NATURALIS COMERCIO ATACADISTA LTDA",
        "ENEIDE ALVARENGA TERTO VIEIRA RAMALHO",
        "PAULO DOUGLAS DE AZEVEDO TEOTONIO LTDA",
        "MARIA JOSE DE ARAUJO SILVA CUNHA",
        "CONGREGACAO DAS IRMAS DOS POBRES DE SANTA CATARINA DE SENA - PROVINCIA SAGRADO CORACAO DE JESUS",
    },
}
ATUALIZAR_PARA_ESTADUAL = {
    "razao_social_destinatario": {
        "CRUZ VERMELHA BRASILEIRA FILIAL DO ESTADO DO RIO GRANDE DO SUL",
    }
}


def _normalize(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def carregar_dataframe() -> pd.DataFrame:
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(
            f"Arquivo {INPUT_ZIP.name} não encontrado. Execute a Etapa 19 antes."
        )

    print("\n" + "=" * 80)
    print("CARREGANDO DADOS AJUSTADOS (ETAPA 19)")
    print("=" * 80)

    with zipfile.ZipFile(INPUT_ZIP, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            raise ValueError("Nenhum CSV encontrado no arquivo da Etapa 19.")
        with zf.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, sep=";", low_memory=False)

    print(f"[OK] Registros carregados: {len(df):,}")
    return df


def garantir_base_esfera() -> pd.DataFrame:
    if not ESFERA_FILE.exists():
        try:
            import gdown  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "Arquivo classificacao_esfera.csv não encontrado e gdown não está instalado. "
                "Instale gdown ou adicione o arquivo manualmente em support/."
            ) from exc

        ESFERA_FILE.parent.mkdir(parents=True, exist_ok=True)
        gdown.download(ESFERA_URL, str(ESFERA_FILE), quiet=False)

    df_esfera = pd.read_csv(ESFERA_FILE, sep=";", dtype=str)
    expected = {"CNPJ", "ID_ESFERA"}
    if not expected.issubset(df_esfera.columns):
        raise ValueError(
            "Arquivo de esferas inválido. Deve conter as colunas 'CNPJ' e 'ID_ESFERA'."
        )
    return df_esfera


def classificar(df: pd.DataFrame, tabela_esfera: pd.DataFrame) -> pd.DataFrame:
    df_proc = df.copy()
    for coluna in ("cpf_cnpj", "nome_fantasia_destinatario", "razao_social_destinatario"):
        if coluna not in df_proc.columns:
            df_proc[coluna] = pd.NA

    df_proc["cpf_cnpj_limpo"] = _normalize(df_proc["cpf_cnpj"]).str.replace(r"\D", "", regex=True).str.zfill(14)
    tabela_esfera = tabela_esfera.copy()
    tabela_esfera["CNPJ_limpo"] = _normalize(tabela_esfera["CNPJ"]).str.replace(r"\D", "", regex=True).str.zfill(14)

    df_proc = df_proc.merge(
        tabela_esfera[["CNPJ_limpo", "ID_ESFERA"]],
        left_on="cpf_cnpj_limpo",
        right_on="CNPJ_limpo",
        how="left",
    )
    df_proc.drop(columns=["cpf_cnpj_limpo", "CNPJ_limpo"], inplace=True, errors="ignore")

    print("\n--- Aplicando filtros de exclusão ---")
    antes = len(df_proc)
    mask_nf = ~_normalize(df_proc["nome_fantasia_destinatario"]).isin(EXCLUIR_NOME_FANTASIA)
    mask_rs = ~_normalize(df_proc["razao_social_destinatario"]).isin(EXCLUIR_RAZAO_SOCIAL)
    df_proc = df_proc[mask_nf & mask_rs]
    depois = len(df_proc)
    print(f"Registros removidos: {antes - depois:,}")

    print("\n--- Aplicando atualizações manuais ---")
    for coluna, nomes in ATUALIZAR_PARA_MUNICIPAL.items():
        normalized = _normalize(df_proc[coluna])
        df_proc.loc[normalized.isin(nomes), "ID_ESFERA"] = 1
    for coluna, nomes in ATUALIZAR_PARA_ESTADUAL.items():
        normalized = _normalize(df_proc[coluna])
        df_proc.loc[normalized.isin(nomes), "ID_ESFERA"] = 2

    df_proc["ID_ESFERA"] = pd.to_numeric(df_proc["ID_ESFERA"], errors="coerce")
    df_proc["ID_ESFERA"].fillna(1, inplace=True)
    df_proc["ID_ESFERA"] = df_proc["ID_ESFERA"].astype("Int64")

    return df_proc


def exportar(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        buffer = io.StringIO()
        df.to_csv(buffer, sep=";", index=False, encoding="utf-8")
        zf.writestr(CSV_NAME, buffer.getvalue())
    print(f"[OK] Arquivo salvo: {OUTPUT_ZIP.name}")


def gerar_resumo(df: pd.DataFrame) -> None:
    resumo = (
        df["ID_ESFERA"].value_counts(dropna=False)
        .rename_axis("ID_ESFERA")
        .reset_index(name="quantidade")
    )
    resumo["percentual"] = (resumo["quantidade"] / len(df) * 100).round(2)
    resumo.to_csv(OUTPUT_RESUMO, sep=";", index=False, encoding="utf-8")
    print(f"[OK] Distribuição salva: {OUTPUT_RESUMO.name}")


def main() -> bool:
    try:
        for nome in ("df_analise", "df_merged"):
            if nome in globals():
                del globals()[nome]
        gc.collect()

        df = carregar_dataframe()
        tabela = garantir_base_esfera()
        df_esfera = classificar(df, tabela)
        exportar(df_esfera)
        gerar_resumo(df_esfera)
        print("\n[SUCESSO] Etapa 20 concluída!")
        return True
    except Exception as exc:  # pragma: no cover
        print(f"\n[ERRO] Etapa 20 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
