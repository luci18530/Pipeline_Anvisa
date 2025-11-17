# -*- coding: utf-8 -*-
"""ETAPA 22: PARTICIONAMENTO DE TABELAS PARA QLIKVIEW.

Gera tabelas auxiliares consumidas pelo QlikView e move o arquivo
``nfe_vencimento.csv`` para a mesma pasta, concatenando com conteúdo prévio
quando existir.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from paths import DATA_DIR, PROJECT_ROOT

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa21_unidades_padronizadas.zip"
QLIKVIEW_DIR = PROJECT_ROOT / "QlikView"
CENTRAL_CSV = QLIKVIEW_DIR / "df_central.csv"
VENCIMENTO_ORIGEM = DATA_DIR / "external" / "nfe_vencimento.csv"
VENCIMENTO_DESTINO = QLIKVIEW_DIR / "nfe_vencimento.csv"

TABELAS_A_CRIAR: Dict[str, List[str]] = {
    "df_dosagem.csv": ["QUANTIDADE MG", "QUANTIDADE ML", "QUANTIDADE UI"],
    "df_registro_anvisa.csv": ["REGISTRO"],
    "df_entidades.csv": [
        "cpf_cnpj",
        "razao_social_destinatario",
        "nome_fantasia_destinatario",
        "cpf_cnpj_emitente",
        "razao_social_emitente",
        "nome_fantasia_emitente",
    ],
    "df_valores_ajustados.csv": ["valor_produtos_ajustado", "valor_unitario_ajustado"],
    "df_chaves.csv": ["chave_codigo"],
    "df_eans.csv": ["EAN_1", "EAN_2", "EAN_3"],
}


def carregar_dataframe() -> pd.DataFrame:
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(
            "Arquivo da Etapa 21 não encontrado. Execute a etapa anterior primeiro."
        )

    print("\n" + "=" * 80)
    print("CARREGANDO DADOS DA ETAPA 21 PARA PARTICIONAMENTO")
    print("=" * 80)

    with zipfile.ZipFile(INPUT_ZIP, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            raise ValueError("Nenhum CSV encontrado dentro do arquivo da Etapa 21.")
        with zf.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, sep=";", low_memory=False)

    print(f"[OK] Registros carregados: {len(df):,}")
    return df


def preparar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_proc = df.copy()
    df_proc.reset_index(drop=True, inplace=True)
    df_proc["id"] = df_proc.index

    for coluna in ["valor_produtos_ajustado", "valor_unitario_ajustado"]:
        if coluna in df_proc.columns:
            df_proc[coluna] = pd.to_numeric(df_proc[coluna], errors="coerce")

    return df_proc


def salvar_qlikview(df: pd.DataFrame, destino: Path, nome_arquivo: str) -> None:
    destino.mkdir(parents=True, exist_ok=True)
    caminho = destino / nome_arquivo

    if caminho.exists():
        df_antigo = pd.read_csv(caminho, sep=";", low_memory=False)
        df = pd.concat([df_antigo, df], ignore_index=True)
        df.drop_duplicates(inplace=True)

    df.to_csv(caminho, sep=";", index=False, encoding="utf-8")
    print(f"[OK] Arquivo atualizado em {caminho.relative_to(PROJECT_ROOT)}")


def extrair_tabelas(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    df_central = df.copy()
    estatisticas = {}

    for nome_arquivo, colunas in TABELAS_A_CRIAR.items():
        colunas_existentes = [col for col in colunas if col in df_central.columns]
        if not colunas_existentes:
            print(f"[AVISO] Colunas para {nome_arquivo} não encontradas. Pulando.")
            continue

        print(f"Processando {nome_arquivo}...")
        subset = df_central[["id"] + colunas_existentes].copy()
        subset.dropna(how="all", subset=colunas_existentes, inplace=True)
        subset.drop_duplicates(inplace=True)

        salvar_qlikview(subset, QLIKVIEW_DIR, nome_arquivo)
        estatisticas[nome_arquivo] = len(subset)

        df_central.drop(columns=colunas_existentes, inplace=True)

    return df_central, estatisticas


def ajustar_municipio(df: pd.DataFrame) -> pd.DataFrame:
    if "municipio" in df.columns:
        df.loc[df["municipio"] == "SANTA TERESINHA", "municipio"] = "SANTA TEREZINHA"
    return df


def exportar_central(df: pd.DataFrame) -> None:
    """Export df_central as CSV in QlikView/; concatenate + dedupe if exists."""
    QLIKVIEW_DIR.mkdir(parents=True, exist_ok=True)
    caminho = CENTRAL_CSV
    if caminho.exists():
        df_antigo = pd.read_csv(caminho, sep=";", low_memory=False)
        df = pd.concat([df_antigo, df], ignore_index=True)
        df.drop_duplicates(inplace=True)

    df.to_csv(caminho, sep=";", index=False, encoding="utf-8")
    tamanho_mb = caminho.stat().st_size / (1024 * 1024)
    print(f"[OK] df_central.csv salvo em QlikView ({tamanho_mb:.2f} MB)")


def mover_nfe_vencimento() -> None:
    if not VENCIMENTO_ORIGEM.exists():
        print("[AVISO] nfe_vencimento.csv não encontrado em data/external. Pulando cópia.")
        return

    df_venc = pd.read_csv(VENCIMENTO_ORIGEM, sep=";", low_memory=False)
    df_venc.drop_duplicates(inplace=True)

    if VENCIMENTO_DESTINO.exists():
        df_antigo = pd.read_csv(VENCIMENTO_DESTINO, sep=";", low_memory=False)
        df_venc = pd.concat([df_antigo, df_venc], ignore_index=True)
        df_venc.drop_duplicates(inplace=True)

    QLIKVIEW_DIR.mkdir(parents=True, exist_ok=True)
    df_venc.to_csv(VENCIMENTO_DESTINO, sep=";", index=False, encoding="utf-8")
    print("[OK] nfe_vencimento.csv disponível na pasta QlikView")


def main() -> bool:
    try:
        df = carregar_dataframe()
        df_preparado = preparar_dataframe(df)
        df_central, estatisticas = extrair_tabelas(df_preparado)
        df_central = ajustar_municipio(df_central)
        exportar_central(df_central)
        mover_nfe_vencimento()

        print("\nResumo do particionamento:")
        for nome, linhas in estatisticas.items():
            print(f" - {nome}: {linhas:,} linhas")
        print("\n[SUCESSO] Etapa 22 concluída!")
        return True
    except Exception as exc:  # pragma: no cover
        print(f"\n[ERRO] Etapa 22 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
