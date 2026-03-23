# -*- coding: utf-8 -*-
"""ETAPA 22: PARTICIONAMENTO DE TABELAS PARA QLIKVIEW.

Gera tabelas auxiliares para o QlikView e exporta `df_central.csv` com
estratégia incremental. A deduplicação usa chave de negócio explícita
(`chave_codigo`, `id_descricao`) e registra reconciliação em trilha auditável.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from pipelines.nfe.src.paths import DATA_DIR, PROJECT_ROOT

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa21_unidades_padronizadas.zip"
QLIKVIEW_DIR = PROJECT_ROOT / "QlikView"
CENTRAL_CSV = QLIKVIEW_DIR / "df_central.csv"
VENCIMENTO_ORIGEM = DATA_DIR / "external" / "nfe_vencimento.csv"
VENCIMENTO_DESTINO = QLIKVIEW_DIR / "nfe_vencimento.csv"
RECONCILIACAO_DEDUP = DATA_DIR / "processed" / "etapa22_reconciliacao_deduplicacao.csv"
CSV_NAME = "df_etapa22_particionamento.csv"

BUSINESS_KEY_COLUMNS = ["chave_codigo", "id_descricao"]

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
    "df_eans.csv": ["EAN_1", "EAN_2", "EAN_3"],
}


def _resolver_chave_negocio(df: pd.DataFrame) -> List[str]:
    if "chave_codigo" not in df.columns:
        raise ValueError(
            "Coluna obrigatória 'chave_codigo' ausente. Não é possível deduplicar por chave de negócio."
        )
    cols = ["chave_codigo"]
    if "id_descricao" in df.columns:
        cols.append("id_descricao")
    return cols


def _registrar_reconciliacao(
    df_duplicados: pd.DataFrame,
    chaves: List[str],
    origem: str,
    etapa: str,
) -> None:
    if df_duplicados.empty:
        return

    log_df = df_duplicados.copy()
    for c in chaves:
        if c not in log_df.columns:
            log_df[c] = pd.NA

    log_df["reconciliacao_timestamp"] = pd.Timestamp.now().isoformat()
    log_df["reconciliacao_origem"] = origem
    log_df["reconciliacao_etapa"] = etapa

    cols = ["reconciliacao_timestamp", "reconciliacao_origem", "reconciliacao_etapa"] + chaves
    extras = [c for c in ("descricao_produto", "codigo_ean", "cpf_cnpj", "valor_produtos_ajustado") if c in log_df.columns]
    cols.extend(extras)

    RECONCILIACAO_DEDUP.parent.mkdir(parents=True, exist_ok=True)
    exists = RECONCILIACAO_DEDUP.exists()
    log_df[cols].to_csv(
        RECONCILIACAO_DEDUP,
        sep=";",
        index=False,
        mode="a" if exists else "w",
        header=not exists,
        encoding="utf-8",
    )


def carregar_dataframe() -> pd.DataFrame:
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(
            "Arquivo da Etapa 21 não encontrado. Execute a etapa anterior primeiro."
        )

    print("\n" + "=" * 80)
    print("CARREGANDO DADOS DA ETAPA 21 PARA PARTICIONAMENTO - MODO CHUNKED")
    print("=" * 80)

    with zipfile.ZipFile(INPUT_ZIP, "r") as zf:
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            raise ValueError("Nenhum CSV encontrado dentro do arquivo da Etapa 21.")

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", text=False)
        try:
            with os.fdopen(tmp_fd, "wb") as tmp_file:
                with zf.open(csv_name) as csv_source:
                    tmp_file.write(csv_source.read())

            chunks = []
            chunk_size = 100_000
            for i, chunk in enumerate(
                pd.read_csv(tmp_path, sep=";", low_memory=False, chunksize=chunk_size)
            ):
                chunks.append(chunk)
                if (i + 1) % 10 == 0:
                    print(f"[INFO] Carregados {(i + 1) * chunk_size:,} registros...")
            df = pd.concat(chunks, ignore_index=True)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError as exc:
                print(f"[AVISO] Falha ao remover arquivo temporário ({tmp_path}): {exc}")

    print(f"[OK] Registros carregados: {len(df):,}")
    return df


def limpar_duplicatas_chave_codigo(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 80)
    print("CHECAGEM E LIMPEZA DE DUPLICATAS")
    print("=" * 80)

    chaves = _resolver_chave_negocio(df)
    print(f"[INFO] Deduplicando por chave de negócio: {chaves}")
    print(f"[INFO] Registros antes da limpeza: {len(df):,}")

    mask_dup = df.duplicated(subset=chaves, keep="first")
    qtd_dup = int(mask_dup.sum())
    if qtd_dup == 0:
        print("[OK] Nenhuma duplicata encontrada na entrada da etapa 22")
        print("=" * 80 + "\n")
        return df

    print(f"[AVISO] Encontradas {qtd_dup:,} duplicatas na entrada da etapa 22")
    _registrar_reconciliacao(df.loc[mask_dup].copy(), chaves, "entrada_etapa22", "preparacao")
    df_limpo = df.loc[~mask_dup].copy()
    print(f"[OK] Duplicatas removidas: {qtd_dup:,}")
    print(f"[OK] Registros após limpeza: {len(df_limpo):,}")
    print("=" * 80 + "\n")
    return df_limpo


def preparar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_proc = df.copy()
    df_proc.reset_index(drop=True, inplace=True)

    def gerar_id_hash(row: pd.Series) -> str:
        chave = str(row.get("chave_codigo", ""))
        id_desc = str(row.get("id_descricao", ""))
        desc_prod = str(row.get("descricao_produto", ""))
        ean = str(row.get("codigo_ean", ""))
        string_unica = f"{chave}|{id_desc}|{desc_prod}|{ean}"
        return hashlib.md5(string_unica.encode("utf-8")).hexdigest()[:24]

    print("[INFO] Gerando IDs únicos baseados em hash MD5...")
    df_proc["id"] = df_proc.apply(gerar_id_hash, axis=1)

    duplicatas = int(df_proc["id"].duplicated().sum())
    if duplicatas > 0:
        print(f"[AVISO] Encontradas {duplicatas:,} duplicatas de ID hash - resolvendo com sufixo...")
        df_proc["_counter"] = df_proc.groupby("id").cumcount()
        mask_duplicado = df_proc["_counter"] > 0
        df_proc.loc[mask_duplicado, "id"] = (
            df_proc.loc[mask_duplicado, "id"] + "_" + df_proc.loc[mask_duplicado, "_counter"].astype(str)
        )
        df_proc.drop(columns=["_counter"], inplace=True)
        print(f"[OK] Duplicatas de ID resolvidas - {len(df_proc):,} IDs únicos")
    else:
        print(f"[OK] {len(df_proc):,} IDs únicos gerados")

    for coluna in ("valor_produtos_ajustado", "valor_unitario_ajustado"):
        if coluna in df_proc.columns:
            df_proc[coluna] = pd.to_numeric(df_proc[coluna], errors="coerce")

    return df_proc


def salvar_qlikview(df: pd.DataFrame, destino: Path, nome_arquivo: str) -> None:
    destino.mkdir(parents=True, exist_ok=True)
    caminho = destino / nome_arquivo

    if caminho.exists():
        df_antigo = pd.read_csv(caminho, sep=";", low_memory=False)
        df = pd.concat([df_antigo, df], ignore_index=True)
        if "id" in df.columns:
            df.drop_duplicates(subset=["id"], inplace=True)
        else:
            df.drop_duplicates(inplace=True)

    df.to_csv(caminho, sep=";", index=False, encoding="utf-8")
    print(f"[OK] Arquivo atualizado em {caminho.relative_to(PROJECT_ROOT)}")


def extrair_tabelas(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    df_central = df.copy()
    estatisticas: Dict[str, int] = {}

    for nome_arquivo, colunas in TABELAS_A_CRIAR.items():
        colunas_existentes = [col for col in colunas if col in df_central.columns]
        if not colunas_existentes:
            print(f"[AVISO] Colunas para {nome_arquivo} não encontradas. Pulando.")
            continue

        print(f"Processando {nome_arquivo}...")
        subset = df_central[["id"] + colunas_existentes].copy()
        subset.dropna(how="all", subset=colunas_existentes, inplace=True)
        subset.drop_duplicates(subset=["id"], inplace=True)

        salvar_qlikview(subset, QLIKVIEW_DIR, nome_arquivo)
        estatisticas[nome_arquivo] = len(subset)
        df_central.drop(columns=colunas_existentes, inplace=True)

    return df_central, estatisticas


def ajustar_municipio(df: pd.DataFrame) -> pd.DataFrame:
    if "municipio" in df.columns:
        df.loc[df["municipio"] == "SANTA TERESINHA", "municipio"] = "SANTA TEREZINHA"
    return df


def exportar_central(df: pd.DataFrame) -> None:
    print("\n" + "=" * 80)
    print("EXPORTANDO DF_CENTRAL")
    print("=" * 80)

    chaves = _resolver_chave_negocio(df)
    print(f"[INFO] Deduplicação por chave de negócio: {chaves}")

    mask_dup_novo = df.duplicated(subset=chaves, keep="first")
    qtd_dup_novo = int(mask_dup_novo.sum())
    if qtd_dup_novo > 0:
        print(f"[AVISO] Encontradas {qtd_dup_novo:,} duplicatas nos novos dados")
        _registrar_reconciliacao(df.loc[mask_dup_novo].copy(), chaves, "novos_dados", "pre_concat")
        df = df.loc[~mask_dup_novo].copy()
        print(f"[OK] Novos dados deduplicados: {len(df):,} registros")
    else:
        print(f"[OK] Novos dados validados: {len(df):,} registros")

    QLIKVIEW_DIR.mkdir(parents=True, exist_ok=True)
    if CENTRAL_CSV.exists():
        df_antigo = pd.read_csv(CENTRAL_CSV, sep=";", low_memory=False)
        print(f"[INFO] Base anterior carregada: {len(df_antigo):,} registros")

        mask_dup_antigo = df_antigo.duplicated(subset=chaves, keep="first")
        qtd_dup_antigo = int(mask_dup_antigo.sum())
        if qtd_dup_antigo > 0:
            print(f"[AVISO] Base anterior contém {qtd_dup_antigo:,} duplicatas")
            _registrar_reconciliacao(df_antigo.loc[mask_dup_antigo].copy(), chaves, "base_anterior", "pre_concat")
            df_antigo = df_antigo.loc[~mask_dup_antigo].copy()
            print(f"[OK] Base anterior deduplicada: {len(df_antigo):,} registros")

        tamanho_antes = len(df_antigo)
        df = pd.concat([df_antigo, df], ignore_index=True)

        mask_dup_pos = df.duplicated(subset=chaves, keep="first")
        qtd_dup_pos = int(mask_dup_pos.sum())
        if qtd_dup_pos > 0:
            print(f"[AVISO] Duplicatas entre cargas encontradas: {qtd_dup_pos:,}")
            _registrar_reconciliacao(df.loc[mask_dup_pos].copy(), chaves, "pos_concatenacao", "pos_concat")
            df = df.loc[~mask_dup_pos].copy()
            print(f"[OK] Base consolidada deduplicada: {len(df):,} registros")

        incremento_liquido = len(df) - tamanho_antes
        print(f"[RESUMO] Incremento líquido no df_central: +{incremento_liquido:,} registros")
    else:
        print(f"[INFO] Primeira exportação do df_central: {len(df):,} registros")

    df.to_csv(CENTRAL_CSV, sep=";", index=False, encoding="utf-8")
    tamanho_mb = CENTRAL_CSV.stat().st_size / (1024 * 1024)
    print(f"[OK] df_central.csv salvo em QlikView ({tamanho_mb:.2f} MB)")
    print("=" * 80)


def mover_nfe_vencimento() -> None:
    if not VENCIMENTO_ORIGEM.exists():
        print("[AVISO] nfe_vencimento.csv não encontrado em data/external. Pulando cópia.")
        return

    df_venc = pd.read_csv(VENCIMENTO_ORIGEM, sep=";", low_memory=False)
    chaves_venc = [c for c in BUSINESS_KEY_COLUMNS if c in df_venc.columns]
    if chaves_venc:
        df_venc.drop_duplicates(subset=chaves_venc, inplace=True)
    else:
        df_venc.drop_duplicates(inplace=True)

    if VENCIMENTO_DESTINO.exists():
        df_antigo = pd.read_csv(VENCIMENTO_DESTINO, sep=";", low_memory=False)
        df_venc = pd.concat([df_antigo, df_venc], ignore_index=True)
        if chaves_venc:
            df_venc.drop_duplicates(subset=chaves_venc, inplace=True)
        else:
            df_venc.drop_duplicates(inplace=True)

    QLIKVIEW_DIR.mkdir(parents=True, exist_ok=True)
    df_venc.to_csv(VENCIMENTO_DESTINO, sep=";", index=False, encoding="utf-8")
    print("[OK] nfe_vencimento.csv disponível na pasta QlikView")


def main() -> bool:
    try:
        df = carregar_dataframe()
        df_limpo = limpar_duplicatas_chave_codigo(df)
        df_preparado = preparar_dataframe(df_limpo)
        df_central, estatisticas = extrair_tabelas(df_preparado)
        df_central = ajustar_municipio(df_central)
        exportar_central(df_central)
        mover_nfe_vencimento()

        print("\nResumo do particionamento:")
        for nome, linhas in estatisticas.items():
            print(f" - {nome}: {linhas:,} linhas")
        print(f" - df_central.csv: {len(df_central):,} linhas")
        print("\n[SUCESSO] Etapa 22 concluída!")
        return True
    except Exception as exc:  # pragma: no cover
        print(f"\n[ERRO] Etapa 22 falhou: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
