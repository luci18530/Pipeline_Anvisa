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
import time
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.nfe.src.paths import DATA_DIR, PROJECT_ROOT

INPUT_ZIP = DATA_DIR / "processed" / "df_etapa21_unidades_padronizadas.zip"
QLIKVIEW_DIR = PROJECT_ROOT / "QlikView"
CENTRAL_CSV = QLIKVIEW_DIR / "df_central.csv"
VENCIMENTO_ORIGEM = DATA_DIR / "external" / "nfe_vencimento.csv"
VENCIMENTO_DESTINO = QLIKVIEW_DIR / "nfe_vencimento.csv"
RECONCILIACAO_DEDUP = DATA_DIR / "processed" / "etapa22_reconciliacao_deduplicacao.csv"
CSV_NAME = "df_etapa22_particionamento.csv"
PARQUET_DIR = QLIKVIEW_DIR / "compact_parquet"

BUSINESS_KEY_COLUMNS = ["chave_codigo", "id_descricao"]

TABELAS_A_CRIAR: Dict[str, List[str]] = {
    "df_entidades.csv": [
        "cpf_cnpj",
        "razao_social_destinatario",
        "nome_fantasia_destinatario",
        "cpf_cnpj_emitente",
        "razao_social_emitente",
        "nome_fantasia_emitente",
    ],
    "df_valores_ajustados.csv": ["valor_produtos_ajustado", "valor_unitario_ajustado"],
}

WRITE_RETRY_ATTEMPTS = 3
WRITE_RETRY_DELAY_SECONDS = 1.5
CHUNK_SIZE = 150_000
ARQUIVOS_OBSOLETOS = [
    "df_dosagem.csv",
    "df_eans.csv",
    "df_registro_anvisa.csv",
    "compact_parquet/df_registro_anvisa.parquet",
]
COLUNAS_IDENTIFICADORES_NUMERICOS = ["EAN_1", "EAN_2", "EAN_3", "REGISTRO", "codigo_ean", "cod_anvisa"]


def _resolver_chave_negocio(df: pd.DataFrame) -> List[str]:
    if "chave_codigo" not in df.columns:
        raise ValueError(
            "Coluna obrigatória 'chave_codigo' ausente. Não é possível deduplicar por chave de negócio."
        )
    cols = ["chave_codigo"]
    if "id_descricao" in df.columns:
        cols.append("id_descricao")
    return cols


def _salvar_csv_com_retry(df: pd.DataFrame, caminho: Path, sep: str = ";", encoding: str = "utf-8") -> None:
    """Salva CSV com retry para lidar com lock temporario de arquivo."""
    ultimo_erro = None
    caminho.parent.mkdir(parents=True, exist_ok=True)

    for tentativa in range(1, WRITE_RETRY_ATTEMPTS + 1):
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".csv",
                delete=False,
                encoding=encoding,
                newline="",
            ) as tmp_file:
                tmp_path = Path(tmp_file.name)
                df.to_csv(tmp_file, sep=sep, index=False)

            os.replace(tmp_path, caminho)
            return
        except PermissionError as exc:
            ultimo_erro = exc
            if tmp_path is not None and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            if tentativa < WRITE_RETRY_ATTEMPTS:
                print(
                    f"[AVISO] Arquivo em uso: {caminho}. "
                    f"Tentando novamente ({tentativa}/{WRITE_RETRY_ATTEMPTS})..."
                )
                time.sleep(WRITE_RETRY_DELAY_SECONDS)
        except Exception:
            if tmp_path is not None and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            raise

    raise PermissionError(
        f"Falha ao salvar {caminho} apos {WRITE_RETRY_ATTEMPTS} tentativas. "
        "Feche o arquivo em outro programa (ex.: Excel/QlikView) e execute novamente."
    ) from ultimo_erro


def _append_csv_com_retry(df: pd.DataFrame, caminho: Path, sep: str = ";", encoding: str = "utf-8") -> None:
    """Append em CSV com retry para cenarios de lock temporario."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    ultimo_erro = None
    append_header = not caminho.exists() or caminho.stat().st_size == 0

    for tentativa in range(1, WRITE_RETRY_ATTEMPTS + 1):
        try:
            df.to_csv(
                caminho,
                sep=sep,
                index=False,
                mode="a",
                header=append_header,
                encoding=encoding,
            )
            return
        except PermissionError as exc:
            ultimo_erro = exc
            if tentativa < WRITE_RETRY_ATTEMPTS:
                print(
                    f"[AVISO] Arquivo em uso: {caminho}. "
                    f"Tentando novamente ({tentativa}/{WRITE_RETRY_ATTEMPTS})..."
                )
                time.sleep(WRITE_RETRY_DELAY_SECONDS)

    raise PermissionError(
        f"Falha ao append em {caminho} apos {WRITE_RETRY_ATTEMPTS} tentativas. "
        "Feche o arquivo em outro programa (ex.: Excel/QlikView) e execute novamente."
    ) from ultimo_erro


def _hash_key_series(df: pd.DataFrame, chaves: List[str]) -> pd.Series:
    base = df[chaves].copy()
    for c in chaves:
        base[c] = base[c].astype("string").fillna("")
    return pd.util.hash_pandas_object(base, index=False).astype("uint64")


def _carregar_hashes_existentes(caminho_csv: Path, chaves: List[str], chunksize: int = 250_000) -> set[int]:
    hashes_existentes: set[int] = set()
    if not caminho_csv.exists():
        return hashes_existentes

    for chunk in pd.read_csv(
        caminho_csv,
        sep=";",
        usecols=chaves,
        low_memory=False,
        chunksize=chunksize,
    ):
        hashes_existentes.update(_hash_key_series(chunk, chaves).tolist())

    return hashes_existentes


def _normalizar_identificadores_numericos(df: pd.DataFrame) -> None:
    """Normaliza colunas de identificadores para string inteira sem sufixo '.0'."""
    for coluna in COLUNAS_IDENTIFICADORES_NUMERICOS:
        if coluna not in df.columns:
            continue
        serie = df[coluna].astype("string").str.strip()
        serie = serie.str.replace(",", ".", regex=False)
        serie = serie.str.replace(r"\.0+$", "", regex=True)
        serie = serie.replace({"": pd.NA, "<NA>": pd.NA, "nan": pd.NA, "None": pd.NA})
        df[coluna] = serie


def _ler_colunas_csv(caminho: Path) -> List[str]:
    if not caminho.exists():
        return []
    return pd.read_csv(caminho, sep=";", nrows=0).columns.tolist()


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
    if exists:
        log_antigo = pd.read_csv(RECONCILIACAO_DEDUP, sep=";", low_memory=False)
        log_saida = pd.concat([log_antigo, log_df[cols]], ignore_index=True)
    else:
        log_saida = log_df[cols]
    _salvar_csv_com_retry(log_saida, RECONCILIACAO_DEDUP, sep=";", encoding="utf-8")


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

        try:
            with zf.open(csv_name) as csv_source:
                df = pd.read_csv(csv_source, sep=";", low_memory=False)
        except Exception as exc:
            print(f"[AVISO] Leitura direta do ZIP falhou ({exc}). Aplicando fallback chunked...")
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", text=False)
            try:
                with os.fdopen(tmp_fd, "wb") as tmp_file:
                    with zf.open(csv_name) as csv_source:
                        tmp_file.write(csv_source.read())

                chunks = []
                for i, chunk in enumerate(
                    pd.read_csv(tmp_path, sep=";", low_memory=False, chunksize=CHUNK_SIZE)
                ):
                    chunks.append(chunk)
                    if (i + 1) % 10 == 0:
                        print(f"[INFO] Fallback chunked: processados ~{(i + 1) * CHUNK_SIZE:,} registros")
                df = pd.concat(chunks, ignore_index=True)
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

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
    df_proc = df
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

    _normalizar_identificadores_numericos(df_proc)

    return df_proc


def salvar_qlikview(df: pd.DataFrame, destino: Path, nome_arquivo: str) -> None:
    destino.mkdir(parents=True, exist_ok=True)
    caminho = destino / nome_arquivo

    if caminho.exists():
        colunas_atuais = _ler_colunas_csv(caminho)
        colunas_novas = df.columns.tolist()
        if colunas_atuais != colunas_novas:
            print(f"[AVISO] Schema alterado em {nome_arquivo}. Recriando arquivo com novo layout.")
            if "id" in df.columns:
                _salvar_csv_com_retry(df.drop_duplicates(subset=["id"]), caminho, sep=";", encoding="utf-8")
            else:
                _salvar_csv_com_retry(df.drop_duplicates(), caminho, sep=";", encoding="utf-8")
            print(f"[OK] Arquivo recriado em {caminho.relative_to(PROJECT_ROOT)}")
            return

    if caminho.exists() and "id" in df.columns:
        ids_existentes = set()
        for chunk in pd.read_csv(caminho, sep=";", usecols=["id"], low_memory=False, chunksize=250_000):
            ids_existentes.update(chunk["id"].astype("string").dropna().tolist())
        mask_novos = ~df["id"].astype("string").isin(ids_existentes)
        df_novo = df.loc[mask_novos].copy()
        if df_novo.empty:
            print(f"[OK] {nome_arquivo} sem novos registros para append")
            return
        _append_csv_com_retry(df_novo, caminho, sep=";", encoding="utf-8")
    elif caminho.exists():
        _append_csv_com_retry(df.drop_duplicates(), caminho, sep=";", encoding="utf-8")
    else:
        _salvar_csv_com_retry(df, caminho, sep=";", encoding="utf-8")

    print(f"[OK] Arquivo atualizado em {caminho.relative_to(PROJECT_ROOT)}")


def extrair_tabelas(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    df_central = df
    estatisticas: Dict[str, int] = {}

    for nome_arquivo, colunas in TABELAS_A_CRIAR.items():
        colunas_existentes = [col for col in colunas if col in df_central.columns]
        if not colunas_existentes:
            print(f"[AVISO] Colunas para {nome_arquivo} não encontradas. Pulando.")
            continue

        print(f"Processando {nome_arquivo}...")
        subset = df_central[["id"] + colunas_existentes].copy()
        _normalizar_identificadores_numericos(subset)
        subset.dropna(how="all", subset=colunas_existentes, inplace=True)
        subset.drop_duplicates(subset=["id"], inplace=True)

        salvar_qlikview(subset, QLIKVIEW_DIR, nome_arquivo)
        estatisticas[nome_arquivo] = len(subset)
        df_central.drop(columns=colunas_existentes, inplace=True)

    if "CHECK_EMISSAO_APOS_VIGENCIA" in df_central.columns:
        df_central.drop(columns=["CHECK_EMISSAO_APOS_VIGENCIA"], inplace=True)

    _normalizar_identificadores_numericos(df_central)
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
        colunas_atuais = _ler_colunas_csv(CENTRAL_CSV)
        colunas_novas = df.columns.tolist()
        if colunas_atuais != colunas_novas:
            print("[AVISO] Schema do df_central mudou. Recriando arquivo completo com novo layout.")
            _salvar_csv_com_retry(df, CENTRAL_CSV, sep=";", encoding="utf-8")
            tamanho_mb = CENTRAL_CSV.stat().st_size / (1024 * 1024)
            print(f"[OK] df_central.csv recriado ({tamanho_mb:.2f} MB)")
            print("=" * 80)
            return

        hashes_existentes = _carregar_hashes_existentes(CENTRAL_CSV, chaves)
        print(f"[INFO] Hashes de chave da base anterior carregados: {len(hashes_existentes):,}")

        hashes_novos = _hash_key_series(df, chaves)
        mask_novos = ~hashes_novos.isin(hashes_existentes)
        qtd_repetidos = int((~mask_novos).sum())
        if qtd_repetidos > 0:
            print(f"[INFO] Registros já existentes no df_central: {qtd_repetidos:,}")

        df_incremento = df.loc[mask_novos].copy()
        if df_incremento.empty:
            print("[RESUMO] Nenhum registro novo para adicionar ao df_central")
            print("=" * 80)
            return

        _append_csv_com_retry(df_incremento, CENTRAL_CSV, sep=";", encoding="utf-8")
        print(f"[RESUMO] Incremento líquido no df_central: +{len(df_incremento):,} registros")
    else:
        print(f"[INFO] Primeira exportação do df_central: {len(df):,} registros")
        _salvar_csv_com_retry(df, CENTRAL_CSV, sep=";", encoding="utf-8")

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
    _salvar_csv_com_retry(df_venc, VENCIMENTO_DESTINO, sep=";", encoding="utf-8")
    print("[OK] nfe_vencimento.csv disponível na pasta QlikView")


def remover_arquivos_obsoletos() -> None:
    for nome in ARQUIVOS_OBSOLETOS:
        caminho = QLIKVIEW_DIR / nome
        if caminho.exists():
            caminho.unlink()
            print(f"[OK] Arquivo obsoleto removido: {caminho.relative_to(PROJECT_ROOT)}")


def exportar_parquet_compacto_de_csv(caminho_csv: Path, caminho_parquet: Path) -> None:
    """Converte CSV em Parquet compactado (ZSTD), em modo chunked."""
    caminho_parquet.parent.mkdir(parents=True, exist_ok=True)
    if caminho_parquet.exists():
        caminho_parquet.unlink()

    writer = None
    try:
        for chunk in pd.read_csv(
            caminho_csv,
            sep=";",
            dtype="string",
            low_memory=False,
            chunksize=CHUNK_SIZE,
        ):
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    where=caminho_parquet,
                    schema=table.schema,
                    compression="zstd",
                    use_dictionary=True,
                )
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    print(f"[OK] Parquet compacto gerado: {caminho_parquet.relative_to(PROJECT_ROOT)}")


def gerar_pacote_compacto_parquet() -> None:
    """Gera pacote compacto para transporte via internet."""
    alvos_csv = [
        QLIKVIEW_DIR / "df_central.csv",
        QLIKVIEW_DIR / "df_entidades.csv",
        QLIKVIEW_DIR / "df_valores_ajustados.csv",
        QLIKVIEW_DIR / "nfe_vencimento.csv",
    ]
    print("\n" + "=" * 80)
    print("GERANDO PACOTE COMPACTO (PARQUET/ZSTD)")
    print("=" * 80)
    for caminho_csv in alvos_csv:
        if not caminho_csv.exists():
            print(f"[AVISO] CSV ausente para compactação: {caminho_csv.relative_to(PROJECT_ROOT)}")
            continue
        caminho_parquet = PARQUET_DIR / f"{caminho_csv.stem}.parquet"
        exportar_parquet_compacto_de_csv(caminho_csv, caminho_parquet)


def main() -> bool:
    try:
        df = carregar_dataframe()
        df_limpo = limpar_duplicatas_chave_codigo(df)
        df_preparado = preparar_dataframe(df_limpo)
        df_central, estatisticas = extrair_tabelas(df_preparado)
        df_central = ajustar_municipio(df_central)
        exportar_central(df_central)
        mover_nfe_vencimento()
        remover_arquivos_obsoletos()
        gerar_pacote_compacto_parquet()

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
