"""Ferramenta utilitária para extrair um recorte mensal das notas fiscais.

Exemplo de uso:
    python -m pipelines.nfe.tools.filtrar_mes_notas --fonte NOTAS_FISCAIS.csv --destino nfe --ano 2025 --mes 9
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def filtrar_mes(
    caminho_fonte: Path,
    destino_dir: Path,
    ano: int,
    mes: int,
    *,
    chunksize: int = 250_000,
    encoding: str = "latin1",
) -> Path:
    """Filtra o CSV de origem mantendo apenas registros do mês/ano informado."""
    if not caminho_fonte.exists():
        raise FileNotFoundError(f"Arquivo fonte não encontrado: {caminho_fonte}")

    destino_dir.mkdir(parents=True, exist_ok=True)
    destino_csv = destino_dir / "nfe.csv"
    if destino_csv.exists():
        destino_csv.unlink()

    header_escrito = False
    total_registros = 0

    for chunk in pd.read_csv(
        caminho_fonte,
        sep=";",
        dtype=str,
        chunksize=chunksize,
        low_memory=False,
        encoding=encoding,
    ):
        datas = pd.to_datetime(chunk["data_emissao"], errors="coerce")
        filtro = (datas.dt.year == ano) & (datas.dt.month == mes)
        selecionados = chunk.loc[filtro].copy()
        if selecionados.empty:
            continue

        selecionados.to_csv(
            destino_csv,
            sep=";",
            index=False,
            mode="a",
            header=not header_escrito,
            encoding="utf-8",
        )
        header_escrito = True
        total_registros += len(selecionados)

    if not header_escrito:
        raise ValueError(f"Nenhum registro encontrado para {mes:02d}/{ano}")

    print(
        f"[OK] Recorte com {total_registros:,} registros salvo em {destino_csv.as_posix()}"
    )
    return destino_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fonte", type=Path, required=True, help="CSV completo de notas")
    parser.add_argument(
        "--destino",
        type=Path,
        required=True,
        help="Diretório onde o arquivo reduzido (nfe.csv) será salvo",
    )
    parser.add_argument("--ano", type=int, required=True)
    parser.add_argument("--mes", type=int, required=True)
    parser.add_argument("--chunksize", type=int, default=250_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    filtrar_mes(args.fonte, args.destino, args.ano, args.mes, chunksize=args.chunksize)


if __name__ == "__main__":
    main()
