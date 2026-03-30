"""Gera uma amostra aleatoria de linhas do arquivo nfe.csv para testes."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import pandas as pd


def detectar_separador(csv_path: Path, encoding: str = "utf-8-sig") -> str:
    """Detecta o separador mais provavel do CSV."""
    with csv_path.open("r", encoding=encoding, newline="") as f:
        amostra = f.read(4096)

    try:
        dialect = csv.Sniffer().sniff(amostra, delimiters=";,\t")
        return dialect.delimiter
    except csv.Error:
        return ";"


def gerar_amostra(
    arquivo_entrada: Path,
    arquivo_saida: Path,
    n_linhas: int = 10_000,
    seed: int = 42,
    encoding: str = "utf-8-sig",
) -> int:
    separador = detectar_separador(arquivo_entrada, encoding=encoding)
    df = pd.read_csv(arquivo_entrada, sep=separador, encoding=encoding)

    if df.empty:
        raise ValueError("Arquivo de entrada esta vazio.")

    if len(df) <= n_linhas:
        amostra = df
    else:
        amostra = df.sample(n=n_linhas, random_state=seed, replace=False)

    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
    amostra.to_csv(arquivo_saida, sep=separador, index=False, encoding=encoding)
    return len(amostra)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seleciona linhas aleatorias do arquivo nfe.csv para teste."
    )
    parser.add_argument("--input", default="nfe/nfe.csv", help="Caminho do CSV de entrada")
    parser.add_argument(
        "--output",
        default="nfe/nfe_10k.csv",
        help="Caminho do CSV de saida com a amostra",
    )
    parser.add_argument("--n", type=int, default=10_000, help="Quantidade de linhas na amostra")
    parser.add_argument("--seed", type=int, default=42, help="Seed para reproducao")
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="Encoding do arquivo de entrada/saida",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    entrada = Path(args.input)
    saida = Path(args.output)

    if not entrada.exists():
        print(f"[ERRO] Arquivo de entrada nao encontrado: {entrada}")
        return 1

    try:
        qtd = gerar_amostra(
            arquivo_entrada=entrada,
            arquivo_saida=saida,
            n_linhas=args.n,
            seed=args.seed,
            encoding=args.encoding,
        )
    except Exception as exc:
        print(f"[ERRO] Falha ao gerar amostra: {exc}")
        return 1

    print(f"[OK] Amostra gerada com {qtd:,} linhas em: {saida}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

