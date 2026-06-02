# -*- coding: utf-8 -*-
"""Treina o modelo local da Etapa 14 a partir do cache supervisionado."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.nfe.src.nfe_etapa14_modelo_local import (  # noqa: E402
    DEFAULT_METRICS_PATH,
    DEFAULT_MIN_CONFIDENCE,
    DEFAULT_MODEL_PATH,
    DEFAULT_TRAINING_CSV,
    treinar_e_salvar_modelo,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Treina o modelo local da Etapa 14")
    parser.add_argument("--training-csv", default=str(DEFAULT_TRAINING_CSV))
    parser.add_argument("--model-path", default=str(DEFAULT_MODEL_PATH))
    parser.add_argument("--metrics-path", default=str(DEFAULT_METRICS_PATH))
    parser.add_argument("--min-confidence", type=float, default=DEFAULT_MIN_CONFIDENCE)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    args = parser.parse_args()

    print("=" * 80)
    print("TREINANDO MODELO LOCAL DA ETAPA 14")
    print("=" * 80)
    print(f"Dataset: {args.training_csv}")
    print(f"Modelo:  {args.model_path}")
    print(f"Metricas:{args.metrics_path}")

    payload = treinar_e_salvar_modelo(
        training_csv=args.training_csv,
        model_path=args.model_path,
        metrics_path=args.metrics_path,
        min_confidence=args.min_confidence,
        test_size=args.test_size,
        random_state=args.random_state,
    )

    print("\n[OK] Modelo treinado e salvo.")
    print(f"Linhas de treino: {payload['linhas_dataset']:,}")
    print(f"Descricoes unicas: {payload['descricoes_unicas']:,}")
    print(f"Tipo de modelo: {payload['tipo_modelo']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
