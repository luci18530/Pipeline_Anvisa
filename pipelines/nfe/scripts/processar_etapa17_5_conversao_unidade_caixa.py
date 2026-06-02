# -*- coding: utf-8 -*-
"""Wrapper de execucao da Etapa 17.5."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.nfe.src.nfe_etapa17_5_conversao_unidade_caixa import main


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
