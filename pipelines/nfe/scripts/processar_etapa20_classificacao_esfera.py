"""Script auxiliar para executar a Etapa 20 (Classificação por Esfera)."""

import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfe_etapa20_classificacao_esfera import main as executar_etapa20


def main() -> int:
    sucesso = executar_etapa20()
    return 0 if sucesso else 1


if __name__ == "__main__":
    raise SystemExit(main())
