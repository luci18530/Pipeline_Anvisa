"""Script auxiliar para executar a Etapa 21 (Padronização de Unidades)."""

import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfe_etapa21_padronizacao_unidades import main as executar_etapa21


def main() -> int:
    sucesso = executar_etapa21()
    return 0 if sucesso else 1


if __name__ == "__main__":
    raise SystemExit(main())
