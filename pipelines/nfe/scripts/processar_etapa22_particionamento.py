"""Script auxiliar para executar a Etapa 22 (Particionamento QlikView)."""

import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = CURRENT_DIR.parent
PROJECT_ROOT = PIPELINE_ROOT.parent.parent
SRC_DIR = PIPELINE_ROOT / "src"

for path in (PROJECT_ROOT, PIPELINE_ROOT, SRC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from pipelines.nfe.src.nfe_etapa22_particionamento import main as executar_etapa22


def main() -> int:
    sucesso = executar_etapa22()
    return 0 if sucesso else 1


if __name__ == "__main__":
    raise SystemExit(main())
