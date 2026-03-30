"""Script auxiliar para executar a Etapa 23 (Diagnóstico Final)."""

import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = CURRENT_DIR.parent
PROJECT_ROOT = PIPELINE_ROOT.parent.parent
SRC_DIR = PIPELINE_ROOT / "src"

for path in (PROJECT_ROOT, PIPELINE_ROOT, SRC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from pipelines.nfe.src.nfe_etapa23_diagnostico_final import executar_diagnostico_final


def main() -> int:
    sucesso = executar_diagnostico_final()
    return 0 if sucesso else 1


if __name__ == "__main__":
    raise SystemExit(main())

