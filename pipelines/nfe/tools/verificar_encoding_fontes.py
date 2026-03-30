"""
Valida encoding das fontes Python do pipeline NFe.
"""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.nfe.src.encoding_guard import assert_no_encoding_corruption


def main() -> int:
    nfe_root = ROOT / "pipelines" / "nfe"
    try:
        assert_no_encoding_corruption(nfe_root)
        print("[OK] Nenhuma corrupcao de encoding detectada nas fontes Python do NFe.")
        return 0
    except RuntimeError as exc:
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

