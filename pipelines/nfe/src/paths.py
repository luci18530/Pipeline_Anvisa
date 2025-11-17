"""Common path helpers for the NFe pipeline."""
from pathlib import Path

PIPELINE_ROOT = Path(__file__).resolve().parent.parent
PROJECT_ROOT = PIPELINE_ROOT.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
SUPPORT_DIR = PIPELINE_ROOT / "support"
SRC_DIR = PIPELINE_ROOT / "src"
ANVISA_SRC_DIR = PIPELINE_ROOT.parent / "anvisa_base" / "src"
ANVISA_MODULES_DIR = ANVISA_SRC_DIR / "modules"
