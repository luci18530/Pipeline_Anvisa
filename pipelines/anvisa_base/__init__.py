"""Pipeline ANVISA (CMED)."""

from pipelines.anvisa_base.download import run as run_download
from pipelines.anvisa_base.main import run as run_pipeline

__all__ = ["run_download", "run_pipeline"]

