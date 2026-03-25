"""Pipeline ANVISA (CMED)."""


def run_download() -> None:
    from pipelines.anvisa_base.download import run

    run()


def run_pipeline(
    skip_download: bool = False,
    skip_stage15: bool = False,
    skip_advanced: bool = False,
    force_refresh: bool = False,
) -> None:
    from pipelines.anvisa_base.main import run

    run(
        skip_download=skip_download,
        skip_stage15=skip_stage15,
        skip_advanced=skip_advanced,
        force_refresh=force_refresh,
    )


__all__ = ["run_download", "run_pipeline"]

