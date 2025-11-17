#!/usr/bin/env python
"""Compatibilidade: delega execução ao pipeline oficial de NFe."""

from pipelines.nfe.main import run

if __name__ == "__main__":
    success = run()
    raise SystemExit(0 if success else 1)
