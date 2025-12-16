#!/usr/bin/env python
"""Compatibilidade: executa o pipeline da base ANVISA após a reorganização."""

from pipelines.anvisa_base.main import run

if __name__ == "__main__":
    run()
