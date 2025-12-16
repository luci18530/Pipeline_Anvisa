#!/usr/bin/env python
"""Compatibilidade: delega execução ao pipeline oficial de NFe."""

import argparse
from pipelines.nfe.main import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Executa pipeline NFe')
    parser.add_argument('--debug', dest='debug', action='store_true', help='Ativa análise de debug (sobrescreve config)')
    parser.add_argument('--no-debug', dest='debug', action='store_false', help='Força execução sem debug, ignorando config')
    parser.add_argument('--cleanup-processed', dest='cleanup_processed', action='store_true', help='Limpa data/processed ao final do pipeline (somente em caso de sucesso)')
    parser.add_argument('--no-cleanup-processed', dest='cleanup_processed', action='store_false', help='Mantém data/processed mesmo se config pedir limpeza')
    parser.set_defaults(debug=None, cleanup_processed=None)
    args = parser.parse_args()

    success = run(debug_enabled=args.debug, cleanup_processed=args.cleanup_processed)
    raise SystemExit(0 if success else 1)
