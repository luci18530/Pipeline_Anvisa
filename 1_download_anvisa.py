#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIPELINE 1.0 - PREPARAÇÃO DA BASE BRUTA ANVISA

Fase única para preparar o consolidado bruto da ANVISA.

Modos disponíveis:
- download: baixa arquivos da ANVISA e consolida
- reconsolidar: usa ANVISA_LIMPO_*.csv já existentes e reconsolida

Exemplos:
  python 1_download_anvisa.py
  python 1_download_anvisa.py --modo reconsolidar

Próximo passo após esta fase:
  python 2_processar_base_anvisa.py
"""
from __future__ import annotations

import argparse
import sys
import os
import glob


PROJECT_ROOT = os.path.dirname(__file__)

# Adicionar scripts ao path
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "pipelines", "anvisa_base", "scripts")
sys.path.insert(0, SCRIPTS_DIR)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fase 1 unificada da base ANVISA (download ou reconsolidação)."
    )
    parser.add_argument(
        "--modo",
        choices=["download", "reconsolidar", "auto"],
        default="download",
        help=(
            "download: baixa e consolida; "
            "reconsolidar: reconsolida arquivos ANVISA_LIMPO; "
            "auto: reconsolida se houver ANVISA_LIMPO, senão faz download"
        ),
    )
    return parser.parse_args()


def _tem_arquivos_limpos() -> bool:
    pattern = os.path.join(PROJECT_ROOT, "data", "processed", "ANVISA_LIMPO_*.csv")
    return bool(glob.glob(pattern))


def _run_module(module_name: str, file_name: str) -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, os.path.join(SCRIPTS_DIR, file_name))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Não foi possível carregar o módulo {module_name} ({file_name})")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()


def _resolver_modo(modo: str) -> str:
    if modo != "auto":
        return modo
    return "reconsolidar" if _tem_arquivos_limpos() else "download"


def main() -> None:
    args = _parse_args()
    modo = _resolver_modo(args.modo)

    print("=" * 80)
    print("PIPELINE 1.0 - PREPARAÇÃO DA BASE BRUTA ANVISA")
    print("=" * 80)
    print(f"[INFO] Modo selecionado: {modo}")
    print()

    try:
        if modo == "download":
            _run_module("download_consolidacao", "1_download_consolidacao.py")
            mensagem = "[OK] Download + consolidação concluídos!"
        else:
            _run_module("reconsolidar", "reconsolidar_anvisa_limpo.py")
            mensagem = "[OK] Reconsolidação concluída!"

        print()
        print("=" * 80)
        print(mensagem)
        print("Execute agora: python 2_processar_base_anvisa.py")
        print("=" * 80)
    except Exception as e:
        print(f"[ERRO] Falha na fase 1 ({modo}): {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
