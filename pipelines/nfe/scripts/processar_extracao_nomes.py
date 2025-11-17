"""
Script: processar_extracao_nomes.py
Descrição: Executa a Etapa 10 - Extração de Nomes de Produtos
Autor: Pipeline ANVISA
Data: 2025-11-13
"""

import sys
import os
import glob
from pathlib import Path

# Adicionar src da pipeline ao path
CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
from nfe_etapa10_extracao_nomes import processar_extracao_nomes

if __name__ == "__main__":
    try:
        df_resultado = processar_extracao_nomes()
        
        if df_resultado is not None:
            print("\n[SUCESSO] Extracao de nomes concluida!")
            sys.exit(0)
        else:
            print("\n[ERRO] Falha na extracao de nomes.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
