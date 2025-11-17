"""
Script: processar_refinamento_nomes.py
Descrição: Executa a Etapa 11 - Refinamento de Nomes
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
from nfe_refinamento_nomes import processar_refinamento_nomes

if __name__ == "__main__":
    try:
        df_resultado = processar_refinamento_nomes()
        
        if df_resultado is not None:
            print("\n[SUCESSO] Refinamento de nomes concluido!")
            sys.exit(0)
        else:
            print("\n[ERRO] Falha no refinamento de nomes.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
