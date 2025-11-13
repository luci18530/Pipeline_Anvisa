"""
Script: processar_refinamento_nomes.py
Descrição: Executa a Etapa 11 - Refinamento de Nomes
Autor: Pipeline ANVISA
Data: 2025-11-13
"""

import os
import sys

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.nfe_refinamento_nomes import processar_refinamento_nomes

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
