"""
Script: processar_extracao_nomes.py
Descrição: Executa a Etapa 10 - Extração de Nomes de Produtos
Autor: Pipeline ANVISA
Data: 2025-11-13
"""

import os
import sys

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.nfe_extracao_nomes import processar_extracao_nomes

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
