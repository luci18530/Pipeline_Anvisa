"""
Script para processar a base ANVISA (CMED)
Carrega, limpa e prepara os dados de preços de medicamentos
"""

import sys
import os

# Adicionar diretório raiz ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.anvisa_base import processar_base_anvisa


def main():
    """Função principal"""
    try:
        # Processar base ANVISA
        dfpre = processar_base_anvisa()
        
        print("\n[INFO] Base ANVISA pronta para uso!")
        print(f"[INFO] Variável 'dfpre' contém {len(dfpre):,} registros")
        
        return dfpre
        
    except FileNotFoundError as e:
        print(f"\n[ERRO] {str(e)}")
        return None
    except Exception as e:
        print(f"\n[ERRO] Erro inesperado: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    dfpre = main()
