"""
Script de processamento de limpeza de descrições de NFe
Etapa 3 do pipeline
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

from nfe_limpeza import processar_limpeza_nfe


def main():
    """Função principal"""
    try:
        # Encontrar arquivo processado mais recente (carregamento)
        arquivos = glob.glob("data/processed/nfe_etapa01_processado.csv")
        
        if not arquivos:
            print("[ERRO] Nenhum arquivo processado encontrado em data/processed/")
            print("[INFO] Execute primeiro: python scripts/processar_nfe.py")
            sys.exit(1)
        
        # Pegar o mais recente
        arquivo_entrada = max(arquivos, key=os.path.getmtime)
        
        # Processar limpeza
        df_limpo, caminho_saida = processar_limpeza_nfe(arquivo_entrada)
        
        print(f"\n[SUCESSO] Limpeza concluída!")
        print(f"[INFO] Arquivo gerado: {caminho_saida}")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
