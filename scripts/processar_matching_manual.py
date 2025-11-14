"""
Script de Processamento de Matching Manual NFe x Base Manual
Etapa 8: Matching com base manual do Google Sheets
"""

import sys
import os
import glob

# Adicionar src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.nfe_matching_manual import processar_matching_manual


def main():
    """Função principal"""
    
    print("="*60)
    print("Script de Matching Manual NFe")
    print("="*60 + "\n")
    
    # Encontrar arquivo de matching mais recente
    print("[INFO] Procurando arquivo nfe_etapa07_matched.csv...")
    arquivo_entrada = "data/processed/nfe_etapa07_matched.csv"
    
    if not os.path.exists(arquivo_entrada):
        # Fallback: procura com timestamp
        arquivos_matched = glob.glob("data/processed/nfe_matched_*.csv")
        if not arquivos_matched:
            print("[ERRO] Nenhum arquivo nfe_matched encontrado!")
            print("\nExecute primeiro as etapas 1-7 do pipeline.")
            sys.exit(1)
        arquivo_entrada = max(arquivos_matched, key=os.path.getmtime)
    
    print(f"[OK] Arquivo encontrado: {os.path.basename(arquivo_entrada)}\n")
    
    try:
        # Processar matching manual
        df, arquivo_saida = processar_matching_manual(arquivo_entrada)
        
        print("\n[SUCESSO] Matching manual concluído!")
        print(f"[INFO] Arquivo gerado: {arquivo_saida}")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
