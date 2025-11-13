"""
Script de processamento de enriquecimento de dados de NFe com informações de município
"""

import sys
import os
import glob

# Adicionar src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.nfe_enriquecimento import processar_enriquecimento_nfe


def main():
    """Função principal"""
    try:
        # Encontrar arquivo limpo mais recente
        arquivos = glob.glob("data/processed/nfe_limpo_*.csv")
        
        if not arquivos:
            print("[ERRO] Nenhum arquivo limpo encontrado em data/processed/")
            print("[INFO] Execute primeiro: python scripts/processar_limpeza.py")
            sys.exit(1)
        
        # Pegar o mais recente
        arquivo_entrada = max(arquivos, key=os.path.getmtime)
        
        # Processar enriquecimento
        df_enriquecido, caminho_saida = processar_enriquecimento_nfe(arquivo_entrada)
        
        print(f"\n[SUCESSO] Enriquecimento concluído!")
        print(f"[INFO] Arquivo gerado: {caminho_saida}")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
