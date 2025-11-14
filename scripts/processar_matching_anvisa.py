"""
Script para processar matching entre NFe e base ANVISA (CMED)
Parte da Etapa 7 do pipeline
"""

import sys
import os
import glob

# Adicionar src ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from datetime import datetime
from src.nfe_matching_anvisa import processar_matching_anvisa
from src.anvisa_base import processar_base_anvisa


def main():
    """Função principal"""
    
    print("="*60)
    print("Script de Matching NFe x ANVISA (CMED)")
    print("="*60 + "\n")
    
    # ========== 1. Carregar NFe enriquecido (após otimização) ==========
    data_dir = "data/processed"
    
    # MODIFICADO: Busca arquivo SEM timestamp
    arquivo_nfe = os.path.join(data_dir, "nfe_etapa04_enriquecido.csv")
    
    if not os.path.exists(arquivo_nfe):
        # Fallback: procura com timestamp
        pattern = os.path.join(data_dir, "nfe_enriquecido_*.csv")
        arquivos_nfe = glob.glob(pattern)
        if not arquivos_nfe:
            print(f"[ERRO] Nenhum arquivo de NFe enriquecido encontrado em: {pattern}")
            print("[INFO] Execute as etapas anteriores do pipeline primeiro")
            return False
        arquivo_nfe = max(arquivos_nfe, key=os.path.getctime)
    
    print(f"[INFO] Carregando NFe enriquecido: {os.path.basename(arquivo_nfe)}")
    
    try:
        df_nfe = pd.read_csv(arquivo_nfe, sep=';', encoding='utf-8-sig')
        print(f"[OK] {len(df_nfe):,} registros de NFe carregados\n")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar NFe: {str(e)}")
        return False
    
    # ========== 2. Carregar base ANVISA ==========
    print("[INFO] Carregando base ANVISA (CMED)...")
    
    try:
        dfpre_anvisa = processar_base_anvisa()
        print(f"[OK] {len(dfpre_anvisa):,} registros ANVISA carregados\n")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar base ANVISA: {str(e)}")
        return False
    
    # ========== 3. Processar matching ==========
    print("[INFO] Iniciando matching NFe x ANVISA...")
    
    try:
        df_matched = processar_matching_anvisa(df_nfe, dfpre_anvisa)
        
        # Gerar nome do arquivo de saída
        arquivo_saida = os.path.join(data_dir, "nfe_etapa07_matched.csv")
        
        # Salvar resultado
        print(f"\n[INFO] Salvando dados com matching em: {arquivo_saida}")
        df_matched.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8-sig')
        
        tamanho_mb = os.path.getsize(arquivo_saida) / (1024 * 1024)
        print(f"[OK] Arquivo salvo com sucesso ({tamanho_mb:.1f} MB)")
        
        print("\n" + "="*60)
        print("[SUCESSO] Matching concluído!")
        print("="*60)
        print(f"\nArquivo gerado:")
        print(f"  - {os.path.basename(arquivo_saida)}")
        
        return True
        
    except Exception as e:
        print(f"\n[ERRO] Falha no matching: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    sucesso = main()
    sys.exit(0 if sucesso else 1)
