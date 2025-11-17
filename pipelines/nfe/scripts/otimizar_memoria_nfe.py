"""
Script para otimizar memória do DataFrame de NFe enriquecido
Parte da Etapa 6 do pipeline
"""

import sys
import os
import glob
from pathlib import Path

# Adicionar src ao path
CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd
from nfe_etapa06_otimizacao_memoria import preparar_nfe_para_matching


def main():
    """Função principal"""
    
    # Encontrar arquivo de NFe enriquecido mais recente
    data_dir = "data/processed"
    
    # MODIFICADO: Busca arquivo SEM timestamp
    arquivo_path = os.path.join(data_dir, "nfe_etapa04_enriquecido.csv")
    
    if not os.path.exists(arquivo_path):
        # Fallback: procura com timestamp
        pattern = os.path.join(data_dir, "nfe_enriquecido_*.csv")
        arquivos = glob.glob(pattern)
        if not arquivos:
            print(f"[ERRO] Nenhum arquivo de NFe enriquecido encontrado em: {pattern}")
            print("[INFO] Execute as etapas anteriores do pipeline primeiro")
            return False
        arquivo_path = max(arquivos, key=os.path.getmtime)
    
    # Usar arquivo
    arquivo_entrada = arquivo_path
    print(f"[INFO] Carregando: {os.path.basename(arquivo_entrada)}")
    
    # Carregar dados
    try:
        df = pd.read_csv(arquivo_entrada, sep=';', encoding='utf-8-sig')
        print(f"[OK] {len(df):,} registros carregados\n")
    except Exception as e:
        print(f"[ERRO] Falha ao carregar arquivo: {str(e)}")
        return False
    
    # Preparar para matching (remove colunas desnecessárias e otimiza memória)
    try:
        df_otimizado = preparar_nfe_para_matching(df)
        
        print(f"\n[INFO] DataFrame otimizado pronto para próximas etapas!")
        print(f"[INFO] Memória final: {df_otimizado.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"[ERRO] Falha na otimização: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    sucesso = main()
    sys.exit(0 if sucesso else 1)
