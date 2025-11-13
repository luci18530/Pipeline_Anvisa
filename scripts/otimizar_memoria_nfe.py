"""
Script para otimizar memória do DataFrame de NFe enriquecido
Parte da Etapa 6 do pipeline
"""

import sys
import os
import glob

# Adicionar src ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from src.otimizar_memoria import preparar_nfe_para_matching


def main():
    """Função principal"""
    
    # Encontrar arquivo de NFe enriquecido mais recente
    data_dir = "data/processed"
    pattern = os.path.join(data_dir, "nfe_enriquecido_*.csv")
    arquivos = glob.glob(pattern)
    
    if not arquivos:
        print(f"[ERRO] Nenhum arquivo de NFe enriquecido encontrado em: {pattern}")
        print("[INFO] Execute as etapas anteriores do pipeline primeiro")
        return False
    
    # Usar arquivo mais recente
    arquivo_entrada = max(arquivos, key=os.path.getctime)
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
