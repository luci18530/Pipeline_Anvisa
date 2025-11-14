"""
Script para processar e separar dados de vencimento de Notas Fiscais
"""

import sys
import os
import glob
import pandas as pd

# Adicionar diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.nfe_vencimento import processar_vencimento_nfe, salvar_dados_vencimento


def main():
    """Executa pipeline de processamento de vencimento"""
    
    print("\n" + "="*60)
    print("Pipeline de Processamento de Vencimento de NFe")
    print("="*60 + "\n")
    
    # Encontrar arquivo processado mais recente
    arquivos = glob.glob("data/processed/nfe_etapa01_processado.csv")
    
    if not arquivos:
        print("[ERRO] Nenhum arquivo processado encontrado em data/processed/")
        print("[INFO] Execute primeiro: python scripts/processar_nfe.py")
        return
    
    arquivo_entrada = max(arquivos, key=os.path.getmtime)
    print(f"[INFO] Arquivo de entrada: {arquivo_entrada}\n")
    
    try:
        # Carregar dados
        print("[INFO] Carregando dados...")
        df = pd.read_csv(arquivo_entrada, sep=';', dtype=str)
        print(f"[OK] {len(df):,} registros carregados\n")
        
        # Processar vencimento
        df_base, df_venc = processar_vencimento_nfe(df)
        
        # Salvar dados de vencimento
        print("\n" + "="*60)
        print("Salvando Dados de Vencimento")
        print("="*60 + "\n")
        
        caminho_venc_parquet = salvar_dados_vencimento(df_venc, formato='parquet')
        caminho_venc_csv = salvar_dados_vencimento(df_venc, formato='csv')
        
        # Exibir distribuição de vencimentos
        print("\n" + "="*60)
        print("Distribuição de Vencimentos")
        print("="*60)
        
        print("\nContagem por categoria:")
        print(df_venc['categoria_vencimento'].value_counts().sort_values(ascending=False))
        
        print("\nPercentual por categoria:")
        print((df_venc['categoria_vencimento'].value_counts(normalize=True).sort_values(ascending=False) * 100).round(2))
        
        # Estatísticas adicionais
        print("\n" + "="*60)
        print("Estatísticas Adicionais")
        print("="*60)
        
        print(f"\nDias restantes:")
        print(f"  - Mínimo: {df_venc['dias_restantes'].min()} dias")
        print(f"  - Máximo: {df_venc['dias_restantes'].max()} dias")
        print(f"  - Média: {df_venc['dias_restantes'].mean():.0f} dias")
        print(f"  - Mediana: {df_venc['dias_restantes'].median():.0f} dias")
        
        print(f"\nVida usada (%):")
        print(f"  - Mínimo: {df_venc['vida_usada_porcento'].min():.2%}")
        print(f"  - Máximo: {df_venc['vida_usada_porcento'].max():.2%}")
        print(f"  - Média: {df_venc['vida_usada_porcento'].mean():.2%}")
        print(f"  - Mediana: {df_venc['vida_usada_porcento'].median():.2%}")
        
        print("\n" + "="*60)
        print("[SUCESSO] Pipeline concluído com sucesso!")
        print("="*60)
        print(f"\nArquivos gerados:")
        print(f"  - Parquet: {caminho_venc_parquet}")
        print(f"  - CSV: {caminho_venc_csv}")
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
