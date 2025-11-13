"""
Script de teste para carregamento e processamento de dados NFe
"""

import sys
import os

# Adicionar diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.nfe_carregamento import carregar_e_processar_nfe, salvar_dados_processados


def main():
    """Executa pipeline de carregamento de NFe"""
    
    print("\n" + "="*60)
    print("Pipeline de Carregamento de Notas Fiscais (NFe)")
    print("="*60 + "\n")
    
    # Configurações
    arquivo_entrada = "nfe/nfe.csv"
    data_minima = "2020-01-01"
    
    # Verificar se arquivo existe
    if not os.path.exists(arquivo_entrada):
        print(f"[ERRO] Arquivo não encontrado: {arquivo_entrada}")
        print("[INFO] Coloque o arquivo nfe.csv na pasta nfe/")
        return
    
    try:
        # Carregar e processar
        print(f"[INFO] Arquivo de entrada: {arquivo_entrada}")
        print(f"[INFO] Data mínima: {data_minima}\n")
        
        df = carregar_e_processar_nfe(
            arquivo_entrada,
            data_minima=data_minima
        )
        
        # Estatísticas básicas
        print("\n" + "="*60)
        print("Estatísticas do Dataset")
        print("="*60)
        
        if 'valor_produtos' in df.columns:
            print(f"Valor total de produtos: R$ {df['valor_produtos'].sum():,.2f}")
        
        if 'quantidade' in df.columns:
            print(f"Quantidade total: {df['quantidade'].sum():,.0f}")
        
        if 'razao_social_emitente' in df.columns:
            print(f"Número de emitentes únicos: {df['razao_social_emitente'].nunique():,}")
        
        if 'descricao_produto' in df.columns:
            print(f"Número de produtos únicos: {df['descricao_produto'].nunique():,}")
        
        # Salvar dados processados
        print("\n" + "="*60)
        print("Salvando Dados Processados")
        print("="*60)
        
        # Salvar em parquet (mais eficiente)
        caminho_parquet = salvar_dados_processados(df, formato='parquet')
        
        # Salvar em CSV também (para compatibilidade)
        caminho_csv = salvar_dados_processados(df, formato='csv')
        
        print("\n" + "="*60)
        print("[SUCESSO] Pipeline concluído com sucesso!")
        print("="*60)
        print(f"\nArquivos gerados:")
        print(f"  - Parquet: {caminho_parquet}")
        print(f"  - CSV: {caminho_csv}")
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
