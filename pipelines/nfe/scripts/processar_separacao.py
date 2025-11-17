"""
Script: processar_separacao.py
Descrição: Executa a Etapa 9 - Separação e Filtragem de NFe
Autor: Pipeline ANVISA
Data: 2025-11-13
"""

import sys
import os
import glob
from pathlib import Path
from datetime import datetime

import pandas as pd

# Adicionar src da pipeline ao path
CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nfe_etapa09_separacao import processar_separacao_e_filtragem

def main():
    """Função principal para executar separação e filtragem."""
    
    print("\n" + "="*80)
    print("ETAPA 9: SEPARACAO E FILTRAGEM DE NFe")
    print("="*80)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*80)
    
    inicio_total = datetime.now()
    
    # ============================================================
    # 1. LOCALIZAR ARQUIVO DE ENTRADA
    # ============================================================
    
    diretorio_dados = "data/processed"
    
    print("\n[INFO] Procurando arquivo de entrada...")
    # Busca primeiro arquivo SEM timestamp
    arquivo_entrada = os.path.join(diretorio_dados, "nfe_etapa08_matched_manual.csv")
    
    if not os.path.exists(arquivo_entrada):
        # Fallback: procura com timestamp
        arquivos = sorted([
            f for f in os.listdir(diretorio_dados)
            if f.startswith("nfe_matched_manual_") and f.endswith(".csv")
        ], reverse=True)
        
        if not arquivos:
            print("[ERRO] Nenhum arquivo 'nfe_etapa08_matched_manual.csv' encontrado.")
            print("   Execute primeiro as Etapas 1-8 do pipeline.")
            return False
        
        arquivo_entrada = os.path.join(diretorio_dados, arquivos[0])
    tamanho_mb = os.path.getsize(arquivo_entrada) / (1024 * 1024)
    
    print(f"[OK] Arquivo encontrado:")
    print(f"   Nome: {os.path.basename(arquivo_entrada)}")
    print(f"   Tamanho: {tamanho_mb:.2f} MB")
    
    # ============================================================
    # 2. CARREGAR DADOS
    # ============================================================
    
    print(f"\n[INFO] Carregando dados...")
    try:
        df = pd.read_csv(arquivo_entrada, sep=';', encoding='utf-8-sig')
        print(f"   [OK] Carregado com sucesso!")
        print(f"   Shape: {df.shape}")
        print(f"   Memoria: {df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")
    except Exception as e:
        print(f"[ERRO] Erro ao carregar arquivo: {e}")
        return False
    
    # ============================================================
    # 3. PROCESSAR SEPARAÇÃO E FILTRAGEM
    # ============================================================
    
    try:
        df_completo, df_trabalhando = processar_separacao_e_filtragem(
            df=df,
            exportar=True,
            diretorio=diretorio_dados
        )
        
        if df_completo is None or df_trabalhando is None:
            print("❌ Erro no processamento. Verifique os logs acima.")
            return False
            
    except Exception as e:
        print(f"\n[ERRO] Erro durante o processamento: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # ============================================================
    # 4. RESUMO FINAL
    # ============================================================
    
    duracao_total = (datetime.now() - inicio_total).total_seconds()
    
    print("\n" + "="*80)
    print("[SUCESSO] ETAPA 9 CONCLUIDA!")
    print("="*80)
    print(f"\n[INFO] Resumo dos Resultados:")
    print(f"   df_completo (matched):      {len(df_completo):,} registros")
    print(f"   df_trabalhando (filtrado):  {len(df_trabalhando):,} registros")
    print(f"\n[INFO] Tempo total de execucao: {duracao_total:.2f}s")
    print("="*80)
    
    # Lista arquivos gerados
    print("\n[INFO] Arquivos gerados:")
    arquivos_gerados = sorted([
        f for f in os.listdir(diretorio_dados)
        if (f.startswith("df_completo_") or f.startswith("df_trabalhando_")) 
        and f.endswith(".zip")
    ], reverse=True)
    
    for arquivo in arquivos_gerados[:4]:  # Mostra ultimos 2 de cada
        caminho = os.path.join(diretorio_dados, arquivo)
        tamanho = os.path.getsize(caminho) / (1024 * 1024)
        print(f"   - {arquivo} ({tamanho:.2f} MB)")
    
    print("\n" + "="*80)
    
    return True


if __name__ == "__main__":
    sucesso = main()
    sys.exit(0 if sucesso else 1)
