#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para executar apenas a Etapa 21 (Padronização de Unidades) do pipeline NFe.
Usa os dados da Etapa 20 que já foram processados.
"""
import sys
from pathlib import Path

# Root do projeto
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "pipelines" / "nfe" / "src"))

print("=" * 80)
print("EXECUTANDO ETAPA 21 - PADRONIZAÇÃO DE UNIDADES")
print("=" * 80)
print()

# Verificar se arquivo da etapa 20 existe
ETAPA_20 = PROJECT_ROOT / "data" / "processed" / "df_etapa20_classificacao_esfera.zip"
if not ETAPA_20.exists():
    print("[ERRO] Arquivo da etapa 20 não encontrado!")
    print(f"  Esperado: {ETAPA_20}")
    print()
    print("Execute o pipeline completo primeiro:")
    print("  python 3_pipeline_nfe.py")
    sys.exit(1)

print("[OK] Arquivo da etapa 20 encontrado")
print()

try:
    from pipelines.nfe.src.nfe_etapa21_padronizacao_unidades import main as executar_etapa21
    
    print("[INFO] Iniciando processamento da etapa 21...")
    print("[INFO] Esta etapa processa 2,6 milhões de registros com otimizações de memória")
    print()
    
    sucesso = executar_etapa21()
    
    if sucesso:
        print()
        print("=" * 80)
        print("[OK] Etapa 21 concluída com sucesso!")
        print()
        print("Arquivos gerados:")
        print("  - data/processed/df_etapa21_unidades_padronizadas.zip")
        print("  - data/processed/df_etapa21_unidades_resumo.csv")
        print("  - data/processed/df_etapa21_unidades_metricas.csv")
        print("=" * 80)
        sys.exit(0)
    else:
        print()
        print("=" * 80)
        print("[ERRO] Etapa 21 falhou!")
        print("=" * 80)
        sys.exit(1)
        
except Exception as e:
    print()
    print("=" * 80)
    print(f"[ERRO] Falha ao executar etapa 21: {e}")
    print("=" * 80)
    import traceback
    traceback.print_exc()
    sys.exit(1)
