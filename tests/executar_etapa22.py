#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para executar apenas a Etapa 22 (Particionamento QlikView) do pipeline NFe.
Usa os dados da Etapa 21 que já foram processados.
"""
import sys
from pathlib import Path

# Root do projeto
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "pipelines" / "nfe" / "src"))

print("=" * 80)
print("EXECUTANDO ETAPA 22 - PARTICIONAMENTO QLIKVIEW")
print("=" * 80)
print()

# Verificar se arquivo da etapa 21 existe
ETAPA_21 = PROJECT_ROOT / "data" / "processed" / "df_etapa21_unidades_padronizadas.zip"
if not ETAPA_21.exists():
    print("[ERRO] Arquivo da etapa 21 não encontrado!")
    print(f"  Esperado: {ETAPA_21}")
    print()
    print("Execute primeiro:")
    print("  python executar_etapa21.py")
    sys.exit(1)

print("[OK] Arquivo da etapa 21 encontrado")
print()

try:
    # Importar script da etapa 22
    import subprocess
    
    script_etapa22 = PROJECT_ROOT / "pipelines" / "nfe" / "scripts" / "processar_etapa22_particionamento.py"
    
    if not script_etapa22.exists():
        print(f"[ERRO] Script da etapa 22 não encontrado: {script_etapa22}")
        sys.exit(1)
    
    print("[INFO] Iniciando processamento da etapa 22...")
    print()
    
    resultado = subprocess.run(
        [sys.executable, str(script_etapa22)],
        capture_output=False,
        text=True
    )
    
    if resultado.returncode == 0:
        print()
        print("=" * 80)
        print("[OK] Etapa 22 concluída com sucesso!")
        print()
        print("Arquivos gerados:")
        print("  - QlikView/df_central.csv")
        print("  - QlikView/df_dosagem.csv")
        print("  - QlikView/df_registro_anvisa.csv")
        print("  - QlikView/df_entidades.csv")
        print("  - QlikView/df_valores_ajustados.csv")
        print("  - QlikView/df_chaves.csv")
        print("  - QlikView/df_eans.csv")
        print("  - QlikView/nfe_vencimento.csv")
        print("=" * 80)
        sys.exit(0)
    else:
        print()
        print("=" * 80)
        print("[ERRO] Etapa 22 falhou!")
        print("=" * 80)
        sys.exit(1)
        
except Exception as e:
    print()
    print("=" * 80)
    print(f"[ERRO] Falha ao executar etapa 22: {e}")
    print("=" * 80)
    import traceback
    traceback.print_exc()
    sys.exit(1)
