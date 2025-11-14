# -*- coding: utf-8 -*-
"""
TESTE DAS ETAPAS 15 E 16

Script para testar se as novas etapas estão configuradas corretamente.
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

print("="*80)
print("TESTE DE CONFIGURACAO - ETAPAS 15 E 16")
print("="*80)

# Verificar estrutura de pastas
print("\n1. Verificando estrutura de pastas...")
pastas = [
    BASE_DIR / 'data' / 'processed',
    BASE_DIR / 'output' / 'anvisa',
    BASE_DIR / 'src'
]

for pasta in pastas:
    status = "✓" if pasta.exists() else "✗"
    print(f"  {status} {pasta}")

# Verificar arquivos de entrada
print("\n2. Verificando arquivos de entrada...")
arquivos_entrada = [
    BASE_DIR / 'data' / 'processed' / 'df_etapa14_final_enriquecido.zip',
    BASE_DIR / 'output' / 'anvisa' / 'baseANVISA.csv'
]

for arquivo in arquivos_entrada:
    if arquivo.exists():
        tamanho = arquivo.stat().st_size / (1024 * 1024)
        print(f"  ✓ {arquivo.name} ({tamanho:.2f} MB)")
    else:
        print(f"  ✗ {arquivo.name} (NAO ENCONTRADO)")

# Verificar modulos
print("\n3. Verificando modulos Python...")
modulos = [
    ('src.nfe_matching_hibrido', 'nfe_matching_hibrido.py'),
    ('src.nfe_finalizacao_pipeline', 'nfe_finalizacao_pipeline.py')
]

sys.path.insert(0, str(BASE_DIR))

for nome_modulo, arquivo in modulos:
    arquivo_path = BASE_DIR / 'src' / arquivo
    if arquivo_path.exists():
        try:
            __import__(nome_modulo)
            print(f"  ✓ {arquivo}")
        except Exception as e:
            print(f"  ✗ {arquivo} (ERRO: {e})")
    else:
        print(f"  ✗ {arquivo} (NAO ENCONTRADO)")

# Verificar dependencias
print("\n4. Verificando dependencias...")
dependencias = [
    'pandas',
    'numpy',
    'rapidfuzz',
    'tqdm'
]

for dep in dependencias:
    try:
        __import__(dep)
        print(f"  ✓ {dep}")
    except ImportError:
        print(f"  ✗ {dep} (NAO INSTALADO)")

# Resumo
print("\n" + "="*80)
print("RESUMO")
print("="*80)

# Verificar se pode rodar etapa 15
etapa14_ok = (BASE_DIR / 'data' / 'processed' / 'df_etapa14_final_enriquecido.zip').exists()
base_ok = (BASE_DIR / 'output' / 'anvisa' / 'baseANVISA.csv').exists()

if etapa14_ok and base_ok:
    print("\n✓ PRONTO para executar Etapa 15")
    print(f"\n  Para rodar:")
    print(f"  python src/nfe_matching_hibrido.py")
else:
    print("\n✗ NAO PRONTO para Etapa 15")
    if not etapa14_ok:
        print("  -> Falta: df_etapa14_final_enriquecido.zip")
        print("     Execute: python src/nfe_extracao_ia.py")
    if not base_ok:
        print("  -> Falta: baseANVISA.csv")
        print("     Execute: python reprocessar_base_anvisa.py")

print("\n" + "="*80)
