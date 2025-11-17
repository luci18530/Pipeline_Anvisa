# -*- coding: utf-8 -*-
"""
VALIDAÇÃO COMPLETA DAS ETAPAS 14, 15 E 16
Verifica se as novas etapas foram integradas corretamente ao pipeline
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

print("="*80)
print("VALIDACAO COMPLETA - ETAPAS 14, 15 E 16 NO PIPELINE")
print("="*80)

# 1. Verificar módulos
print("\n1. Verificando módulos Python...")
modulos_esperados = [
    ('src/nfe_extracao_ia.py', 'Etapa 14: Extração IA'),
    ('src/nfe_matching_hibrido.py', 'Etapa 15: Matching Híbrido'),
    ('src/nfe_finalizacao_pipeline.py', 'Etapa 16: Finalização'),
    ('main_nfe.py', 'Pipeline Principal')
]

modulos_ok = 0
for arquivo, descricao in modulos_esperados:
    path = BASE_DIR / arquivo
    if path.exists():
        print(f"  ✓ {descricao:<40} {arquivo}")
        modulos_ok += 1
    else:
        print(f"  ✗ {descricao:<40} {arquivo} (NÃO ENCONTRADO)")

# 2. Verificar integração no main_nfe.py
print("\n2. Verificando integração no main_nfe.py...")
try:
    sys.path.insert(0, str(BASE_DIR))
    from main_nfe import PipelineNFe
    
    p = PipelineNFe()
    
    # Verificar métodos das etapas
    etapas_esperadas = [
        ('etapa_14_extracao_ia', 'Etapa 14: Extração IA'),
        ('etapa_15_matching_hibrido', 'Etapa 15: Matching Híbrido'),
        ('etapa_16_finalizacao_pipeline', 'Etapa 16: Finalização')
    ]
    
    etapas_ok = 0
    for metodo, descricao in etapas_esperadas:
        if hasattr(p, metodo):
            print(f"  ✓ Método '{metodo}' encontrado - {descricao}")
            etapas_ok += 1
        else:
            print(f"  ✗ Método '{metodo}' NÃO encontrado")
    
    # Contar total de etapas
    total_etapas = len([m for m in dir(p) if m.startswith('etapa_')])
    print(f"\n  → Total de etapas no pipeline: {total_etapas}")
    
except Exception as e:
    print(f"  ✗ Erro ao importar main_nfe.py: {e}")
    etapas_ok = 0
    total_etapas = 0

# 3. Verificar arquivos de entrada necessários
print("\n3. Verificando arquivos de entrada...")
arquivos_entrada = [
    ('output/anvisa/baseANVISA.csv', 'Base ANVISA'),
    ('support/dicionario_labs_para_revisao.csv', 'Dicionário Labs'),
    ('support/extracao_ia_medicamentos.csv', 'Resultados IA (cache)')
]

entrada_ok = 0
for arquivo, descricao in arquivos_entrada:
    path = BASE_DIR / arquivo
    if path.exists():
        tamanho = path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {descricao:<30} {arquivo} ({tamanho:.2f} MB)")
        entrada_ok += 1
    else:
        print(f"  ✗ {descricao:<30} {arquivo} (NÃO ENCONTRADO)")

# 4. Verificar outputs das etapas anteriores
print("\n4. Verificando outputs das etapas anteriores...")
outputs_esperados = [
    ('data/processed/df_etapa13_trabalhando_restante.zip', 'Input Etapa 14'),
]

outputs_ok = 0
for arquivo, descricao in outputs_esperados:
    path = BASE_DIR / arquivo
    if path.exists():
        tamanho = path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {descricao:<30} {arquivo} ({tamanho:.2f} MB)")
        outputs_ok += 1
    else:
        print(f"  ⚠ {descricao:<30} {arquivo} (NÃO ENCONTRADO - normal se não executou)")

# 5. Verificar outputs das novas etapas
print("\n5. Verificando outputs das novas etapas...")
outputs_novos = [
    ('data/processed/df_etapa14_extracao_ia.zip', 'Output Etapa 14a'),
    ('data/processed/df_etapa14_final_enriquecido.zip', 'Output Etapa 14b'),
    ('data/processed/df_etapa15_resultado_matching_hibrido.zip', 'Output Etapa 15'),
    ('data/processed/df_etapa16_matched_hibrido.zip', 'Output Etapa 16a'),
    ('data/processed/df_etapa16_restante.zip', 'Output Etapa 16b'),
    ('data/processed/df_etapa16_atributos_ia.zip', 'Output Etapa 16c')
]

novos_ok = 0
for arquivo, descricao in outputs_novos:
    path = BASE_DIR / arquivo
    if path.exists():
        tamanho = path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {descricao:<30} {arquivo} ({tamanho:.2f} MB)")
        novos_ok += 1
    else:
        print(f"  ⚠ {descricao:<30} {arquivo} (SERÁ CRIADO AO EXECUTAR)")

# 6. Verificar dependências
print("\n6. Verificando dependências críticas...")
deps = ['pandas', 'numpy', 'rapidfuzz', 'tqdm', 'zipfile']

deps_ok = 0
for dep in deps:
    try:
        if dep == 'zipfile':
            import zipfile
        else:
            __import__(dep)
        print(f"  ✓ {dep}")
        deps_ok += 1
    except ImportError:
        print(f"  ✗ {dep} (NÃO INSTALADO)")

# 7. Resumo final
print("\n" + "="*80)
print("RESUMO DA VALIDAÇÃO")
print("="*80)

print(f"\n✓ Módulos Python:              {modulos_ok}/{len(modulos_esperados)}")
print(f"✓ Métodos integrados:          {etapas_ok}/{len(etapas_esperadas)}")
print(f"✓ Total de etapas no pipeline: {total_etapas}/16")
print(f"✓ Arquivos de entrada:         {entrada_ok}/{len(arquivos_entrada)}")
print(f"✓ Dependências:                {deps_ok}/{len(deps)}")
print(f"⚠ Outputs gerados:             {novos_ok}/{len(outputs_novos)} (normal se ainda não executou)")

# Status final
tudo_ok = (
    modulos_ok == len(modulos_esperados) and
    etapas_ok == len(etapas_esperadas) and
    total_etapas == 16 and
    entrada_ok == len(arquivos_entrada) and
    deps_ok == len(deps)
)

print("\n" + "="*80)
if tudo_ok:
    print("✅ VALIDAÇÃO COMPLETA - PIPELINE PRONTO PARA EXECUTAR")
    print("\nPara executar o pipeline completo:")
    print("  python main_nfe.py")
    print("\nPara executar apenas as etapas 14-16:")
    print("  python src/nfe_extracao_ia.py")
    print("  python src/nfe_matching_hibrido.py")
    print("  python src/nfe_finalizacao_pipeline.py")
else:
    print("⚠ VALIDAÇÃO INCOMPLETA - VERIFICAR ITENS ACIMA")
    if modulos_ok < len(modulos_esperados):
        print("  → Módulos Python faltando")
    if etapas_ok < len(etapas_esperadas):
        print("  → Métodos não integrados ao main_nfe.py")
    if total_etapas != 16:
        print(f"  → Esperado 16 etapas, encontrado {total_etapas}")
    if entrada_ok < len(arquivos_entrada):
        print("  → Arquivos de entrada faltando")
    if deps_ok < len(deps):
        print("  → Dependências não instaladas")

print("="*80 + "\n")
