"""
Script para validar o preenchimento da coluna LABORATORIO em etapa13
"""
import pandas as pd
import zipfile

# Load etapa13
with zipfile.ZipFile('data/processed/df_etapa13_match_apresentacao_unica.zip', 'r') as z:
    with z.open('df_etapa13_match_apresentacao_unica.csv') as f:
        df = pd.read_csv(f, sep=';')

print('='*80)
print('VALIDACAO: LABORATORIO EM ETAPA 13')
print('='*80)
print(f'\nTotal de registros: {len(df):,}')
print()

if 'LABORATORIO' in df.columns:
    preenchido = df['LABORATORIO'].notna().sum()
    vazio = df['LABORATORIO'].isna().sum()
    pct = (preenchido / len(df)) * 100
    print(f'LABORATORIO preenchido: {preenchido:,} ({pct:.2f}%)')
    print(f'LABORATORIO vazio: {vazio:,} ({100-pct:.2f}%)')
    print()
    if pct >= 95:
        print(f'✓ META ATINGIDA! {pct:.2f}% >= 95%')
    else:
        print(f'✗ Meta não atingida. {pct:.2f}% < 95%')
    
    print(f'\n--- Amostra de registros com LABORATORIO: ---')
    sample = df[df['LABORATORIO'].notna()][['PRODUTO', 'LABORATORIO', 'APRESENTACAO']].head(5)
    for idx, row in sample.iterrows():
        print(f'  • {row["PRODUTO"][:50]} | Lab: {row["LABORATORIO"][:40]}')
    
    if vazio > 0:
        print(f'\n--- Amostra de registros SEM LABORATORIO: ---')
        sample_empty = df[df['LABORATORIO'].isna()][['PRODUTO', 'LABORATORIO', 'APRESENTACAO']].head(5)
        for idx, row in sample_empty.iterrows():
            print(f'  • {row["PRODUTO"][:50]} | Lab: {row["LABORATORIO"]}')
else:
    print('ERRO: Coluna LABORATORIO não encontrada!')
    print(f'Colunas disponíveis: {list(df.columns)}')
