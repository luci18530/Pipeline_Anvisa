import pandas as pd
import zipfile

# Etapa 17 - Consolidado Final
zf = zipfile.ZipFile('data/processed/df_etapa17_consolidado_final.zip')
df = pd.read_csv(zf.open(zf.namelist()[0]), sep=';')

print(f'CONSOLIDADO FINAL (ETAPA 17):')
print(f'Registros: {len(df):,}')
print(f'Colunas: {len(df.columns)}')
print(f'\nColunas criticas:')
print(f'APRESENTACAO: {df["APRESENTACAO"].notna().sum():,}/{len(df):,} ({df["APRESENTACAO"].notna().sum()/len(df)*100:.1f}%)')
print(f'EAN_1: {df["EAN_1"].notna().sum():,}/{len(df):,} ({df["EAN_1"].notna().sum()/len(df)*100:.1f}%)')
print(f'PRODUTO: {df["PRODUTO"].notna().sum():,}/{len(df):,} ({df["PRODUTO"].notna().sum()/len(df)*100:.1f}%)')
print(f'LABORATORIO: {df["LABORATORIO"].notna().sum():,}/{len(df):,} ({df["LABORATORIO"].notna().sum()/len(df)*100:.1f}%)')
print(f'\nPrimeiras 3 linhas:')
print(df[['APRESENTACAO', 'EAN_1', 'PRODUTO', 'LABORATORIO']].head(3))
