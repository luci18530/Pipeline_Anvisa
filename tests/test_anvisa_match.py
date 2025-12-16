import pandas as pd

# Carregar bases
print("="*60)
print("TESTANDO MATCH NFE x ANVISA")
print("="*60)

# NFE
df_nfe = pd.read_csv('data/processed/nfe_etapa08_matched_manual.csv', sep=';')
print(f"\nNFE: {len(df_nfe)} registros")
print("EANs na NFE:", df_nfe['codigo_ean'].unique())
print("Registros ANVISA na NFE:", df_nfe['cod_anvisa'].unique())

# ANVISA
df_anv = pd.read_csv('data/processed/anvisa/base_anvisa_precos_vigencias.csv', sep=';')
print(f"\nANVISA: {len(df_anv):,} registros")
print("Colunas:", list(df_anv.columns)[:10])

# Buscar por EAN
eans_nfe = df_nfe['codigo_ean'].dropna().unique()
print(f"\nBuscando EANs da NFE na ANVISA...")
for ean in eans_nfe[:3]:
    ean_norm = str(ean).strip().zfill(13)
    match = df_anv[df_anv['EAN 1'].astype(str).str.strip().str.zfill(13) == ean_norm]
    print(f"  EAN {ean} -> {len(match)} matches")
    if len(match) > 0:
        print(f"    Produto: {match.iloc[0]['PRODUTO']}")

# Buscar PERTUZUMABE
print("\nBuscando PERTUZUMABE na ANVISA...")
perjeta = df_anv[df_anv['PRODUTO'].str.contains('PERTUZUMABE', case=False, na=False)]
print(f"  Encontrado: {len(perjeta)} registros")
if len(perjeta) > 0:
    print(f"  Primeiro EAN 1: {perjeta.iloc[0]['EAN 1']}")
    print(f"  Primeiro PRODUTO: {perjeta.iloc[0]['PRODUTO']}")
