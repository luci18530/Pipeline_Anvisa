import pandas as pd

df_anv = pd.read_csv('data/processed/anvisa/base_anvisa_precos_vigencias.csv', sep=';', dtype={'REGISTRO': str})

reg_procurado = '1010006570014'
print(f"Procurando REGISTRO: {reg_procurado}")

# Buscar registro
df_anv['REGISTRO'] = df_anv['REGISTRO'].astype(str).str.strip().str.replace(r'\D', '', regex=True)
match = df_anv[df_anv['REGISTRO'] == reg_procurado]

print(f"Matches: {len(match)}")
if len(match) > 0:
    print(f"\nProduto encontrado!")
    print(f"  PRODUTO: {match.iloc[0]['PRODUTO']}")
    print(f"  EAN 1: {match.iloc[0]['EAN 1']}")
    print(f"  id_produto: {match.iloc[0]['id_produto']}")
    print(f"\nTotal de vigências: {len(match)}")
else:
    print("\nNão encontrado. Testando variações...")
    # Tentar primeiros 13 dígitos
    reg_13 = reg_procurado[:13]
    match2 = df_anv[df_anv['REGISTRO'].str[:13] == reg_13]
    print(f"  Primeiros 13 dígitos: {len(match2)} matches")
