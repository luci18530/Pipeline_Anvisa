import pandas as pd

# Carregar ANVISA
df_anv = pd.read_csv('data/processed/anvisa/base_anvisa_precos_vigencias.csv', sep=';', dtype={'EAN 1': str, 'EAN 2': str, 'EAN 3': str})

# Buscar EAN problemático
ean_procurado = '7613326000598'
print(f"Procurando EAN: {ean_procurado}")
print(f"Comprimento: {len(ean_procurado)}")

# Buscar em todas as colunas de EAN
for col in ['EAN 1', 'EAN 2', 'EAN 3']:
    if col in df_anv.columns:
        df_anv[col] = df_anv[col].astype(str).str.strip()
        match = df_anv[df_anv[col] == ean_procurado]
        print(f"\n{col}: {len(match)} matches")
        
        # Tentar sem zeros à esquerda
        ean_sem_zeros = ean_procurado.lstrip('0')
        match2 = df_anv[df_anv[col] == ean_sem_zeros]
        print(f"{col} (sem zeros): {len(match2)} matches")
        
        # Tentar com 14 dígitos (0 + 13)
        ean_14 = '0' + ean_procurado
        match3 = df_anv[df_anv[col] == ean_14]
        print(f"{col} (14 dígitos): {len(match3)} matches")
        
        if len(match) > 0:
            print(f"  PRODUTO: {match.iloc[0]['PRODUTO']}")

# Buscar PERJETA para ver quais EANs tem
print("\n" + "="*60)
print("PRODUTOS 'PERJETA' NA ANVISA:")
perjeta = df_anv[df_anv['PRODUTO'].str.contains('PERJETA', case=False, na=False)]
print(f"Total: {len(perjeta)} registros")
print("\nEANs únicos:")
for col in ['EAN 1', 'EAN 2', 'EAN 3']:
    if col in perjeta.columns:
        eans = perjeta[col].dropna().unique()
        print(f"  {col}: {eans[:5]}")
