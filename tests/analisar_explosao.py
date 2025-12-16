import pandas as pd
import zipfile
import ast

# Carregar etapa 9 (após matching e explode, mas completo)
print("Carregando df_etapa09_completo...")
with zipfile.ZipFile('data/processed/df_etapa09_completo.zip') as zf:
    df = pd.read_csv(zf.open('df_etapa09_completo.csv'))

print(f"\nLinhas na etapa 6: {len(df):,}")
print(f"Linhas com ID_CMED_PRODUTO_LIST: {df['ID_CMED_PRODUTO_LIST'].notna().sum():,}")

# Contar tamanho das listas
def contar_ids(x):
    if pd.isna(x):
        return 0
    s = str(x).strip()
    if s.startswith('[') and s.endswith(']'):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return len(parsed)
        except:
            pass
        # Contar vírgulas + 1
        return s.count(',') + 1
    return 1

df['list_size'] = df['ID_CMED_PRODUTO_LIST'].apply(contar_ids)

print(f"\nTotal de linhas após explode: {df['list_size'].sum():,}")
print(f"\nDistribuição de tamanhos de lista:")
print(df['list_size'].value_counts().sort_index())

# Exemplos de listas grandes
print(f"\n\nExemplos de linhas com muitos IDs:")
grandes = df[df['list_size'] > 50].copy()
if len(grandes) > 0:
    for idx, row in grandes.head(3).iterrows():
        print(f"\n  descricao_produto: {row['descricao_produto'][:60]}")
        print(f"  match_via: {row['match_via']}")
        print(f"  Quantidade de IDs: {row['list_size']}")
        print(f"  IDs: {str(row['ID_CMED_PRODUTO_LIST'])[:150]}...")
