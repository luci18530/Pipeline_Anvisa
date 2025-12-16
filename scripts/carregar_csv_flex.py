import pandas as pd
import sys

# Uso: python carregar_csv_flex.py caminho/para/arquivo.csv
if len(sys.argv) < 2:
    print("Uso: python carregar_csv_flex.py <arquivo.csv>")
    sys.exit(1)

ARQUIVO = sys.argv[1]

# Detecta o separador automaticamente
with open(ARQUIVO, 'r', encoding='utf-8-sig') as f:
    first_line = f.readline()
    if first_line.count(';') > first_line.count(','):
        sep = ';'
    else:
        sep = ','

try:
    df = pd.read_csv(ARQUIVO, sep=sep, encoding='utf-8-sig')
    print(f"Separador detectado: '{sep}'")
    print(f"Shape: {df.shape}")
    print(f"Colunas: {list(df.columns)}")
except Exception as e:
    print(f"Erro ao ler o arquivo: {e}")
    sys.exit(1)

# Exemplo: salva uma amostra
amostra = ARQUIVO.replace('.csv', '_amostra.csv')
df.head(10).to_csv(amostra, index=False)
print(f"Amostra salva em: {amostra}")
