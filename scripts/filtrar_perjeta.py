import pandas as pd

# Caminho do arquivo de entrada
ARQUIVO_ENTRADA = 'NOTAS_FISCAIS.csv'
# Caminho do arquivo de saída
ARQUIVO_SAIDA = 'NOTAS_FISCAIS_filtrado_perjeta.csv'

# Lê o arquivo CSV
try:
    df = pd.read_csv(ARQUIVO_ENTRADA, sep=None, engine='python')
except Exception as e:
    print(f'Erro ao ler o arquivo: {e}')
    exit(1)

# Filtra linhas onde a coluna 'descricao_produto' contém 'perjeta' (case-insensitive)
mask = df['descricao_produto'].str.contains('perjeta', case=False, na=False)
df_filtrado = df[mask]

# Salva o resultado
if not df_filtrado.empty:
    df_filtrado.to_csv(ARQUIVO_SAIDA, index=False)
    print(f'Arquivo salvo: {ARQUIVO_SAIDA} ({len(df_filtrado)} linhas)')
else:
    print('Nenhuma linha encontrada com o termo "perjeta" em descricao_produto.')
