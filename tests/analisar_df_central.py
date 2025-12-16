import pandas as pd

print("="*70)
print("ANÁLISE DO OUTPUT FINAL: QlikView/df_central.csv")
print("="*70)

# Ler arquivo final (separador ponto-e-vírgula)
df = pd.read_csv('QlikView/df_central.csv', sep=';', low_memory=False, on_bad_lines='skip')

print(f"\nTotal de linhas: {len(df):,}")
print(f"Total de colunas: {len(df.columns)}")

# Ver origem das linhas
if 'match_via' in df.columns:
    print(f"\n{'='*70}")
    print("DISTRIBUIÇÃO POR MÉTODO DE MATCHING")
    print("="*70)
    print(df['match_via'].value_counts().to_string())

# Verificar chaves únicas
if 'id_descricao' in df.columns:
    print(f"\n{'='*70}")
    print("ANÁLISE DE DUPLICAÇÃO")
    print("="*70)
    ids_unicos = df['id_descricao'].nunique()
    print(f"IDs únicos (id_descricao): {ids_unicos:,}")
    print(f"Total de linhas: {len(df):,}")
    print(f"Multiplicador médio: {len(df) / ids_unicos:.2f}x")
    
    # Ver quais IDs geraram mais linhas
    print(f"\n{'='*70}")
    print("TOP 10 PRODUTOS COM MAIS DUPLICAÇÕES")
    print("="*70)
    duplicacoes = df.groupby('id_descricao').size().sort_values(ascending=False).head(10)
    for id_desc, count in duplicacoes.items():
        produto = df[df['id_descricao'] == id_desc]['descricao_produto'].iloc[0]
        print(f"  {id_desc}: {count:,} linhas - {produto[:50]}")

# Verificar vigências
if 'VIG_INICIO' in df.columns and 'VIG_FIM' in df.columns:
    print(f"\n{'='*70}")
    print("ANÁLISE DE VIGÊNCIAS (múltiplos períodos de preço)")
    print("="*70)
    vigor = df[['id_descricao', 'VIG_INICIO', 'VIG_FIM']].groupby('id_descricao').size()
    print(f"Produtos com apenas 1 vigência: {(vigor == 1).sum():,}")
    print(f"Produtos com 2+ vigências: {(vigor > 1).sum():,}")
    print(f"Média de vigências por produto: {vigor.mean():.2f}")
