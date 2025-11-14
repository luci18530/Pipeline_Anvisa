"""Diagnóstico completo das colunas APRESENTACAO/EAN ao longo do pipeline."""

from pathlib import Path
import pandas as pd
import zipfile


COLUNAS_CHAVE = ['APRESENTACAO', 'EAN_1', 'PRODUTO', 'LABORATORIO', 'PRINCIPIO_ATIVO']


def detectar_separador(filepath):
    """Tenta detectar automaticamente o separador do arquivo."""
    candidatos = ['\t', ';', ',', '|']
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            linhas = [next(f, '') for _ in range(5)]
    except FileNotFoundError:
        return ';'
    texto = ''.join(linhas)
    if not texto:
        return ';'
    contagens = {sep: texto.count(sep) for sep in candidatos}
    melhor = max(contagens, key=contagens.get)
    return melhor if contagens[melhor] > 0 else ';'


def carregar_dataframe(path: str) -> pd.DataFrame:
    """Carrega CSV ou ZIP detectando separador automaticamente."""
    if path.endswith('.zip'):
        with zipfile.ZipFile(path) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                return pd.read_csv(f, sep=';', dtype=str, low_memory=False)
    sep = detectar_separador(path)
    return pd.read_csv(
        path,
        sep=sep,
        dtype=str,
        engine='python',
        on_bad_lines='warn'
    )


def analisar_arquivo(path, nome):
    """Analisa um arquivo e retorna estatísticas de colunas."""
    try:
        df = carregar_dataframe(path)
        
        print(f"\n{'='*80}")
        print(f"=== {nome.upper()} ===")
        print(f"{'='*80}")
        print(f"Arquivo: {path}")
        print(f"Total de registros: {len(df):,}")
        print(f"Total de colunas: {len(df.columns)}")
        
        # Verificar colunas específicas
        print(f"\n--- Colunas Importantes ---")
        for col in COLUNAS_CHAVE:
            if col in df.columns:
                total = len(df)
                nulos = df[col].isna().sum()
                preenchidos = df[col].notna().sum()
                vazios = (df[col] == '').sum() if df[col].dtype == 'object' else 0
                pct_preenchido = (preenchidos/total)*100 if total > 0 else 0
                print(f"{col:20s}: {preenchidos:6,}/{total:6,} ({pct_preenchido:5.1f}% preenchido) | {nulos:6,} nulos | {vazios:6,} vazios")
            else:
                print(f"{col:20s}: *** COLUNA NÃO EXISTE ***")
        
        # Mostrar algumas linhas de exemplo
        print(f"\n--- Amostra de Dados ---")
        colunas_exibir = [c for c in COLUNAS_CHAVE if c in df.columns][:5]
        if colunas_exibir:
            print(df[colunas_exibir].head(3).to_string())
        
        return df
        
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"ERRO ao analisar {nome}: {str(e)}")
        print(f"{'='*80}")
        return None

# Analisar base ANVISA primeiro
print("\n" + "="*80)
print("DIAGNÓSTICO DE COLUNAS NO PIPELINE NFE")
print("="*80)

base_anvisa_path = 'output/anvisa/baseANVISA.csv'
df_anvisa = analisar_arquivo(base_anvisa_path, 'Base ANVISA (Origem)')

# Analisar etapas do pipeline
etapas = {
    'Etapa 09 - Completo': 'data/processed/df_etapa09_completo.zip',
    'Etapa 09 - Trabalhando': 'data/processed/df_etapa09_trabalhando.zip',
    'Etapa 13 - Trabalhando Restante': 'data/processed/df_etapa13_trabalhando_restante.zip',
    'Etapa 13 - Match Apresentação': 'data/processed/df_etapa13_match_apresentacao_unica.zip',
    'Etapa 14 - Enriquecido': 'data/processed/df_etapa14_final_enriquecido.zip',
    'Etapa 15 - Matching Híbrido': 'data/processed/df_etapa15_resultado_matching_hibrido.zip',
    'Etapa 16 - Restante': 'data/processed/df_etapa16_restante.zip',
    'Etapa 17 - Consolidado Final': 'data/processed/df_etapa17_consolidado_final.zip',
}

dfs = {}
for nome, path in etapas.items():
    if Path(path).exists():
        dfs[nome] = analisar_arquivo(path, nome)
    else:
        print(f"\n{'='*80}")
        print(f"ARQUIVO NÃO ENCONTRADO: {nome}")
        print(f"Path: {path}")
        print(f"{'='*80}")

# Análise específica do Etapa 16
print("\n" + "="*80)
print("=== ANÁLISE DETALHADA: ETAPA 16 (RESTANTE) ===")
print("="*80)

df16 = dfs.get('Etapa 16 - Restante')
if df16 is not None:
    if 'APRESENTACAO' in df16.columns:
        print(f"\nRegistros sem APRESENTACAO: {df16['APRESENTACAO'].isna().sum():,}")
        print("\n--- Exemplos de registros SEM APRESENTACAO ---")
        sem_apresentacao = df16[df16['APRESENTACAO'].isna()].head(5)
        if len(sem_apresentacao) > 0:
            colunas_debug = ['PRODUTO', 'LABORATORIO', 'EAN_1', 'APRESENTACAO']
            colunas_debug = [c for c in colunas_debug if c in df16.columns]
            print(sem_apresentacao[colunas_debug].to_string())
    else:
        print("Coluna APRESENTACAO inexistente nesta etapa.")

    if 'EAN_1' in df16.columns:
        print(f"\nRegistros sem EAN_1: {df16['EAN_1'].isna().sum():,}")
        print("\n--- Exemplos de registros SEM EAN_1 ---")
        sem_ean = df16[df16['EAN_1'].isna()].head(5)
        if len(sem_ean) > 0:
            colunas_debug = ['PRODUTO', 'LABORATORIO', 'EAN_1', 'APRESENTACAO']
            colunas_debug = [c for c in colunas_debug if c in df16.columns]
            print(sem_ean[colunas_debug].to_string())
    else:
        print("Coluna EAN_1 inexistente nesta etapa.")
else:
    print("Etapa 16 - Restante não encontrada para análise detalhada.")

print("\n" + "="*80)
print("DIAGNÓSTICO CONCLUÍDO")
print("="*80)
