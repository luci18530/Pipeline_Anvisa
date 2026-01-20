"""
Script de Debug e Análise de Qualidade do df_central.csv

Gera relatório completo com:
- Contagem de nulos por coluna
- Distribuição de CHECK_EMISSAO_APOS_VIGENCIA
- Estatísticas descritivas
- Métricas de qualidade
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Configuração
QLIKVIEW_DIR = Path("QlikView")
INPUT_FILE = QLIKVIEW_DIR / "df_central.csv"
OUTPUT_DIR = QLIKVIEW_DIR / "debug"
OUTPUT_DIR.mkdir(exist_ok=True)

def carregar_dados():
    """Carrega df_central.csv usando chunks para evitar MemoryError."""
    print("\n" + "=" * 80)
    print("CARREGANDO df_central.csv")
    print("=" * 80)
    
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {INPUT_FILE}")
    
    # Carregar em chunks
    chunks = []
    chunk_size = 100_000
    for i, chunk in enumerate(pd.read_csv(INPUT_FILE, sep=";", low_memory=False, chunksize=chunk_size)):
        chunks.append(chunk)
        if (i + 1) % 10 == 0:
            print(f"[INFO] Carregados {(i + 1) * chunk_size:,} registros...")
    
    df = pd.concat(chunks, ignore_index=True)
    print(f"[OK] Total de registros: {len(df):,}")
    print(f"[OK] Total de colunas: {len(df.columns)}")
    return df

def analisar_nulos(df):
    """Analisa valores nulos por coluna."""
    print("\n" + "=" * 80)
    print("ANÁLISE DE VALORES NULOS")
    print("=" * 80)
    
    nulos = df.isnull().sum()
    total = len(df)
    pct_nulos = (nulos / total * 100).round(2)
    
    df_nulos = pd.DataFrame({
        'coluna': nulos.index,
        'nulos': nulos.values,
        'percentual': pct_nulos.values
    }).sort_values('nulos', ascending=False)
    
    # Salvar relatório completo
    output_file = OUTPUT_DIR / "nulos_por_coluna.csv"
    df_nulos.to_csv(output_file, sep=";", index=False, encoding='utf-8')
    print(f"[OK] Relatório salvo: {output_file.name}")
    
    # Mostrar top 20 colunas com mais nulos
    print(f"\nTOP 20 COLUNAS COM MAIS VALORES NULOS:")
    print("-" * 80)
    for _, row in df_nulos.head(20).iterrows():
        print(f"{row['coluna']:45s} {row['nulos']:>10,d} ({row['percentual']:>6.2f}%)")
    
    # Resumo
    colunas_com_nulos = (nulos > 0).sum()
    print(f"\n[RESUMO] {colunas_com_nulos} de {len(df.columns)} colunas possuem valores nulos")
    
    return df_nulos

def analisar_check_emissao_vigencia(df):
    """Analisa distribuição de CHECK_EMISSAO_APOS_VIGENCIA."""
    print("\n" + "=" * 80)
    print("ANÁLISE: CHECK_EMISSAO_APOS_VIGENCIA")
    print("=" * 80)
    
    if 'CHECK_EMISSAO_APOS_VIGENCIA' not in df.columns:
        print("[AVISO] Coluna CHECK_EMISSAO_APOS_VIGENCIA não encontrada")
        return
    
    col = df['CHECK_EMISSAO_APOS_VIGENCIA']
    
    # Contagem de valores
    contagem = col.value_counts(dropna=False).sort_index()
    total = len(df)
    
    print("\nDistribuição:")
    print("-" * 50)
    for valor, qtd in contagem.items():
        pct = (qtd / total * 100)
        if pd.isna(valor):
            print(f"  NULL/NaN:  {qtd:>12,d} ({pct:>6.2f}%)")
        else:
            label = "DENTRO DA VIGÊNCIA" if valor == 0 else "APÓS VIGÊNCIA"
            print(f"  {valor} ({label}):  {qtd:>12,d} ({pct:>6.2f}%)")
    
    # Salvar
    df_check = pd.DataFrame({
        'valor': contagem.index.astype(str),
        'quantidade': contagem.values,
        'percentual': (contagem.values / total * 100).round(2)
    })
    output_file = OUTPUT_DIR / "check_emissao_vigencia.csv"
    df_check.to_csv(output_file, sep=";", index=False, encoding='utf-8')
    print(f"\n[OK] Relatório salvo: {output_file.name}")

def analisar_classe_valor(df):
    """Analisa distribuição de CLASSE_VALOR."""
    print("\n" + "=" * 80)
    print("ANÁLISE: CLASSE_VALOR (Classificação de Sobrepreço)")
    print("=" * 80)
    
    if 'CLASSE_VALOR' not in df.columns:
        print("[AVISO] Coluna CLASSE_VALOR não encontrada")
        return
    
    col = df['CLASSE_VALOR']
    contagem = col.value_counts(dropna=False)
    total = len(df)
    
    print("\nDistribuição:")
    print("-" * 80)
    for valor, qtd in contagem.items():
        pct = (qtd / total * 100)
        if pd.isna(valor):
            print(f"  NULL/NaN:                          {qtd:>12,d} ({pct:>6.2f}%)")
        else:
            print(f"  {str(valor):35s}  {qtd:>12,d} ({pct:>6.2f}%)")
    
    # Salvar
    df_classe = pd.DataFrame({
        'classe': contagem.index.astype(str),
        'quantidade': contagem.values,
        'percentual': (contagem.values / total * 100).round(2)
    })
    output_file = OUTPUT_DIR / "classe_valor_distribuicao.csv"
    df_classe.to_csv(output_file, sep=";", index=False, encoding='utf-8')
    print(f"\n[OK] Relatório salvo: {output_file.name}")

def analisar_esfera(df):
    """Analisa distribuição de ID_ESFERA."""
    print("\n" + "=" * 80)
    print("ANÁLISE: ID_ESFERA (Esfera Administrativa)")
    print("=" * 80)
    
    if 'ID_ESFERA' not in df.columns:
        print("[AVISO] Coluna ID_ESFERA não encontrada")
        return
    
    col = df['ID_ESFERA']
    contagem = col.value_counts(dropna=False).sort_index()
    total = len(df)
    
    print("\nDistribuição:")
    print("-" * 50)
    for valor, qtd in contagem.items():
        pct = (qtd / total * 100)
        if pd.isna(valor):
            print(f"  NULL/NaN:         {qtd:>12,d} ({pct:>6.2f}%)")
        else:
            label = "MUNICIPAL" if valor == 1 else "ESTADUAL" if valor == 2 else "DESCONHECIDO"
            print(f"  {int(valor)} ({label}):  {qtd:>12,d} ({pct:>6.2f}%)")

def analisar_valores_financeiros(df):
    """Analisa estatísticas de valores financeiros."""
    print("\n" + "=" * 80)
    print("ANÁLISE: VALORES FINANCEIROS")
    print("=" * 80)
    
    colunas_valor = ['valor_produtos', 'valor_unitario', 'quantidade', 'TETO_DE_PRECO', 'RAZAO_VALOR_TETO']
    
    for col in colunas_valor:
        if col not in df.columns:
            print(f"\n[AVISO] Coluna {col} não encontrada")
            continue
        
        dados = pd.to_numeric(df[col], errors='coerce')
        
        print(f"\n{col}:")
        print("-" * 50)
        print(f"  Valores válidos: {dados.notna().sum():,} ({dados.notna().sum()/len(df)*100:.2f}%)")
        print(f"  Valores nulos:   {dados.isna().sum():,} ({dados.isna().sum()/len(df)*100:.2f}%)")
        
        if dados.notna().sum() > 0:
            print(f"  Mínimo:          {dados.min():,.4f}")
            print(f"  Máximo:          {dados.max():,.4f}")
            print(f"  Média:           {dados.mean():,.4f}")
            print(f"  Mediana:         {dados.median():,.4f}")
            print(f"  Soma total:      R$ {dados.sum():,.2f}")

def analisar_datas(df):
    """Analisa range de datas."""
    print("\n" + "=" * 80)
    print("ANÁLISE: DATAS")
    print("=" * 80)
    
    colunas_data = ['data_emissao', 'data_emissao_original', 'VIG_FIM_ANVISA']
    
    for col in colunas_data:
        if col not in df.columns:
            continue
        
        try:
            datas = pd.to_datetime(df[col], errors='coerce')
            validas = datas.notna().sum()
            
            print(f"\n{col}:")
            print("-" * 50)
            print(f"  Datas válidas: {validas:,} ({validas/len(df)*100:.2f}%)")
            
            if validas > 0:
                print(f"  Data mínima:   {datas.min()}")
                print(f"  Data máxima:   {datas.max()}")
                print(f"  Range:         {(datas.max() - datas.min()).days} dias")
        except:
            print(f"\n{col}: [ERRO ao processar]")

def analisar_duplicatas(df):
    """Analisa registros duplicados."""
    print("\n" + "=" * 80)
    print("ANÁLISE: DUPLICATAS")
    print("=" * 80)
    
    # Duplicatas completas
    duplicatas_completas = df.duplicated().sum()
    print(f"\nRegistros completamente duplicados: {duplicatas_completas:,}")
    
    # Duplicatas por chave (se existir coluna 'id')
    if 'id' in df.columns:
        duplicatas_id = df['id'].duplicated().sum()
        print(f"IDs duplicados: {duplicatas_id:,}")
    
    # Duplicatas por chave_codigo + id_descricao
    if 'chave_codigo' in df.columns and 'id_descricao' in df.columns:
        duplicatas_chave = df.duplicated(subset=['chave_codigo', 'id_descricao']).sum()
        print(f"Duplicatas por chave_codigo + id_descricao: {duplicatas_chave:,}")

def analisar_campos_criticos(df):
    """Analisa completude de campos críticos."""
    print("\n" + "=" * 80)
    print("ANÁLISE: CAMPOS CRÍTICOS (Completude)")
    print("=" * 80)
    
    campos_criticos = [
        'PRINCIPIO ATIVO',
        'PRODUTO',
        'LABORATORIO',
        'TETO_DE_PRECO',
        'cod_anvisa',
        'municipio',
        'ID_ESFERA'
    ]
    
    print("\nCompletude dos campos críticos:")
    print("-" * 80)
    
    resultados = []
    for campo in campos_criticos:
        if campo in df.columns:
            nulos = df[campo].isna().sum()
            preenchidos = len(df) - nulos
            pct_preenchido = (preenchidos / len(df) * 100)
            
            status = "✓" if pct_preenchido >= 95 else "⚠" if pct_preenchido >= 80 else "✗"
            print(f"  {status} {campo:30s} {preenchidos:>10,d} / {len(df):,} ({pct_preenchido:>6.2f}%)")
            
            resultados.append({
                'campo': campo,
                'preenchidos': preenchidos,
                'nulos': nulos,
                'percentual_preenchido': round(pct_preenchido, 2)
            })
        else:
            print(f"  ? {campo:30s} [NÃO ENCONTRADO]")
    
    # Salvar
    if resultados:
        df_criticos = pd.DataFrame(resultados)
        output_file = OUTPUT_DIR / "campos_criticos.csv"
        df_criticos.to_csv(output_file, sep=";", index=False, encoding='utf-8')
        print(f"\n[OK] Relatório salvo: {output_file.name}")

def gerar_resumo_geral(df):
    """Gera resumo geral do dataset."""
    print("\n" + "=" * 80)
    print("RESUMO GERAL")
    print("=" * 80)
    
    resumo = {
        'Arquivo': INPUT_FILE.name,
        'Data Análise': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Total Registros': len(df),
        'Total Colunas': len(df.columns),
        'Tamanho Memória (MB)': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
        'Registros Duplicados': df.duplicated().sum(),
        'Colunas com Nulos': df.isnull().any().sum(),
        'Células Totais': len(df) * len(df.columns),
        'Células Nulas': df.isnull().sum().sum(),
        'Taxa Preenchimento (%)': round((1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100, 2)
    }
    
    print("\nMétricas Gerais:")
    print("-" * 80)
    for chave, valor in resumo.items():
        print(f"  {chave:30s} {str(valor):>20s}")
    
    # Salvar
    df_resumo = pd.DataFrame([resumo]).T.reset_index()
    df_resumo.columns = ['metrica', 'valor']
    output_file = OUTPUT_DIR / "resumo_geral.csv"
    df_resumo.to_csv(output_file, sep=";", index=False, encoding='utf-8')
    print(f"\n[OK] Resumo salvo: {output_file.name}")

def main():
    print("\n" + "=" * 80)
    print("DEBUG E ANÁLISE DE QUALIDADE - df_central.csv")
    print("=" * 80)
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Carregar dados
        df = carregar_dados()
        
        # Análises
        gerar_resumo_geral(df)
        analisar_nulos(df)
        analisar_check_emissao_vigencia(df)
        analisar_classe_valor(df)
        analisar_esfera(df)
        analisar_valores_financeiros(df)
        analisar_datas(df)
        analisar_duplicatas(df)
        analisar_campos_criticos(df)
        
        print("\n" + "=" * 80)
        print("✅ ANÁLISE CONCLUÍDA COM SUCESSO!")
        print("=" * 80)
        print(f"\nRelatórios salvos em: {OUTPUT_DIR}")
        print("\nArquivos gerados:")
        for arquivo in sorted(OUTPUT_DIR.glob("*.csv")):
            tamanho = arquivo.stat().st_size / 1024
            print(f"  - {arquivo.name} ({tamanho:.1f} KB)")
        
        return True
        
    except Exception as e:
        print(f"\n[ERRO] Falha na análise: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
