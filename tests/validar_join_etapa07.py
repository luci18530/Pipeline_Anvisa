import pandas as pd
import sys
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
FILE_PATH = PROJECT_ROOT / "data" / "processed" / "nfe_etapa07_matched.csv"

def main():
    print("="*80)
    print("VALIDAÇÃO DO JOIN (ETAPA 07)")
    print("="*80)
    
    if not FILE_PATH.exists():
        print(f"[ERRO] Arquivo não encontrado: {FILE_PATH}")
        sys.exit(1)
        
    print(f"[INFO] Carregando {FILE_PATH}...")
    # Ler apenas colunas relevantes para economizar memória e tempo
    cols = [
        'ID_CMED_PRODUTO_LIST', 
        'GRUPO TERAPEUTICO', 
        'GRUPO ANATOMICO', 
        'STATUS', 
        'PRODUTO', 
        'PRINCIPIO ATIVO'
    ]
    
    try:
        df = pd.read_csv(FILE_PATH, sep=';', usecols=lambda c: c in cols, encoding='utf-8-sig')
    except ValueError:
        # Fallback se alguma coluna não existir
        print("[AVISO] Algumas colunas não foram encontradas. Lendo todas...")
        df = pd.read_csv(FILE_PATH, sep=';', encoding='utf-8-sig')
    
    total = len(df)
    print(f"[INFO] Total de registros: {total:,}")
    
    # Filtrar apenas os que deram match (tem ID_CMED)
    # ID_CMED_PRODUTO_LIST é a chave que indica sucesso no join
    if 'ID_CMED_PRODUTO_LIST' in df.columns:
        matched = df[df['ID_CMED_PRODUTO_LIST'].notna()]
    else:
        print("[ERRO] Coluna ID_CMED_PRODUTO_LIST não encontrada! O join falhou estruturalmente.")
        sys.exit(1)
        
    count_matched = len(matched)
    perc_matched = (count_matched / total) * 100 if total > 0 else 0
    
    print(f"[INFO] Registros com Match (ID_CMED identificado): {count_matched:,} ({perc_matched:.2f}%)")
    print("-" * 80)
    
    if count_matched == 0:
        print("[ERRO] Nenhum match encontrado! Algo está muito errado.")
        sys.exit(1)
        
    # Analisar nulos APENAS nos registros que deram match
    print("ANÁLISE DE NULOS NOS REGISTROS COM MATCH:")
    cols_check = ['GRUPO TERAPEUTICO', 'GRUPO ANATOMICO', 'STATUS']
    
    all_good = True
    for col in cols_check:
        if col not in matched.columns:
            print(f"  [ERRO] Coluna {col} NÃO EXISTE no arquivo!")
            all_good = False
            continue
            
        nulls = matched[col].isna().sum()
        valid = count_matched - nulls
        perc_null = (nulls / count_matched) * 100
        
        status_icon = "[OK]" if nulls == 0 else "[ALERTA]"
        if perc_null > 50: status_icon = "[ERRO]"
        
        print(f"  {status_icon} {col:<20}: {valid:,} preenchidos | {nulls:,} nulos ({perc_null:.2f}%)")
        
        if nulls > 0:
            all_good = False

    print("-" * 80)
    
    if all_good:
        print("\n[CONCLUSÃO] O JOIN FUNCIONOU PERFEITAMENTE! AS COLUNAS ESTÃO PREENCHIDAS.")
        
        print("\nAMOSTRA DE DADOS (5 registros):")
        print(matched[cols_check + ['PRODUTO']].head().to_string())
    else:
        print("\n[CONCLUSÃO] O JOIN AINDA TEM PROBLEMAS DE DADOS FALTANTES.")

if __name__ == "__main__":
    main()
