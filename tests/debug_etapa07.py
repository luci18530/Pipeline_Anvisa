import sys
import os
from pathlib import Path
import pandas as pd

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "pipelines" / "nfe" / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "pipelines" / "anvisa_base" / "src"))

from nfe_etapa07_matching_anvisa import processar_matching_anvisa
from anvisa_base import processar_base_anvisa

def main():
    print("="*60)
    print("DEBUG ETAPA 07 - MATCHING ANVISA")
    print("="*60)

    # 1. Load NFe Sample
    nfe_path = PROJECT_ROOT / "data" / "processed" / "nfe_etapa04_enriquecido.csv"
    print(f"[INFO] Loading sample from {nfe_path}...")
    df_nfe = pd.read_csv(nfe_path, sep=';', encoding='utf-8-sig', nrows=10000)
    print(f"[OK] Loaded {len(df_nfe)} NFe records")

    # 2. Load ANVISA Base
    print("[INFO] Loading ANVISA base...")
    df_anvisa = processar_base_anvisa()
    print(f"[OK] Loaded {len(df_anvisa)} ANVISA records")
    
    # Check columns in ANVISA
    cols_check = ['GRUPO TERAPEUTICO', 'GRUPO ANATOMICO', 'STATUS']
    print("\n[CHECK] ANVISA Columns:")
    for col in cols_check:
        if col in df_anvisa.columns:
            nulls = df_anvisa[col].isna().sum()
            print(f"  - {col}: {len(df_anvisa)-nulls} valid, {nulls} nulls")
        else:
            print(f"  - {col}: MISSING!")

    # 3. Run Matching
    print("\n[INFO] Running matching...")
    df_matched = processar_matching_anvisa(df_nfe, df_anvisa)
    
    # 4. Analyze Results
    print("\n[RESULT] Matching Analysis:")
    print(f"Total records: {len(df_matched)}")
    
    # Check ID_CMED_PRODUTO_LIST (indicates match)
    matches = df_matched['ID_CMED_PRODUTO_LIST'].notna().sum()
    print(f"Matches found: {matches} ({matches/len(df_matched)*100:.2f}%)")
    
    print("\n[CHECK] Result Columns (for matched records):")
    df_only_matched = df_matched[df_matched['ID_CMED_PRODUTO_LIST'].notna()]
    
    if len(df_only_matched) > 0:
        for col in cols_check:
            if col in df_only_matched.columns:
                nulls = df_only_matched[col].isna().sum()
                print(f"  - {col}: {len(df_only_matched)-nulls} valid, {nulls} nulls ({nulls/len(df_only_matched)*100:.2f}% null)")
            else:
                print(f"  - {col}: MISSING!")
    else:
        print("No matches found to analyze.")

if __name__ == "__main__":
    main()
