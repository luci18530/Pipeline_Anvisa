import pandas as pd
import zipfile

# Verificar etapas consolidadas
etapas = {
    '09_completo': 'data/processed/df_etapa09_completo.zip',
    '13_apresentacao_unica': 'data/processed/df_etapa13_match_apresentacao_unica.zip',
    '16_matched_hibrido': 'data/processed/df_etapa16_matched_hibrido.zip',
    '17_consolidado_final': 'data/processed/df_etapa17_consolidado_final.zip'
}

print("="*70)
print("ANÁLISE DA EXPLOSÃO DE LINHAS NO PIPELINE NFe")
print("="*70)

for nome, caminho in etapas.items():
    try:
        with zipfile.ZipFile(caminho) as zf:
            csv_name = zf.namelist()[0]
            df = pd.read_csv(zf.open(csv_name), nrows=0)  # Só ler header
            
        # Agora ler contando linhas
        with zipfile.ZipFile(caminho) as zf:
            df_full = pd.read_csv(zf.open(csv_name), low_memory=False, on_bad_lines='skip')
            
        print(f"\n{nome}:")
        print(f"  Linhas: {len(df_full):,}")
        print(f"  Colunas: {len(df_full.columns)}")
        
        # Ver match_via se existir
        if 'match_via' in df_full.columns:
            print(f"\n  Distribuição match_via:")
            print(df_full['match_via'].value_counts().to_string().replace('\n', '\n    '))
            
    except Exception as e:
        print(f"\n{nome}: ERRO - {e}")

print("\n" + "="*70)
print("CONCLUSÃO")
print("="*70)
print("\n71 linhas input (nfe.csv) ->")
print("  - Etapa 09: matching por EAN")
print("  - Etapa 13: matching por apresentação única")
print("  - Etapa 16: matching híbrido")
print("  - Etapa 17: CONSOLIDAÇÃO de todas as etapas")
print("\nCada linha pode gerar múltiplas correspondências!")
