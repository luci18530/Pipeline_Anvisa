"""
Script de validação de dados de vencimento de NFe processados
"""

import pandas as pd
import sys
import os
import glob


def validar_dados_vencimento(arquivo_csv):
    """Valida dados de vencimento processados"""
    
    print("="*60)
    print("Validação de Dados de Vencimento NFe")
    print("="*60)
    print(f"Arquivo: {arquivo_csv}\n")
    
    # Carregar dados
    df = pd.read_csv(arquivo_csv, sep=';')
    
    # Converter colunas numéricas
    for col in ['vida_total', 'vida_usada', 'dias_restantes', 'vida_usada_porcento']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Converter colunas de data
    for col in ['dt_fabricacao', 'dt_validade', 'dt_emissao']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Validações
    validacoes = []
    
    # 1. Colunas esperadas
    colunas_esperadas = [
        'id_venc', 'dt_fabricacao', 'dt_validade', 'dt_emissao',
        'vida_total', 'vida_usada', 'dias_restantes',
        'vida_usada_porcento', 'categoria_vencimento'
    ]
    
    colunas_faltantes = set(colunas_esperadas) - set(df.columns)
    if not colunas_faltantes:
        validacoes.append(("[OK]", f"Todas as {len(colunas_esperadas)} colunas presentes"))
    else:
        validacoes.append(("[ERRO]", f"Colunas faltantes: {colunas_faltantes}"))
    
    # 2. Tipos de dados
    if pd.api.types.is_datetime64_any_dtype(df['dt_fabricacao']):
        validacoes.append(("[OK]", "dt_fabricacao é datetime"))
    else:
        validacoes.append(("[AVISO]", f"dt_fabricacao não é datetime: {df['dt_fabricacao'].dtype}"))
    
    if pd.api.types.is_datetime64_any_dtype(df['dt_validade']):
        validacoes.append(("[OK]", "dt_validade é datetime"))
    else:
        validacoes.append(("[AVISO]", f"dt_validade não é datetime: {df['dt_validade'].dtype}"))
    
    if pd.api.types.is_datetime64_any_dtype(df['dt_emissao']):
        validacoes.append(("[OK]", "dt_emissao é datetime"))
    else:
        validacoes.append(("[AVISO]", f"dt_emissao não é datetime: {df['dt_emissao'].dtype}"))
    
    if df['vida_usada_porcento'].dtype in ['float64', 'float32']:
        validacoes.append(("[OK]", "vida_usada_porcento é numérico"))
    else:
        validacoes.append(("[AVISO]", f"vida_usada_porcento não é numérico: {df['vida_usada_porcento'].dtype}"))
    
    # 3. Categorias válidas
    categorias_validas = {
        'VENCIDO', 'MUITO PROXIMO AO VENCIMENTO', 'PROXIMO AO VENCIMENTO',
        'PRAZO ACEITAVEL', 'INDETERMINADO'
    }
    
    categorias_unicas = set(df['categoria_vencimento'].unique())
    if categorias_unicas.issubset(categorias_validas):
        validacoes.append(("[OK]", f"Todas as categorias são válidas: {categorias_unicas}"))
    else:
        invalidas = categorias_unicas - categorias_validas
        validacoes.append(("[ERRO]", f"Categorias inválidas: {invalidas}"))
    
    # 4. Valores nulos em colunas críticas
    colunas_criticas = ['id_venc', 'dt_emissao', 'categoria_vencimento']
    for col in colunas_criticas:
        if col in df.columns:
            nulos = df[col].isna().sum()
            pct = nulos / len(df) * 100
            if pct < 1:
                validacoes.append(("[OK]", f"{col}: {nulos} nulos ({pct:.2f}%)"))
            else:
                validacoes.append(("[AVISO]", f"{col}: {nulos} nulos ({pct:.2f}%)"))
    
    # 5. Consistência de datas
    # Verificar se fabricação é antes de validade (quando ambas existem)
    mascara_ambas = df['dt_fabricacao'].notna() & df['dt_validade'].notna()
    fabricacao_depois_validade = (df.loc[mascara_ambas, 'dt_fabricacao'] > df.loc[mascara_ambas, 'dt_validade']).sum()
    
    if fabricacao_depois_validade == 0:
        validacoes.append(("[OK]", "Data fabricacao sempre <= data validade"))
    else:
        validacoes.append(("[AVISO]", f"{fabricacao_depois_validade} registros com fabricação > validade"))
    
    # 6. Registros totais
    if len(df) > 0:
        validacoes.append(("[OK]", f"Total de registros: {len(df):,}"))
    else:
        validacoes.append(("[ERRO]", "Dataset vazio!"))
    
    # 7. ID_VENC único
    duplicatas_id = df['id_venc'].duplicated().sum()
    if duplicatas_id == 0:
        validacoes.append(("[OK]", "Todos os id_venc são únicos"))
    else:
        validacoes.append(("[AVISO]", f"{duplicatas_id} duplicatas em id_venc"))
    
    # Exibir resultados
    print("Resultados das Validações:")
    print("-" * 60)
    
    erros = 0
    avisos = 0
    sucesso = 0
    
    for status, mensagem in validacoes:
        print(f"{status} {mensagem}")
        if status == "[ERRO]":
            erros += 1
        elif status == "[AVISO]":
            avisos += 1
        else:
            sucesso += 1
    
    # Resumo
    print("\n" + "="*60)
    print(f"Resumo: {sucesso} OK | {avisos} Avisos | {erros} Erros")
    print("="*60)
    
    # Distribuição de categorias
    print("\nDistribuição de Categorias:")
    dist = df['categoria_vencimento'].value_counts()
    for categoria, count in dist.items():
        pct = (count / len(df)) * 100
        print(f"  - {categoria:.<40} {count:>6,} ({pct:>5.1f}%)")
    
    # Estatísticas de dias restantes
    print("\nEstatísticas de Dias Restantes:")
    print(f"  - Mínimo: {df['dias_restantes'].min():>6.0f} dias")
    print(f"  - Q1: {df['dias_restantes'].quantile(0.25):>6.0f} dias")
    print(f"  - Mediana: {df['dias_restantes'].median():>6.0f} dias")
    print(f"  - Q3: {df['dias_restantes'].quantile(0.75):>6.0f} dias")
    print(f"  - Máximo: {df['dias_restantes'].max():>6.0f} dias")
    print(f"  - Média: {df['dias_restantes'].mean():>6.0f} dias")
    
    return erros == 0


if __name__ == "__main__":
    
    # Encontrar arquivo de vencimento mais recente
    arquivos = glob.glob("data/processed/nfe_vencimento_*.csv")
    
    if not arquivos:
        print("[ERRO] Nenhum arquivo de vencimento encontrado em data/processed/")
        print("[INFO] Execute primeiro: python scripts/processar_vencimento.py")
        sys.exit(1)
    
    # Pegar o mais recente
    arquivo_mais_recente = max(arquivos, key=os.path.getmtime)
    
    # Validar
    sucesso = validar_dados_vencimento(arquivo_mais_recente)
    
    if sucesso:
        print("\n[SUCESSO] Validação concluída sem erros!")
        sys.exit(0)
    else:
        print("\n[ERRO] Validação encontrou problemas!")
        sys.exit(1)
