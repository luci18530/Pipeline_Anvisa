"""
Script de validação de dados NFe processados
"""

import pandas as pd
import sys
import os

# Adicionar src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def validar_dados_nfe(arquivo_csv):
    """Valida dados processados de NFe"""
    
    print("="*60)
    print("Validação de Dados NFe")
    print("="*60)
    print(f"Arquivo: {arquivo_csv}\n")
    
    # Carregar dados
    df = pd.read_csv(arquivo_csv, sep=';', encoding='utf-8')
    df.columns = [c.replace('\ufeff', '').strip() for c in df.columns]
    
    # Converter colunas numéricas
    for col in ['valor_produtos', 'valor_unitario', 'quantidade', 'ano_emissao', 'mes_emissao']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Converter colunas de data
    for col in ['data_emissao', 'data_emissao_original']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Validações
    validacoes = []
    
    # 1. Colunas esperadas
    colunas_esperadas = [
        'id_descricao', 'descricao_produto', 'cod_anvisa',
        'codigo_municipio_destinatario', 'data_emissao', 'codigo_ncm', 'codigo_ean',
        'valor_produtos', 'valor_unitario', 'quantidade', 'unidade',
        'cpf_cnpj_emitente', 'chave_codigo', 'cpf_cnpj', 'razao_social_emitente',
        'nome_fantasia_emitente', 'razao_social_destinatario', 'nome_fantasia_destinatario',
        'id_data_fabricacao', 'id_data_validade', 'data_emissao_original',
        'ano_emissao', 'mes_emissao'
    ]
    
    colunas_faltantes = set(colunas_esperadas) - set(df.columns)
    if not colunas_faltantes:
        validacoes.append(("[OK]", f"Todas as {len(colunas_esperadas)} colunas presentes"))
    else:
        validacoes.append(("[ERRO]", f"Colunas faltantes: {colunas_faltantes}"))
    
    # 2. Tipos de dados
    if df['valor_produtos'].dtype in ['float64', 'float32']:
        validacoes.append(("[OK]", "valor_produtos é numérico"))
    else:
        validacoes.append(("[ERRO]", f"valor_produtos não é numérico: {df['valor_produtos'].dtype}"))
    
    if pd.api.types.is_datetime64_any_dtype(df['data_emissao']):
        validacoes.append(("[OK]", "data_emissao é datetime"))
    else:
        validacoes.append(("[ERRO]", f"data_emissao não é datetime: {df['data_emissao'].dtype}"))
    
    # 3. Valores nulos em colunas críticas
    colunas_criticas = ['descricao_produto', 'data_emissao', 'valor_produtos', 'quantidade']
    for col in colunas_criticas:
        if col in df.columns:
            nulos = df[col].isna().sum()
            pct = nulos / len(df) * 100
            if pct < 5:
                validacoes.append(("[OK]", f"{col}: {nulos} nulos ({pct:.2f}%)"))
            elif pct < 20:
                validacoes.append(("[AVISO]", f"{col}: {nulos} nulos ({pct:.2f}%)"))
            else:
                validacoes.append(("[ERRO]", f"{col}: {nulos} nulos ({pct:.2f}%)"))
    
    # 4. Valores negativos
    if (df['valor_produtos'] < 0).any():
        validacoes.append(("[AVISO]", "Existem valores_produtos negativos"))
    else:
        validacoes.append(("[OK]", "Nenhum valor_produtos negativo"))
    
    if (df['quantidade'] < 0).any():
        validacoes.append(("[AVISO]", "Existem quantidades negativas"))
    else:
        validacoes.append(("[OK]", "Nenhuma quantidade negativa"))
    
    # 5. Datas válidas
    if 'ano_emissao' in df.columns:
        anos = df['ano_emissao'].dropna().unique()
        if all(2020 <= ano <= 2030 for ano in anos):
            validacoes.append(("[OK]", f"Anos válidos: {sorted(anos)}"))
        else:
            validacoes.append(("[AVISO]", f"Anos fora do esperado: {sorted(anos)}"))
    
    # 6. Duplicatas
    duplicatas = df.duplicated(subset=['chave_codigo', 'descricao_produto']).sum()
    if duplicatas == 0:
        validacoes.append(("[OK]", "Nenhuma duplicata de chave+produto"))
    else:
        validacoes.append(("[AVISO]", f"{duplicatas} possíveis duplicatas"))
    
    # 7. Registros totais
    if len(df) > 0:
        validacoes.append(("[OK]", f"Total de registros: {len(df):,}"))
    else:
        validacoes.append(("[ERRO]", "Dataset vazio!"))
    
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
    
    # Estatísticas adicionais
    print("\nEstatísticas Adicionais:")
    print(f"  - Período: {df['data_emissao'].min()} a {df['data_emissao'].max()}")
    print(f"  - Valor total: R$ {df['valor_produtos'].sum():,.2f}")
    print(f"  - Quantidade total: {df['quantidade'].sum():,.0f}")
    print(f"  - Emitentes únicos: {df['cpf_cnpj_emitente'].nunique():,}")
    print(f"  - Produtos únicos: {df['descricao_produto'].nunique():,}")
    
    return erros == 0


if __name__ == "__main__":
    import glob
    
    # Encontrar arquivo processado mais recente
    arquivos = glob.glob("data/processed/nfe_etapa01_processado.csv")
    
    if not arquivos:
        print("[ERRO] Nenhum arquivo processado encontrado em data/processed/")
        print("[INFO] Execute primeiro: python scripts/processar_nfe.py")
        sys.exit(1)
    
    # Pegar o mais recente
    arquivo_mais_recente = max(arquivos, key=os.path.getmtime)
    
    # Validar
    sucesso = validar_dados_nfe(arquivo_mais_recente)
    
    if sucesso:
        print("\n[SUCESSO] Validação concluída sem erros!")
        sys.exit(0)
    else:
        print("\n[ERRO] Validação encontrou problemas!")
        sys.exit(1)
