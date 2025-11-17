"""
Script de validação de dados enriquecidos de NFe
"""

import pandas as pd
import sys
import os
import glob


def validar_dados_enriquecidos(arquivo_csv):
    """Valida dados enriquecidos processados"""
    
    print("="*60)
    print("Validação de Dados Enriquecidos de NFe")
    print("="*60)
    print(f"Arquivo: {arquivo_csv}\n")
    
    # Carregar dados
    df = pd.read_csv(arquivo_csv, sep=';')
    
    # Validações
    validacoes = []
    
    # 1. Colunas críticas presentes
    colunas_criticas = ['municipio', 'codigo_municipio_destinatario', 'descricao_produto']
    colunas_faltantes = set(colunas_criticas) - set(df.columns)
    if not colunas_faltantes:
        validacoes.append(("[OK]", f"Todas as colunas críticas presentes"))
    else:
        validacoes.append(("[ERRO]", f"Colunas faltantes: {colunas_faltantes}"))
    
    # 2. Municípios preenchidos
    if 'municipio' in df.columns:
        municipios_vazios = df['municipio'].isna().sum()
        municipios_vazios += (df['municipio'].astype(str).str.strip() == '').sum()
        municipios_vazios += (df['municipio'].astype(str).str.upper() == 'NAN').sum()
        
        pct_preenchidos = ((len(df) - municipios_vazios) / len(df)) * 100
        
        if pct_preenchidos >= 95:
            validacoes.append(("[OK]", f"{pct_preenchidos:.1f}% de municípios preenchidos"))
        elif pct_preenchidos >= 80:
            validacoes.append(("[AVISO]", f"{pct_preenchidos:.1f}% de municípios preenchidos"))
        else:
            validacoes.append(("[ERRO]", f"{pct_preenchidos:.1f}% de municípios preenchidos"))
    
    # 3. Municípios em maiúsculas
    if 'municipio' in df.columns:
        municipios_minusculas = df['municipio'].astype(str).str.islower().sum()
        if municipios_minusculas == 0:
            validacoes.append(("[OK]", "Todos os municípios em maiúsculas"))
        else:
            validacoes.append(("[AVISO]", f"{municipios_minusculas} municípios não estão em maiúsculas"))
    
    # 4. Códigos de município válidos (5 dígitos)
    if 'codigo_municipio_destinatario' in df.columns:
        codigos_invalidos = df['codigo_municipio_destinatario'].astype(str).str.len() != 5
        codigos_invalidos_count = codigos_invalidos.sum()
        
        if codigos_invalidos_count == 0:
            validacoes.append(("[OK]", "Todos os códigos de município válidos (5 dígitos)"))
        else:
            pct = (codigos_invalidos_count / len(df)) * 100
            validacoes.append(("[AVISO]", f"{codigos_invalidos_count} códigos inválidos ({pct:.2f}%)"))
    
    # 5. Municípios únicos
    if 'municipio' in df.columns:
        municipios_unicos = df['municipio'].nunique()
        validacoes.append(("[INFO]", f"{municipios_unicos:,} municípios únicos"))
    
    # 6. Total de registros
    validacoes.append(("[OK]", f"Total de registros: {len(df):,}"))
    
    # 7. Sem registros duplicados (por chave)
    if 'chave_codigo' in df.columns:
        duplicatas = df.duplicated(subset=['chave_codigo']).sum()
        if duplicatas == 0:
            validacoes.append(("[OK]", "Nenhum registro duplicado por chave"))
        else:
            validacoes.append(("[AVISO]", f"{duplicatas} registros duplicados"))
    
    # Exibir resultados
    print("Resultados das Validações:")
    print("-" * 60)
    
    erros = 0
    avisos = 0
    ok = 0
    
    for status, msg in validacoes:
        print(f"{status} {msg}")
        if status == "[ERRO]":
            erros += 1
        elif status == "[AVISO]":
            avisos += 1
        elif status == "[OK]":
            ok += 1
    
    print("\n" + "="*60)
    print(f"Resumo: {ok} OK | {avisos} Avisos | {erros} Erros")
    print("="*60)
    
    # Estatísticas adicionais
    if 'municipio' in df.columns:
        print("\nTop 10 Municípios:")
        print("-" * 60)
        top_municipios = df['municipio'].value_counts().head(10)
        for mun, count in top_municipios.items():
            pct = (count / len(df)) * 100
            print(f"  {count:>6} ({pct:>5.2f}%) - {mun}")
    
    if erros > 0:
        print("\n[ERRO] Validação falhou!")
        return False
    else:
        print("\n[SUCESSO] Validação concluída sem erros!")
        return True


if __name__ == "__main__":
    
    # Encontrar arquivo enriquecido mais recente
    arquivos = glob.glob("data/processed/nfe_etapa04_enriquecido.csv")
    
    if not arquivos:
        print("[ERRO] Nenhum arquivo enriquecido encontrado em data/processed/")
        print("[INFO] Execute primeiro: python scripts/processar_enriquecimento.py")
        sys.exit(1)
    
    # Pegar o mais recente
    arquivo_mais_recente = max(arquivos, key=os.path.getmtime)
    
    # Validar
    sucesso = validar_dados_enriquecidos(arquivo_mais_recente)
    
    if sucesso:
        sys.exit(0)
    else:
        sys.exit(1)
