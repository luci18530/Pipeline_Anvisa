"""
Script de validação de dados limpos de NFe
"""

import pandas as pd
import sys
import os
import glob


def validar_dados_limpos(arquivo_csv):
    """Valida dados limpos processados"""
    
    print("="*60)
    print("Validação de Dados Limpos de NFe")
    print("="*60)
    print(f"Arquivo: {arquivo_csv}\n")
    
    # Carregar dados
    df = pd.read_csv(arquivo_csv, sep=';')
    
    # Validações
    validacoes = []
    
    # 1. Colunas críticas presentes
    colunas_criticas = ['descricao_produto', 'quantidade', 'valor_produtos', 'data_emissao']
    colunas_faltantes = set(colunas_criticas) - set(df.columns)
    if not colunas_faltantes:
        validacoes.append(("[OK]", f"Todas as colunas críticas presentes"))
    else:
        validacoes.append(("[ERRO]", f"Colunas faltantes: {colunas_faltantes}"))
    
    # 2. Descrições não vazias
    desc_vazias = df['descricao_produto'].isna().sum()
    desc_vazias += (df['descricao_produto'].astype(str).str.strip() == '').sum()
    if desc_vazias == 0:
        validacoes.append(("[OK]", "Nenhuma descrição vazia"))
    else:
        pct = desc_vazias / len(df) * 100
        if pct < 1:
            validacoes.append(("[AVISO]", f"{desc_vazias} descrições vazias ({pct:.2f}%)"))
        else:
            validacoes.append(("[ERRO]", f"{desc_vazias} descrições vazias ({pct:.2f}%)"))
    
    # 3. Descrições em maiúsculas
    desc_minusculas = df['descricao_produto'].astype(str).str.islower().sum()
    if desc_minusculas == 0:
        validacoes.append(("[OK]", "Todas as descrições em maiúsculas"))
    else:
        validacoes.append(("[AVISO]", f"{desc_minusculas} descrições não estão em maiúsculas"))
    
    # 4. Caracteres especiais removidos
    desc_com_especiais = df['descricao_produto'].astype(str).str.contains(r'["#\$\'\(\)\*@]', regex=True).sum()
    if desc_com_especiais == 0:
        validacoes.append(("[OK]", "Caracteres especiais removidos"))
    else:
        validacoes.append(("[AVISO]", f"{desc_com_especiais} descrições com caracteres especiais"))
    
    # 5. Espaços múltiplos removidos
    desc_espacos_multiplos = df['descricao_produto'].astype(str).str.contains(r'\s{2,}', regex=True).sum()
    if desc_espacos_multiplos == 0:
        validacoes.append(("[OK]", "Sem espaços múltiplos"))
    else:
        validacoes.append(("[AVISO]", f"{desc_espacos_multiplos} descrições com espaços múltiplos"))
    
    # 6. Quantidade é numérica
    try:
        qtd_numeric = pd.to_numeric(df['quantidade'], errors='coerce')
        qtd_invalidas = qtd_numeric.isna().sum()
        if qtd_invalidas == 0:
            validacoes.append(("[OK]", "Todas as quantidades são numéricas"))
        else:
            validacoes.append(("[AVISO]", f"{qtd_invalidas} quantidades inválidas"))
    except:
        validacoes.append(("[ERRO]", "Erro ao validar quantidades"))
    
    # 7. Descrições únicas
    total_desc = len(df)
    desc_unicas = df['descricao_produto'].nunique()
    pct_unicas = (desc_unicas / total_desc) * 100
    validacoes.append(("[INFO]", f"{desc_unicas:,} descrições únicas ({pct_unicas:.1f}%)"))
    
    # 8. Comprimento de descrições
    desc_len = df['descricao_produto'].astype(str).str.len()
    desc_muito_curtas = (desc_len < 5).sum()
    desc_muito_longas = (desc_len > 200).sum()
    
    if desc_muito_curtas == 0:
        validacoes.append(("[OK]", "Nenhuma descrição muito curta (<5 chars)"))
    else:
        pct = desc_muito_curtas / len(df) * 100
        validacoes.append(("[AVISO]", f"{desc_muito_curtas} descrições muito curtas ({pct:.2f}%)"))
    
    if desc_muito_longas == 0:
        validacoes.append(("[OK]", "Nenhuma descrição muito longa (>200 chars)"))
    else:
        pct = desc_muito_longas / len(df) * 100
        validacoes.append(("[AVISO]", f"{desc_muito_longas} descrições muito longas ({pct:.2f}%)"))
    
    # 9. Total de registros
    validacoes.append(("[OK]", f"Total de registros: {len(df):,}"))
    
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
    print("\nEstatísticas de Descrições:")
    print(f"  - Comprimento médio: {desc_len.mean():.1f} caracteres")
    print(f"  - Comprimento mínimo: {desc_len.min()} caracteres")
    print(f"  - Comprimento máximo: {desc_len.max()} caracteres")
    print(f"  - Total de palavras: {df['descricao_produto'].astype(str).str.split().str.len().sum():,}")
    print(f"  - Média de palavras por descrição: {df['descricao_produto'].astype(str).str.split().str.len().mean():.1f}")
    
    # Top 10 descrições mais frequentes
    print("\nTop 10 Descrições Mais Frequentes:")
    print("-" * 60)
    top_desc = df['descricao_produto'].value_counts().head(10)
    for desc, count in top_desc.items():
        pct = (count / len(df)) * 100
        print(f"  {count:>6} ({pct:>5.2f}%) - {desc[:70]}")
    
    if erros > 0:
        print("\n[ERRO] Validação falhou!")
        return False
    else:
        print("\n[SUCESSO] Validação concluída sem erros!")
        return True


if __name__ == "__main__":
    
    # Encontrar arquivo limpo mais recente
    arquivos = glob.glob("data/processed/nfe_limpo_*.csv")
    
    if not arquivos:
        print("[ERRO] Nenhum arquivo limpo encontrado em data/processed/")
        print("[INFO] Execute primeiro: python scripts/processar_limpeza.py")
        sys.exit(1)
    
    # Pegar o mais recente
    arquivo_mais_recente = max(arquivos, key=os.path.getmtime)
    
    # Validar
    sucesso = validar_dados_limpos(arquivo_mais_recente)
    
    if sucesso:
        sys.exit(0)
    else:
        sys.exit(1)
