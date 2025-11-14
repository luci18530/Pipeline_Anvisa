"""
Script para reprocessar baseANVISA.csv aplicando normalização de APRESENTACAO
Lê o arquivo existente, aplica normalização e salva novamente
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Adicionar o diretório modules ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src', 'modules'))

from apresentacao import normalizar_apresentacao, limpar_apresentacao_final, expandir_cx_bl


def reprocessar_base_anvisa():
    """
    Reprocessa baseANVISA.csv aplicando normalização de APRESENTACAO
    """
    print("\n" + "="*80)
    print("REPROCESSAMENTO DA BASE ANVISA - NORMALIZACAO DE APRESENTACAO")
    print("="*80 + "\n")
    
    arquivo_entrada = "output/anvisa/baseANVISA.csv"
    arquivo_backup = f"output/anvisa/baseANVISA_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    arquivo_saida = "output/anvisa/baseANVISA.csv"
    dtypes_file = "output/anvisa/baseANVISA_dtypes.json"
    
    # 1. Verificar se arquivo existe
    if not os.path.exists(arquivo_entrada):
        print(f"[ERRO] Arquivo não encontrado: {arquivo_entrada}")
        return False
    
    print(f"[INFO] Carregando: {arquivo_entrada}")
    print("[INFO] Este processo pode demorar alguns minutos...")
    
    # 2. Carregar base ANVISA
    df = pd.read_csv(arquivo_entrada, sep='\t', encoding='utf-8', low_memory=False)
    print(f"[OK] Carregado: {len(df):,} registros, {len(df.columns)} colunas")
    
    # 2.5. Remover colunas duplicadas (terminadas em .1, .2, etc)
    print(f"\n[INFO] Verificando colunas duplicadas...")
    colunas_duplicadas = [col for col in df.columns if '.1' in col or '.2' in col or '.3' in col]
    if colunas_duplicadas:
        print(f"[AVISO] Encontradas {len(colunas_duplicadas)} colunas duplicadas:")
        for col in colunas_duplicadas:
            print(f"  - {col}")
        print(f"[INFO] Removendo colunas duplicadas...")
        df = df.drop(columns=colunas_duplicadas)
        print(f"[OK] Colunas duplicadas removidas! Total de colunas agora: {len(df.columns)}")
    else:
        print(f"[OK] Nenhuma coluna duplicada encontrada")
    
    # 3. Verificar colunas
    print(f"\n[INFO] Verificando colunas de apresentacao...")
    print(f"  - APRESENTACAO_ORIGINAL: {'SIM' if 'APRESENTACAO_ORIGINAL' in df.columns else 'NAO'}")
    print(f"  - APRESENTACAO: {'SIM' if 'APRESENTACAO' in df.columns else 'NAO'}")
    
    if 'APRESENTACAO_ORIGINAL' not in df.columns:
        print("[ERRO] Coluna APRESENTACAO_ORIGINAL não encontrada!")
        return False
    
    # 4. Criar backup
    print(f"\n[INFO] Criando backup: {arquivo_backup}")
    df.to_csv(arquivo_backup, sep='\t', index=False, encoding='utf-8')
    print(f"[OK] Backup criado!")
    
    # 5. Criar coluna APRESENTACAO se não existir
    if 'APRESENTACAO' not in df.columns:
        print(f"\n[INFO] Criando coluna APRESENTACAO a partir de APRESENTACAO_ORIGINAL...")
        df['APRESENTACAO'] = df['APRESENTACAO_ORIGINAL'].copy()
    
    # 6. Criar flag SUBSTANCIA_COMPOSTA se não existir
    if 'SUBSTANCIA_COMPOSTA' not in df.columns:
        if 'PRINCIPIO ATIVO' in df.columns:
            print(f"[INFO] Criando flag SUBSTANCIA_COMPOSTA...")
            df['SUBSTANCIA_COMPOSTA'] = df['PRINCIPIO ATIVO'].str.contains(r'\+', na=False)
            compostos = df['SUBSTANCIA_COMPOSTA'].sum()
            print(f"[OK] {compostos:,} substancias compostas identificadas")
        else:
            df['SUBSTANCIA_COMPOSTA'] = False
    
    # 7. Contar apresentações únicas antes
    unicas_antes = df['APRESENTACAO'].nunique()
    print(f"\n[INFO] Apresentacoes unicas (ANTES): {unicas_antes:,}")
    
    # 8. Aplicar normalização
    print(f"\n[INFO] Aplicando normalizacao (217+ regras)...")
    print("[INFO] Processando linha por linha com progress bar...")
    
    from tqdm import tqdm
    tqdm.pandas(desc="Normalizando")
    
    def normalizar_row(row):
        """Aplica normalização linha por linha"""
        texto = row['APRESENTACAO']
        composta = row['SUBSTANCIA_COMPOSTA']
        
        if pd.isna(texto):
            return texto
        
        # Aplicar normalização principal
        resultado = normalizar_apresentacao(str(texto), bool(composta))
        
        # Aplicar limpeza final
        resultado = limpar_apresentacao_final(resultado)
        
        # Expandir CX BL
        resultado = expandir_cx_bl(resultado)
        
        return resultado
    
    # Aplicar normalização com progress bar
    df['APRESENTACAO'] = df.progress_apply(normalizar_row, axis=1)
    
    # 9. Contar apresentações únicas depois
    unicas_depois = df['APRESENTACAO'].nunique()
    reducao = unicas_antes - unicas_depois
    pct_reducao = (reducao / unicas_antes * 100) if unicas_antes > 0 else 0
    
    print(f"\n[OK] Normalizacao concluida!")
    print(f"[INFO] Apresentacoes unicas (DEPOIS): {unicas_depois:,}")
    print(f"[INFO] Reducao de variacoes: {reducao:,} ({pct_reducao:.1f}%)")
    
    # 10. Mostrar exemplos de normalização
    print(f"\n[INFO] Exemplos de normalizacao:")
    exemplos = df[df['APRESENTACAO_ORIGINAL'] != df['APRESENTACAO']].head(5)
    for idx, (orig, norm) in enumerate(zip(exemplos['APRESENTACAO_ORIGINAL'], exemplos['APRESENTACAO']), 1):
        print(f"\n  {idx}. ANTES: {str(orig)[:70]}")
        print(f"     DEPOIS: {str(norm)[:70]}")
    
    # 11. Salvar arquivo atualizado
    print(f"\n[INFO] Salvando arquivo atualizado: {arquivo_saida}")
    df.to_csv(arquivo_saida, sep='\t', index=False, encoding='utf-8')
    
    tamanho_mb = os.path.getsize(arquivo_saida) / (1024 * 1024)
    print(f"[OK] Arquivo salvo!")
    print(f"  - Registros: {len(df):,}")
    print(f"  - Colunas: {len(df.columns)}")
    print(f"  - Tamanho: {tamanho_mb:.2f} MB")
    
    # 12. Atualizar dtypes
    if os.path.exists(dtypes_file):
        print(f"\n[INFO] Atualizando tipos de dados: {dtypes_file}")
        import json
        dtypes_dict = {col: str(dtype) for col, dtype in df.dtypes.items()}
        with open(dtypes_file, 'w', encoding='utf-8') as f:
            json.dump(dtypes_dict, f, indent=2, ensure_ascii=False)
        print(f"[OK] Tipos de dados atualizados!")
    
    # 13. Verificar produtos com apresentação única
    print(f"\n" + "="*80)
    print("ANALISE DE PRODUTOS COM APRESENTACAO UNICA")
    print("="*80)
    
    contagem = df.groupby('PRODUTO')['APRESENTACAO'].nunique().reset_index(name='qtd_apresentacoes')
    produtos_unicos = contagem[contagem['qtd_apresentacoes'] == 1]
    
    print(f"\nTotal de produtos: {len(contagem):,}")
    print(f"Produtos com 1 apresentacao unica: {len(produtos_unicos):,}")
    print(f"Produtos com multiplas apresentacoes: {len(contagem) - len(produtos_unicos):,}")
    
    # Exemplos de produtos únicos
    print(f"\nExemplos de produtos com apresentacao unica:")
    for i, produto in enumerate(produtos_unicos['PRODUTO'].head(10), 1):
        print(f"  {i}. {produto}")
    
    # Verificar SOLUCAO FISIOLOGICA especificamente
    print(f"\n[VERIFICACAO] Produto SOLUCAO FISIOLOGICA DE CLORETO DE SODIO:")
    solucao = df[df['PRODUTO'].str.contains('SOLUCAO FISIOLOGICA', case=False, na=False)]
    if not solucao.empty:
        print(f"  Total de registros: {len(solucao)}")
        print(f"  Apresentacoes unicas: {solucao['APRESENTACAO'].nunique()}")
        print(f"\n  Sample de apresentacoes (5 primeiras):")
        for i, apres in enumerate(solucao['APRESENTACAO'].unique()[:5], 1):
            count = (solucao['APRESENTACAO'] == apres).sum()
            print(f"    {i}. {apres} ({count} registros)")
    
    print(f"\n" + "="*80)
    print("[SUCESSO] REPROCESSAMENTO CONCLUIDO!")
    print("="*80)
    print(f"\nArquivos gerados:")
    print(f"  1. {arquivo_saida} - Base atualizada com APRESENTACAO normalizada")
    print(f"  2. {arquivo_backup} - Backup da base original")
    print(f"  3. {dtypes_file} - Tipos de dados atualizados")
    
    return True


if __name__ == '__main__':
    try:
        sucesso = reprocessar_base_anvisa()
        if sucesso:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        print(f"\n[ERRO] Falha no reprocessamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
