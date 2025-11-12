# -*- coding: utf-8 -*-
"""
Script principal para processar os dados da Anvisa.
Orquestra todo o pipeline de processamento, executando em sequência ao baixar.py.

Este script processa o arquivo 'base_anvisa_precos_vigencias.csv' gerado pelo baixar.py
e aplica todas as transformações necessárias nos dados.

Uso:
    python processar_dados.py
"""

import pandas as pd
import os
import sys
from datetime import datetime

# Importar módulos locais
from config import configurar_pandas, ARQUIVO_ENTRADA, ARQUIVO_SAIDA
from modules.limpeza_dados import limpar_padronizar_dados
from modules.unificacao_vigencias import unificar_vigencias_consecutivas
from modules.classificacao_terapeutica import processar_classificacao_terapeutica
from modules.principio_ativo import processar_principio_ativo, exportar_principios_ativos_unicos
from modules.produto import processar_produto, exportar_produtos_unicos
from modules.apresentacao import criar_flag_substancia_composta, processar_apresentacao
from modules.tipo_produto import processar_tipo_produto
from modules.dosagem import processar_dosagem
from modules.laboratorio import processar_laboratorio
from modules.grupo_terapeutico import processar_grupo_terapeutico
from modules.finalizacao import processar_finalizacao

def verificar_arquivo_entrada():
    """
    Verifica se o arquivo de entrada existe.
    
    Returns:
        bool: True se o arquivo existe, False caso contrário
    """
    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"[ERRO] Arquivo '{ARQUIVO_ENTRADA}' nao encontrado!")
        print("Certifique-se de executar o script 'baixar.py' primeiro.")
        return False
    
    print(f"[OK] Arquivo '{ARQUIVO_ENTRADA}' encontrado.")
    return True

def carregar_dados():
    """
    Carrega os dados do arquivo CSV.
    
    Returns:
        pandas.DataFrame: DataFrame carregado ou None se houve erro
    """
    try:
        print(f"\nCarregando dados de '{ARQUIVO_ENTRADA}'...")
        
        # Tenta carregar com diferentes configurações
        try:
            # Primeira tentativa: separador ponto-e-vírgula (comum em CSVs brasileiros)
            df = pd.read_csv(ARQUIVO_ENTRADA, sep=';', encoding='utf-8', on_bad_lines='skip')
        except:
            try:
                # Segunda tentativa: separador vírgula
                df = pd.read_csv(ARQUIVO_ENTRADA, sep=',', encoding='utf-8', on_bad_lines='skip')
            except:
                try:
                    # Terceira tentativa: separador tab
                    df = pd.read_csv(ARQUIVO_ENTRADA, sep='\t', encoding='utf-8', on_bad_lines='skip')
                except:
                    # Quarta tentativa: encoding latin1
                    df = pd.read_csv(ARQUIVO_ENTRADA, sep=';', encoding='latin1', on_bad_lines='skip')
        
        print("\n[OK] Dados carregados com sucesso!")
        print("Informacoes do DataFrame:")
        df.info()
        print(f"\nPrimeiras 5 linhas:")
        print(df.head())
        
        return df
        
    except Exception as e:
        print(f"[ERRO] Erro ao carregar dados: {e}")
        return None

def salvar_dados_processados(df):
    """
    Salva os dados processados no arquivo de saída.
    
    Args:
        df (pandas.DataFrame): DataFrame processado para salvar
        
    Returns:
        bool: True se salvou com sucesso, False caso contrário
    """
    try:
        print(f"\nSalvando dados processados em '{ARQUIVO_SAIDA}'...")
        df.to_csv(ARQUIVO_SAIDA, index=False, encoding='utf-8')
        
        print(f"[OK] Dados salvos com sucesso em '{ARQUIVO_SAIDA}'!")
        print(f"Arquivo contem {len(df):,} registros.")
        
        return True
        
    except Exception as e:
        print(f"[ERRO] Erro ao salvar dados: {e}")
        return False

def exibir_estatisticas_finais(df_original, df_processado):
    """
    Exibe estatísticas comparativas entre os dados originais e processados.
    
    Args:
        df_original (pandas.DataFrame): DataFrame original
        df_processado (pandas.DataFrame): DataFrame processado
    """
    print("\n" + "=" * 80)
    print("ESTATÍSTICAS FINAIS DO PROCESSAMENTO")
    print("=" * 80)
    
    print(f"Registros originais: {len(df_original):,}")
    print(f"Registros processados: {len(df_processado):,}")
    
    reducao = len(df_original) - len(df_processado)
    percentual = (reducao / len(df_original)) * 100 if len(df_original) > 0 else 0
    
    print(f"Redução: {reducao:,} registros ({percentual:.2f}%)")
    
    # Verificar se as novas colunas foram criadas
    if 'GRUPO ANATOMICO' in df_processado.columns:
        print(f"\n[OK] Coluna 'GRUPO ANATOMICO' criada com {df_processado['GRUPO ANATOMICO'].nunique()} grupos unicos.")
    
    if 'CLASSE_TERAPEUTICA_ORIGINAL' in df_processado.columns:
        print("[OK] Backup da classe terapeutica original mantido.")
    
    print("\nColunas no DataFrame final:")
    print(list(df_processado.columns))

def main():
    """
    Função principal que orquestra todo o pipeline de processamento.
    """
    print("=" * 80)
    print("PIPELINE DE PROCESSAMENTO DOS DADOS ANVISA")
    print("=" * 80)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configurar pandas
    configurar_pandas()
    
    # Verificar se o arquivo de entrada existe
    if not verificar_arquivo_entrada():
        sys.exit(1)
    
    # Carregar dados
    df_original = carregar_dados()
    if df_original is None:
        sys.exit(1)
    
    # Fazer uma cópia para preservar os dados originais
    df_processado = df_original.copy()
    
    try:
        # ETAPA 1: Limpeza e padronização
        print("\n" + "=" * 80)
        print("ETAPA 1/10: LIMPEZA E PADRONIZACAO")
        print("=" * 80)
        df_processado = limpar_padronizar_dados(df_processado)
        
        # ETAPA 2: Unificação de vigências
        print("\n" + "=" * 80)
        print("ETAPA 2/10: UNIFICACAO DE VIGENCIAS")
        print("=" * 80)
        df_processado = unificar_vigencias_consecutivas(df_processado)
        
        # ETAPA 3: Classificação terapêutica
        print("\n" + "=" * 80)
        print("ETAPA 3/10: PROCESSAMENTO DA CLASSIFICACAO TERAPEUTICA")
        print("=" * 80)
        df_processado = processar_classificacao_terapeutica(df_processado)
        
        # ETAPA 4: Processamento do princípio ativo
        print("\n" + "=" * 80)
        print("ETAPA 4/10: PROCESSAMENTO DO PRINCIPIO ATIVO")
        print("=" * 80)
        df_processado = processar_principio_ativo(df_processado, executar_fuzzy_matching=False)
        
        # ETAPA 5: Processamento do produto
        print("\n" + "=" * 80)
        print("ETAPA 5/10: PROCESSAMENTO DO PRODUTO")
        print("=" * 80)
        df_processado = processar_produto(df_processado)
        
        # ETAPA 6: Criar flag de substancia composta e processar apresentacao
        print("\n" + "=" * 80)
        print("ETAPA 6/10: PROCESSAMENTO DA APRESENTACAO")
        print("=" * 80)
        df_processado = criar_flag_substancia_composta(df_processado)
        df_processado = processar_apresentacao(df_processado)
        
        # ETAPA 7: Categorizar tipo de produto e extrair dosagens
        print("\n" + "=" * 80)
        print("ETAPA 7/10: CATEGORIZACAO E EXTRACAO DE DOSAGENS")
        print("=" * 80)
        df_processado = processar_tipo_produto(df_processado)
        df_processado = processar_dosagem(df_processado, debug=False)
        
        # ETAPA 8: Processamento do laboratorio
        print("\n" + "=" * 80)
        print("ETAPA 8/10: PROCESSAMENTO DO LABORATORIO")
        print("=" * 80)
        df_processado = processar_laboratorio(df_processado)
        
        # ETAPA 9: Processamento do grupo terapeutico
        print("\n" + "=" * 80)
        print("ETAPA 9/10: PROCESSAMENTO DO GRUPO TERAPEUTICO")
        print("=" * 80)
        df_processado = processar_grupo_terapeutico(df_processado, criar_debug=True)
        
        # ETAPA 10: Finalizacao (padronizacao e exports)
        print("\n" + "=" * 80)
        print("ETAPA 10/10: FINALIZACAO E EXPORTACAO")
        print("=" * 80)
        df_processado = processar_finalizacao(df_processado)
        
        # Exportacoes adicionais (listas unicas para referencia)
        print("\nExportando listas de referencia...")
        exportar_principios_ativos_unicos(df_processado)
        exportar_produtos_unicos(df_processado)
        
        # Exibir estatísticas finais
        exibir_estatisticas_finais(df_original, df_processado)
        
        print("\n" + "=" * 80)
        print("[OK] PIPELINE CONCLUIDO COM SUCESSO!")
        print("=" * 80)
        print(f"Finalizado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante o processamento: {e}")
        print("\nDetalhes do erro:")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()