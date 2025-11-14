"""
Script Principal - Pipeline Completo de Processamento de Notas Fiscais (NFe)
Executa todas as etapas do pipeline em sequência
"""

import sys
import os
import glob
import subprocess
import pandas as pd
from datetime import datetime

# Adicionar src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class PipelineNFe:
    """Orquestrador do pipeline completo de NFe"""
    
    def __init__(self):
        self.inicio = datetime.now()
        self.etapas = []
        self.arquivos_gerados = []
        self.erros = []
        self.max_execucoes = 2  # Manter os últimos 2 processamentos
    
    def limpar_arquivos_antigos(self):
        """Remove arquivos de processamentos antigos, mantendo apenas os últimos N"""
        print("\n" + "="*60)
        print("[LIMPEZA] Removendo arquivos de processamentos antigos...")
        print("="*60)
        
        try:
            # Diretórios a limpar
            dirs_limpar = [
                "data/processed"
            ]
            
            for diretorio in dirs_limpar:
                if not os.path.exists(diretorio):
                    continue
                
                # Padrões de arquivo por tipo
                padroes = {
                    'processado': 'nfe_processado_*.csv',
                    'vencimento': 'nfe_vencimento_*.csv',
                    'limpo': 'nfe_limpo_*.csv',
                    'enriquecido': 'nfe_enriquecido_*.csv',
                    'matched': 'nfe_matched_*.csv',
                    'matched_manual': 'nfe_matched_manual_*.csv',
                    'completo': 'df_completo_*.zip',
                    'trabalhando': 'df_trabalhando_*.zip',
                    'trabalhando_nomes': 'df_trabalhando_nomes_*.zip',
                    'trabalhando_refinado': 'df_trabalhando_refinado_*.zip',
                    'final_trabalhando': 'df_final_trabalhando_*.zip',
                    'no_match': 'df_no_match_*.zip'
                }
                
                for tipo, padrao in padroes.items():
                    arquivos = sorted(
                        glob.glob(os.path.join(diretorio, padrao)),
                        key=os.path.getmtime,
                        reverse=True  # Mais novos primeiro
                    )
                    
                    # Remover arquivos além do limite
                    if len(arquivos) > self.max_execucoes:
                        for arquivo in arquivos[self.max_execucoes:]:
                            try:
                                tamanho_mb = os.path.getsize(arquivo) / (1024*1024)
                                os.remove(arquivo)
                                print(f"[REMOVIDO] {os.path.basename(arquivo):<50} ({tamanho_mb:>6.1f} MB)")
                            except Exception as e:
                                print(f"[AVISO] Erro ao remover {os.path.basename(arquivo)}: {str(e)}")
                
            print("="*60)
            print("[OK] Limpeza de arquivos concluída!")
            print("="*60 + "\n")
            
        except Exception as e:
            print(f"[AVISO] Erro durante limpeza de arquivos: {str(e)}")
    
    def log_etapa(self, numero, nome, status, duracao=None):
        """Registra uma etapa executada"""
        duracao_str = f" ({duracao:.1f}s)" if duracao else ""
        print(f"\n{'='*60}")
        print(f"[ETAPA {numero}] {nome}")
        print(f"[{status}]{duracao_str}")
        print(f"{'='*60}")
        self.etapas.append((numero, nome, status, duracao))
    
    def log_arquivo(self, caminho):
        """Registra um arquivo gerado"""
        self.arquivos_gerados.append(caminho)
    
    def log_erro(self, etapa, mensagem):
        """Registra um erro"""
        self.erros.append((etapa, mensagem))
    
    def executar_script(self, script_path, nome_etapa):
        """Executa um script Python e retorna True se bem-sucedido"""
        try:
            print(f"\n[EXECUTANDO] {nome_etapa}...")
            resultado = subprocess.run(
                [sys.executable, script_path],
                capture_output=False,
                text=True,
                timeout=600  # 10 minutos por etapa
            )
            return resultado.returncode == 0
        except subprocess.TimeoutExpired:
            self.log_erro(nome_etapa, "Timeout (>10 minutos)")
            return False
        except Exception as e:
            self.log_erro(nome_etapa, str(e))
            return False
    
    def etapa_1_carregamento(self):
        """Etapa 1: Carregamento e pré-processamento de NFe"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 1: CARREGAMENTO E PRÉ-PROCESSAMENTO")
        print("="*60)
        
        try:
            # Executar script de processamento
            sucesso = self.executar_script(
                "scripts/processar_nfe.py",
                "Processamento de Carregamento"
            )
            
            if not sucesso:
                raise Exception("Script de carregamento falhou")
            
            # Encontrar arquivo gerado
            arquivos = glob.glob("data/processed/nfe_processado_*.csv")
            if not arquivos:
                raise Exception("Nenhum arquivo processado gerado")
            
            arquivo_saida = max(arquivos, key=os.path.getmtime)
            self.log_arquivo(arquivo_saida)
            
            # Validar dados
            print("\n[VALIDANDO] Dados carregados...")
            sucesso = self.executar_script(
                "scripts/validar_nfe.py",
                "Validação de Carregamento"
            )
            
            if not sucesso:
                raise Exception("Validação falhou")
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(1, "Carregamento e Pré-processamento", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(1, "Carregamento e Pré-processamento", "ERRO", duracao)
            self.log_erro("Etapa 1", str(e))
            return False
    
    def etapa_2_vencimento(self):
        """Etapa 2: Processamento de vencimento"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 2: PROCESSAMENTO DE VENCIMENTO")
        print("="*60)
        
        try:
            # Executar script de vencimento
            sucesso = self.executar_script(
                "scripts/processar_vencimento.py",
                "Processamento de Vencimento"
            )
            
            if not sucesso:
                raise Exception("Script de vencimento falhou")
            
            # Encontrar arquivo gerado
            arquivos = glob.glob("data/processed/nfe_vencimento_*.csv")
            if not arquivos:
                raise Exception("Nenhum arquivo de vencimento gerado")
            
            arquivo_saida = max(arquivos, key=os.path.getmtime)
            self.log_arquivo(arquivo_saida)
            
            # Validar dados
            print("\n[VALIDANDO] Dados de vencimento...")
            sucesso = self.executar_script(
                "scripts/validar_vencimento.py",
                "Validação de Vencimento"
            )
            
            if not sucesso:
                raise Exception("Validação de vencimento falhou")
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(2, "Processamento de Vencimento", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(2, "Processamento de Vencimento", "ERRO", duracao)
            self.log_erro("Etapa 2", str(e))
            return False
    
    def etapa_3_limpeza(self):
        """Etapa 3: Limpeza e padronização de descrições"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 3: LIMPEZA DE DESCRIÇÕES")
        print("="*60)
        
        try:
            # Executar script de limpeza
            sucesso = self.executar_script(
                "scripts/processar_limpeza.py",
                "Processamento de Limpeza"
            )
            
            if not sucesso:
                raise Exception("Script de limpeza falhou")
            
            # Encontrar arquivo gerado
            arquivos = glob.glob("data/processed/nfe_limpo_*.csv")
            if not arquivos:
                raise Exception("Nenhum arquivo limpo gerado")
            
            arquivo_saida = max(arquivos, key=os.path.getmtime)
            self.log_arquivo(arquivo_saida)
            
            # Validar dados
            print("\n[VALIDANDO] Dados limpos...")
            sucesso = self.executar_script(
                "scripts/validar_limpeza.py",
                "Validação de Limpeza"
            )
            
            if not sucesso:
                raise Exception("Validação de limpeza falhou")
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(3, "Limpeza de Descrições", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(3, "Limpeza de Descrições", "ERRO", duracao)
            self.log_erro("Etapa 3", str(e))
            return False
    
    def etapa_4_enriquecimento(self):
        """Etapa 4: Enriquecimento com dados de município"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 4: ENRIQUECIMENTO COM DADOS DE MUNICÍPIO")
        print("="*60)
        
        try:
            # Executar script de enriquecimento
            sucesso = self.executar_script(
                "scripts/processar_enriquecimento.py",
                "Processamento de Enriquecimento"
            )
            
            if not sucesso:
                raise Exception("Script de enriquecimento falhou")
            
            # Encontrar arquivo gerado
            arquivos = glob.glob("data/processed/nfe_enriquecido_*.csv")
            if not arquivos:
                raise Exception("Nenhum arquivo enriquecido gerado")
            
            arquivo_saida = max(arquivos, key=os.path.getmtime)
            self.log_arquivo(arquivo_saida)
            
            # Validar dados
            print("\n[VALIDANDO] Dados enriquecidos...")
            sucesso = self.executar_script(
                "scripts/validar_enriquecimento.py",
                "Validação de Enriquecimento"
            )
            
            if not sucesso:
                raise Exception("Validação de enriquecimento falhou")
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(4, "Enriquecimento com Municípios", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(4, "Enriquecimento com Municípios", "ERRO", duracao)
            self.log_erro("Etapa 4", str(e))
            return False
    
    def etapa_5_carregamento_anvisa(self):
        """Etapa 5: Carregamento e preparação da base ANVISA (CMED)"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 5: CARREGAMENTO DA BASE ANVISA (CMED)")
        print("="*60)
        
        try:
            # Executar script de carregamento da base ANVISA
            sucesso = self.executar_script(
                "scripts/processar_base_anvisa.py",
                "Carregamento da Base ANVISA"
            )
            
            if not sucesso:
                raise Exception("Script de carregamento ANVISA falhou")
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(5, "Carregamento da Base ANVISA (CMED)", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(5, "Carregamento da Base ANVISA (CMED)", "ERRO", duracao)
            self.log_erro("Etapa 5", str(e))
            return False
    
    def etapa_6_otimizacao_memoria(self):
        """Etapa 6: Otimização de memória dos DataFrames"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 6: OTIMIZAÇÃO DE MEMÓRIA")
        print("="*60)
        
        try:
            # Executar script de otimização
            sucesso = self.executar_script(
                "scripts/otimizar_memoria_nfe.py",
                "Otimização de Memória"
            )
            
            if not sucesso:
                raise Exception("Script de otimização falhou")
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(6, "Otimização de Memória", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(6, "Otimização de Memória", "ERRO", duracao)
            self.log_erro("Etapa 6", str(e))
            return False
    
    def etapa_7_matching_anvisa(self):
        """Etapa 7: Matching e enriquecimento com base ANVISA (CMED)"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 7: MATCHING NFe x ANVISA (CMED)")
        print("="*60)
        
        try:
            # Executar script de matching
            sucesso = self.executar_script(
                "scripts/processar_matching_anvisa.py",
                "Matching com Base ANVISA"
            )
            
            if not sucesso:
                raise Exception("Script de matching falhou")
            
            # Encontrar arquivo gerado
            arquivos = glob.glob("data/processed/nfe_matched_*.csv")
            if not arquivos:
                raise Exception("Nenhum arquivo de matching gerado")
            
            arquivo_saida = max(arquivos, key=os.path.getmtime)
            self.log_arquivo(arquivo_saida)
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(7, "Matching NFe x ANVISA (CMED)", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(7, "Matching NFe x ANVISA (CMED)", "ERRO", duracao)
            self.log_erro("Etapa 7", str(e))
            return False
    
    def etapa_8_matching_manual(self):
        """Etapa 8: Matching manual com base do Google Sheets"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 8: MATCHING MANUAL (GOOGLE SHEETS)")
        print("="*60)
        
        try:
            # Executar script de matching manual
            sucesso = self.executar_script(
                "scripts/processar_matching_manual.py",
                "Matching Manual"
            )
            
            if not sucesso:
                raise Exception("Script de matching manual falhou")
            
            # Encontrar arquivo gerado
            arquivos = glob.glob("data/processed/nfe_matched_manual_*.csv")
            if not arquivos:
                raise Exception("Nenhum arquivo de matching manual gerado")
            
            arquivo_saida = max(arquivos, key=os.path.getmtime)
            self.log_arquivo(arquivo_saida)
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(8, "Matching Manual (Google Sheets)", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(8, "Matching Manual (Google Sheets)", "ERRO", duracao)
            self.log_erro("Etapa 8", str(e))
            return False
    
    def etapa_9_separacao(self):
        """Etapa 9: Separação em fluxos e filtragem de não-medicinais"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 9: SEPARAÇÃO E FILTRAGEM")
        print("="*60)
        
        try:
            # Executar script de separação
            sucesso = self.executar_script(
                "scripts/processar_separacao.py",
                "Separação e Filtragem"
            )
            
            if not sucesso:
                raise Exception("Script de separação falhou")
            
            # Encontrar arquivos gerados (df_completo e df_trabalhando)
            arquivos_completo = glob.glob("data/processed/df_completo_*.zip")
            arquivos_trabalhando = glob.glob("data/processed/df_trabalhando_*.zip")
            
            if not arquivos_completo or not arquivos_trabalhando:
                raise Exception("Arquivos de separação não foram gerados")
            
            arquivo_completo = max(arquivos_completo, key=os.path.getmtime)
            arquivo_trabalhando = max(arquivos_trabalhando, key=os.path.getmtime)
            
            self.log_arquivo(arquivo_completo)
            self.log_arquivo(arquivo_trabalhando)
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(9, "Separação e Filtragem", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(9, "Separação e Filtragem", "ERRO", duracao)
            self.log_erro("Etapa 9", str(e))
            return False
    
    def etapa_10_extracao_nomes(self):
        """Etapa 10: Extração de nomes de produtos"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 10: EXTRAÇÃO DE NOMES")
        print("="*60)
        
        try:
            # Executar script de extração de nomes
            sucesso = self.executar_script(
                "scripts/processar_extracao_nomes.py",
                "Extração de Nomes"
            )
            
            if not sucesso:
                raise Exception("Script de extração de nomes falhou")
            
            # Encontrar arquivo gerado
            arquivos = glob.glob("data/processed/df_trabalhando_nomes_*.zip")
            if not arquivos:
                raise Exception("Arquivo de extração não foi gerado")
            
            arquivo_saida = max(arquivos, key=os.path.getmtime)
            self.log_arquivo(arquivo_saida)
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(10, "Extração de Nomes", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(10, "Extração de Nomes", "ERRO", duracao)
            self.log_erro("Etapa 10", str(e))
            return False
    
    def etapa_11_refinamento_nomes(self):
        """Etapa 11: Refinamento e limpeza avançada de nomes"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 11: REFINAMENTO DE NOMES")
        print("="*60)
        
        try:
            # Executar script de refinamento
            sucesso = self.executar_script(
                "scripts/processar_refinamento_nomes.py",
                "Refinamento de Nomes"
            )
            
            if not sucesso:
                raise Exception("Script de refinamento falhou")
            
            # Encontrar arquivo gerado
            arquivos = glob.glob("data/processed/df_trabalhando_refinado_*.zip")
            if not arquivos:
                raise Exception("Arquivo de refinamento não foi gerado")
            
            arquivo_saida = max(arquivos, key=os.path.getmtime)
            self.log_arquivo(arquivo_saida)
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(11, "Refinamento de Nomes", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(11, "Refinamento de Nomes", "ERRO", duracao)
            self.log_erro("Etapa 11", str(e))
            return False
    
    def etapa_12_unificacao_matching(self):
        """Etapa 12: Unificação de bases mestre e matching final"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 12: UNIFICAÇÃO E MATCHING FINAL")
        print("="*60)
        
        try:
            # Executar script de unificação
            sucesso = self.executar_script(
                "scripts/processar_unificacao_matching.py",
                "Unificação e Matching Final"
            )
            
            if not sucesso:
                raise Exception("Script de unificação falhou")
            
            # Encontrar arquivos gerados
            arquivos_final = glob.glob("data/processed/df_final_trabalhando_*.zip")
            arquivos_no_match = glob.glob("data/processed/df_no_match_*.zip")
            
            if not arquivos_final:
                raise Exception("Arquivo df_final_trabalhando_*.zip não foi gerado")
            
            arquivo_final = max(arquivos_final, key=os.path.getmtime)
            self.log_arquivo(arquivo_final)
            
            if arquivos_no_match:
                arquivo_no_match = max(arquivos_no_match, key=os.path.getmtime)
                self.log_arquivo(arquivo_no_match)
            
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(12, "Unificação e Matching Final", "SUCESSO", duracao)
            
            return True
            
        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(12, "Unificação e Matching Final", "ERRO", duracao)
            self.log_erro("Etapa 12", str(e))
            return False
    
    def gerar_relatorio(self):
        """Gera relatório final do pipeline"""
        tempo_total = (datetime.now() - self.inicio).total_seconds()
        
        print("\n\n" + "="*70)
        print(" "*15 + "RELATÓRIO FINAL DO PIPELINE")
        print("="*70)
        
        # Resumo de etapas
        print("\nEtapas Executadas:")
        print("-" * 70)
        for num, nome, status, duracao in self.etapas:
            duracao_str = f"{duracao:>6.1f}s" if duracao else "       "
            status_symbol = "[OK]" if status == "SUCESSO" else "[ERRO]"
            print(f"{status_symbol} [{num}] {nome:<50} {duracao_str}")
        
        # Resumo de erros
        if self.erros:
            print("\nErros Encontrados:")
            print("-" * 70)
            for etapa, mensagem in self.erros:
                print(f"[ERRO] [{etapa}] {mensagem}")
        else:
            print("\n[OK] Nenhum erro encontrado!")
        
        # Arquivos gerados
        if self.arquivos_gerados:
            print("\nArquivos Gerados:")
            print("-" * 70)
            for arquivo in self.arquivos_gerados:
                tamanho = os.path.getsize(arquivo) / (1024*1024)  # MB
                print(f"  [*] {os.path.basename(arquivo):<50} ({tamanho:>6.1f} MB)")
        
        # Tempo total
        print("\n" + "="*70)
        print(f"Tempo Total de Execução: {tempo_total/60:.1f} minutos ({tempo_total:.0f} segundos)")
        print("="*70 + "\n")
        
        # Status final
        if not self.erros:
            print("*** PIPELINE CONCLUIDO COM SUCESSO! ***\n")
            return True
        else:
            print("*** PIPELINE CONCLUIDO COM ERROS ***\n")
            return False
    
    def executar(self):
        """Executa o pipeline completo"""
        print("\n" + "#"*70)
        print("#" + " "*68 + "#")
        print("#" + " "*15 + "PIPELINE COMPLETO DE NOTAS FISCAIS (NFe)" + " "*12 + "#")
        print("#" + " "*68 + "#")
        print("#"*70 + "\n")
        
        print(f"Início: {self.inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Limpar arquivos antigos ANTES de começar
        self.limpar_arquivos_antigos()
        
        # Executar etapas
        etapas = [
            ("Carregamento e Pré-processamento", self.etapa_1_carregamento),
            ("Processamento de Vencimento", self.etapa_2_vencimento),
            ("Limpeza de Descrições", self.etapa_3_limpeza),
            ("Enriquecimento com Municípios", self.etapa_4_enriquecimento),
            ("Carregamento da Base ANVISA", self.etapa_5_carregamento_anvisa),
            ("Otimização de Memória", self.etapa_6_otimizacao_memoria),
            ("Matching NFe x ANVISA", self.etapa_7_matching_anvisa),
            ("Matching Manual (Google Sheets)", self.etapa_8_matching_manual),
            ("Separação e Filtragem", self.etapa_9_separacao),
            ("Extração de Nomes", self.etapa_10_extracao_nomes),
            ("Refinamento de Nomes", self.etapa_11_refinamento_nomes),
            ("Unificação e Matching Final", self.etapa_12_unificacao_matching),
            # Próximas etapas virão aqui
]
        
        etapas_executadas = 0
        for nome, funcao in etapas:
            if funcao():
                etapas_executadas += 1
            else:
                print(f"\n[AVISO] Pipeline interrompido em: {nome}")
                break
        
        # Gerar relatório
        sucesso = self.gerar_relatorio()
        
        return sucesso


def analisar_eans_sem_match(arquivo_matched, exportar=True):
    """
    [DEBUG] Analisa EANs que não tiveram match com a base ANVISA
    
    Parâmetros:
        arquivo_matched (str): Caminho do arquivo nfe_matched_*.csv
        exportar (bool): Se True, exporta os resultados em CSV
    """
    print("\n" + "="*80)
    print(" "*20 + "[DEBUG] ANÁLISE DE EANs SEM MATCH")
    print("="*80 + "\n")
    
    try:
        # Carregar arquivo
        print("[INFO] Carregando arquivo de matching...")
        df = pd.read_csv(arquivo_matched, sep=';', dtype={'codigo_ean': str})
        print(f"[OK] {len(df):,} registros carregados\n")
        
        # 1️⃣ Filtrar linhas onde 'PRODUTO' é nulo
        mask_nulo = df['PRODUTO'].isnull() | (df['PRODUTO'].astype(str).str.lower() == 'nan')
        df_produto_nulo = df.loc[mask_nulo].copy()
        
        total_sem_match = len(df_produto_nulo)
        pct_sem_match = (total_sem_match / len(df)) * 100
        
        print(f"[INFO] Registros sem PRODUTO (sem match): {total_sem_match:,} ({pct_sem_match:.2f}%)\n")
        
        if total_sem_match == 0:
            print("[OK] Nenhum EAN sem match encontrado! ✅\n")
            return
        
        # 2️⃣ Contar frequência de EANs
        ean_counts = (
            df_produto_nulo['codigo_ean']
            .value_counts(dropna=False)
            .rename('Frequencia')
        )
        
        # 3️⃣ Manter apenas a descrição mais frequente por EAN
        desc_counts = (
            df_produto_nulo
            .value_counts(['codigo_ean', 'descricao_produto'])
            .reset_index(name='freq_desc')
        )
        
        idx_max = (
            desc_counts
            .groupby('codigo_ean', observed=True)['freq_desc']
            .idxmax()
        )
        
        descricao_top = desc_counts.loc[idx_max, ['codigo_ean', 'descricao_produto']]
        
        # 4️⃣ Unir com contagens de EAN
        resultado = (
            descricao_top
            .merge(ean_counts, left_on='codigo_ean', right_index=True, how='left')
            .sort_values('Frequencia', ascending=False)
            .reset_index(drop=True)
        )
        
        # 5️⃣ Agregar por EAN com métricas financeiras
        df_produto_nulo['valor_produtos'] = pd.to_numeric(df_produto_nulo['valor_produtos'], errors='coerce')
        
        top_ean_metricas = (
            df_produto_nulo.groupby('codigo_ean', observed=False)
            .agg(
                Frequencia=('codigo_ean', 'size'),
                Valor_Total=('valor_produtos', 'sum'),
                Valor_Medio=('valor_produtos', 'mean')
            )
            .sort_values(by=['Frequencia', 'Valor_Total'], ascending=[False, False])
            .reset_index()
        )
        
        # 6️⃣ Exibir resultados
        print("="*80)
        print("TOP 50 EANs SEM MATCH - Ordenado por Frequência")
        print("="*80)
        print(resultado.head(50).to_string(index=False))
        
        # 7️⃣ Exibir com métricas financeiras
        print("\n" + "="*80)
        print("TOP 50 EANs SEM MATCH - Ordenado por Frequência e Valor Total")
        print("="*80 + "\n")
        
        def format_brl(x):
            if pd.isna(x):
                return 'N/A'
            return f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        
        top_ean_metricas_display = top_ean_metricas.head(50).copy()
        top_ean_metricas_display['Valor_Total'] = top_ean_metricas_display['Valor_Total'].apply(format_brl)
        top_ean_metricas_display['Valor_Medio'] = top_ean_metricas_display['Valor_Medio'].apply(format_brl)
        
        print(top_ean_metricas_display.to_string(index=False))
        
        # 8️⃣ Exportar para CSV
        if exportar:
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            
            # Exportar análise simples
            arquivo_saida1 = f"data/processed/debug_eans_sem_match_{timestamp}.csv"
            resultado.to_csv(arquivo_saida1, sep=';', index=False, encoding='utf-8')
            print(f"\n[OK] Análise simples exportada: {arquivo_saida1}")
            
            # Exportar com métricas financeiras
            arquivo_saida2 = f"data/processed/debug_eans_metricas_{timestamp}.csv"
            top_ean_metricas.to_csv(arquivo_saida2, sep=';', index=False, encoding='utf-8')
            print(f"[OK] Análise com métricas exportada: {arquivo_saida2}")
        
        print("\n" + "="*80)
        print("[OK] Análise de DEBUG concluída!")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"[ERRO] Erro durante análise DEBUG: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Função principal"""
    
    # ⚙️ TOGGLE DE DEBUG - Altere para True para executar análise de EANs sem match
    DEBUG_ENABLED = False
    
    # Verificar se arquivo de entrada existe
    if not os.path.exists("nfe/nfe.csv"):
        print("[ERRO] Arquivo 'nfe/nfe.csv' nao encontrado!")
        print("\nColoque seu arquivo CSV de NFe em:")
        print("  nfe/nfe.csv")
        sys.exit(1)
    
    # Criar diretórios necessários
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/raw", exist_ok=True)
    
    # Executar pipeline
    pipeline = PipelineNFe()
    sucesso = pipeline.executar()
    
    # [DEBUG] Executar análise de EANs sem match se toggle estiver ativo
    if DEBUG_ENABLED and sucesso:
        # Encontrar o arquivo matched mais recente
        arquivos_matched = glob.glob("data/processed/nfe_matched_*.csv")
        if arquivos_matched:
            arquivo_recente = max(arquivos_matched, key=os.path.getmtime)
            print(f"\n[DEBUG] Analisando arquivo: {os.path.basename(arquivo_recente)}")
            analisar_eans_sem_match(arquivo_recente, exportar=True)
    
    # Retornar código de saída
    sys.exit(0 if sucesso else 1)



if __name__ == "__main__":
    main()
