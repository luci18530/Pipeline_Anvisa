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
        
        # Executar etapas
        etapas = [
            ("Carregamento e Pré-processamento", self.etapa_1_carregamento),
            ("Processamento de Vencimento", self.etapa_2_vencimento),
            ("Limpeza de Descrições", self.etapa_3_limpeza),
            ("Enriquecimento com Municípios", self.etapa_4_enriquecimento),
            ("Carregamento da Base ANVISA", self.etapa_5_carregamento_anvisa),
            ("Otimização de Memória", self.etapa_6_otimizacao_memoria),
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


def main():
    """Função principal"""
    
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
    
    # Retornar código de saída
    sys.exit(0 if sucesso else 1)


if __name__ == "__main__":
    main()
