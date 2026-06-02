"""
Script Principal - Pipeline Completo de Processamento de Notas Fiscais (NFe)
Executa todas as etapas do pipeline em sequência
"""

import sys
import os
import glob
import json
import subprocess
import shutil
import threading
import time
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import Optional
from uuid import uuid4

if str(Path(__file__).resolve().parents[2]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline_config import get_toggle
from pipelines.nfe.src.encoding_guard import assert_no_encoding_corruption

PIPELINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PIPELINE_ROOT.parent.parent


class PipelineNFe:
    """Orquestrador do pipeline completo de NFe"""
    
    def __init__(self, modo_rapido: bool | None = None, start_stage: int = 1):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
        self.inicio = datetime.now()
        self.etapas = []
        self.arquivos_gerados = []
        self.erros = []
        self.max_execucoes = 2  # Manter os últimos 2 processamentos
        self.pipeline_root = PIPELINE_ROOT
        self.project_root = PROJECT_ROOT
        self.scripts_dir = self.pipeline_root / "scripts"
        self.log_dir = self.project_root / "data" / "processed" / "logs"
        self.log_path = self.log_dir / f"pipeline_nfe_{self.run_id}.jsonl"
        self.start_stage = start_stage
        
        # Modo pipeline rápido: desativa exportações intermediárias
        if modo_rapido is None:
            self.modo_rapido = bool(get_toggle("pipeline", "modo_rapido", default=False))
        else:
            self.modo_rapido = modo_rapido
        
        if self.modo_rapido:
            print("\n" + "="*60)
            print("[MODO RAPIDO] Exportacoes intermediarias DESATIVADAS")
            print("  - Apenas arquivo final será exportado")
            print("  - Processamento 100% em memória")
            print("="*60)
        # Cache em memória para etapas iniciais (1-3)
        self.df_nfe = None
        # Cache em memória para fluxo trabalhando (etapas 9-11)
        self.df_trabalhando = None
        self.df_trabalhando_nomes = None
        self.df_trabalhando_refinado = None
        # Cache de base ANVISA para reutilização
        self.df_anvisa_base = None
        # Cache em memória para etapas 18-21
        self.df_etapa18 = None
        self.df_etapa19 = None
        self.df_etapa20 = None
        self.df_etapa21 = None
        # Cache em memória para etapas 12-17
        self.df_etapa12_final_trabalhando = None
        self.df_etapa12_no_match = None
        self.df_etapa13_match_apresentacao_unica = None
        self.df_etapa13_trabalhando_restante = None
        self.df_etapa14_final_enriquecido = None
        self.df_etapa15_resultado_matching_hibrido = None
        self.df_etapa16_matched_hibrido = None
        self.df_etapa16_restante = None
        self.df_etapa16_atributos_ia = None
        self.df_etapa17_consolidado = None
        self.df_etapa17_5_unidade_caixa = None

    def _emit_structured_log(self, level: str, event: str, **fields) -> None:
        payload = {
            "timestamp": datetime.now().isoformat(),
            "run_id": self.run_id,
            "level": level.upper(),
            "event": event,
        }
        payload.update(fields)
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fp:
                fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as exc:
            print(f"[AVISO] Falha ao gravar log estruturado: {exc}")

    def _iniciar_heartbeat_etapa(self, etapa_numero: int, etapa_nome: str, intervalo_segundos: int = 60):
        """Emite heartbeat periódico para etapas longas."""
        stop_event = threading.Event()
        inicio = time.time()

        def _loop():
            while not stop_event.wait(intervalo_segundos):
                elapsed = round(time.time() - inicio, 1)
                self._emit_structured_log(
                    "info",
                    "etapa_heartbeat",
                    etapa_numero=etapa_numero,
                    etapa_nome=etapa_nome,
                    elapsed_segundos=elapsed,
                )
                print(f"[HEARTBEAT] Etapa {etapa_numero} em execução há {elapsed/60:.1f} min...")

        thread = threading.Thread(target=_loop, daemon=True)
        thread.start()
        return stop_event
    
    def verificar_input_mudou(self):
        """Verifica se o arquivo de input mudou desde a última execução"""
        arquivo_input = self.project_root / "nfe" / "nfe.csv"
        arquivo_timestamp = self.project_root / "data" / "processed" / ".nfe_input_timestamp.txt"
        
        if not arquivo_input.exists():
            return False
        
        # Obter timestamp do input atual
        timestamp_atual = arquivo_input.stat().st_mtime
        
        # Ler timestamp da última execução
        if arquivo_timestamp.exists():
            try:
                with arquivo_timestamp.open('r', encoding='utf-8') as f:
                    timestamp_anterior = float(f.read().strip())
                
                if timestamp_atual != timestamp_anterior:
                    print("\n" + "="*60)
                    print("[AVISO] Arquivo de input (nfe.csv) foi MODIFICADO!")
                    print(f"  Última execução: {datetime.fromtimestamp(timestamp_anterior).strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"  Arquivo atual:   {datetime.fromtimestamp(timestamp_atual).strftime('%Y-%m-%d %H:%M:%S')}")
                    print("  FORÇANDO LIMPEZA COMPLETA de data/processed/")
                    print("="*60)
                    return True
            except Exception as exc:
                self._emit_structured_log(
                    "warning",
                    "timestamp_check_failed",
                    arquivo=str(arquivo_timestamp),
                    erro=str(exc),
                )
                print(f"[AVISO] Não foi possível ler timestamp anterior: {exc}")
        
        return False
    
    def salvar_timestamp_input(self):
        """Salva timestamp do arquivo de input para referência futura"""
        arquivo_input = self.project_root / "nfe" / "nfe.csv"
        arquivo_timestamp = self.project_root / "data" / "processed" / ".nfe_input_timestamp.txt"
        
        if arquivo_input.exists():
            timestamp_atual = arquivo_input.stat().st_mtime
            arquivo_timestamp.parent.mkdir(parents=True, exist_ok=True)
            with arquivo_timestamp.open('w', encoding='utf-8') as f:
                f.write(str(timestamp_atual))
    
    def limpar_arquivos_antigos(self):
        """Remove arquivos de processamentos antigos, mantendo apenas os últimos N"""
        print("\n" + "="*60)
        print("[LIMPEZA] Removendo arquivos de processamentos antigos...")
        print("="*60)
        
        try:
            # Diretórios a limpar
            dirs_limpar = [str(self.project_root / "data" / "processed")]
            
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
                    'no_match': 'df_no_match_*.zip',
                    'match_apresentacao_unica': 'df_match_apresentacao_unica_*.zip',
                    'trabalhando_restante': 'df_trabalhando_restante_*.zip',
                    'etapa14_extracao_ia': 'df_etapa14_extracao_ia*.zip',
                    'etapa14_enriquecido': 'df_etapa14_final_enriquecido*.zip',
                    'etapa15_matching': 'df_etapa15_resultado_matching_hibrido*.zip',
                    'etapa16_matched': 'df_etapa16_matched_hibrido*.zip',
                    'etapa16_restante': 'df_etapa16_restante*.zip',
                    'etapa16_atributos_ia': 'df_etapa16_atributos_ia*.zip',
                    'etapa17_consolidado': 'df_etapa17_consolidado_final*.zip',
                    'etapa17_5_unidade_caixa': 'df_etapa17_5_unidade_caixa*.zip',
                    'etapa17_5_resumo': 'df_etapa17_5_unidade_caixa*.csv',
                    'etapa18_sobrepreco': 'df_etapa18_sobrepreco*.zip',
                    'etapa18_resumo': 'df_etapa18_sobrepreco_resumo*.csv',
                    'etapa18_stats': 'df_etapa18_sobrepreco_stats*.csv',
                    'etapa19_ajuste': 'df_etapa19_valores_ajustados*.zip',
                    'etapa19_resumo': 'df_etapa19_resumo_ajuste*.csv',
                    'etapa20_classificacao': 'df_etapa20_classificacao_esfera*.zip',
                    'etapa20_distribuicao': 'df_etapa20_distribuicao_esfera*.csv',
                    'etapa21_unidades': 'df_etapa21_unidades_padronizadas*.zip',
                    'etapa21_resumo': 'df_etapa21_unidades_resumo*.csv',
                    'etapa21_metricas': 'df_etapa21_unidades_metricas*.csv',
                    'etapa22_central': 'QlikView/df_central.csv',
                    'etapa22_tabelas': 'QlikView/df_*.csv',
                    'etapa22_vencimento': 'QlikView/nfe_vencimento*.csv',
                }
                
                for tipo, padrao in padroes.items():
                    base_glob = self.project_root if padrao.startswith("QlikView/") else Path(diretorio)
                    arquivos = sorted(
                        glob.glob(str(base_glob / padrao)),
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
        self._emit_structured_log(
            "info",
            "etapa_status",
            etapa_numero=numero,
            etapa_nome=nome,
            status=status,
            duracao_segundos=duracao,
        )
    
    def log_arquivo(self, caminho):
        """Registra um arquivo gerado"""
        self.arquivos_gerados.append(caminho)
    
    def log_erro(self, etapa, mensagem):
        """Registra um erro"""
        self.erros.append((etapa, mensagem))
        self._emit_structured_log(
            "error",
            "etapa_erro",
            etapa=etapa,
            mensagem=mensagem,
        )
    
    def executar_script(self, script_path, nome_etapa, timeout_customizado=None):
        """Executa um script Python e retorna True se bem-sucedido"""
        try:
            script_path = Path(script_path)
            if not script_path.is_absolute():
                script_path = self.pipeline_root / script_path

            # Timeout padrão: 30 minutos (otimizado para processar 2.7M registros)
            # Timeout customizado pode ser passado por etapa
            timeout = timeout_customizado if timeout_customizado else 1800  # 30 minutos
            
            print(f"\n[EXECUTANDO] {nome_etapa}... ({script_path.name})")
            print(f"[INFO] Timeout configurado: {timeout//60} minutos")
            self._emit_structured_log(
                "info",
                "script_execucao_inicio",
                etapa=nome_etapa,
                script=str(script_path),
                timeout_segundos=timeout,
            )
            
            # Executa subprocessos em modo unbuffered para logs em tempo real.
            cmd = [sys.executable, "-u", str(script_path)]
            inicio_execucao = time.time()
            ultimo_heartbeat = inicio_execucao
            heartbeat_interval = 60

            processo = subprocess.Popen(
                cmd,
                cwd=str(self.project_root),
            )
            while True:
                retorno = processo.poll()
                agora = time.time()
                decorrido = agora - inicio_execucao

                if retorno is not None:
                    break

                if decorrido > timeout:
                    processo.kill()
                    processo.wait()
                    raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)

                if agora - ultimo_heartbeat >= heartbeat_interval:
                    self._emit_structured_log(
                        "info",
                        "script_execucao_heartbeat",
                        etapa=nome_etapa,
                        script=str(script_path),
                        elapsed_segundos=round(decorrido, 1),
                    )
                    print(f"[HEARTBEAT] {nome_etapa} em execução há {decorrido/60:.1f} min...")
                    ultimo_heartbeat = agora

                time.sleep(1)

            self._emit_structured_log(
                "info",
                "script_execucao_fim",
                etapa=nome_etapa,
                script=str(script_path),
                returncode=retorno,
            )
            return retorno == 0
        except subprocess.TimeoutExpired:
            self.log_erro(nome_etapa, f"Timeout (>{timeout//60} minutos)")
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
            # Executar diretamente em memória (sem salvar CSV intermediário)
            from pipelines.nfe.src.nfe_etapa01_carregamento import carregar_e_processar_nfe

            arquivo_entrada = "nfe/nfe.csv"
            data_minima = "2020-01-01"

            if not os.path.exists(arquivo_entrada):
                raise Exception(f"Arquivo não encontrado: {arquivo_entrada}")

            print(f"[INFO] Arquivo de entrada: {arquivo_entrada}")
            print(f"[INFO] Data mínima: {data_minima}")

            self.df_nfe = carregar_e_processar_nfe(
                arquivo_entrada,
                data_minima=data_minima
            )

            # Validação básica em memória
            print("\n[VALIDANDO] Dados carregados (memória)...")
            colunas_essenciais = {
                'descricao_produto', 'data_emissao', 'valor_produtos', 'quantidade', 'chave_codigo'
            }
            faltantes = colunas_essenciais - set(self.df_nfe.columns)
            if faltantes:
                raise Exception(f"Colunas essenciais faltantes: {sorted(faltantes)}")
            
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
            # Executar diretamente em memória (sem reabrir CSV intermediário)
            from pipelines.nfe.src.nfe_etapa02_vencimento import processar_vencimento_nfe, salvar_dados_vencimento

            if self.df_nfe is None:
                raise Exception("Etapa 1 não executada em memória. Execute a etapa 1 primeiro.")

            df_base, df_venc = processar_vencimento_nfe(self.df_nfe)

            # Salvar apenas o entregável de vencimento (data/external)
            caminho_venc = salvar_dados_vencimento(df_venc, formato='csv')
            self.log_arquivo(caminho_venc)

            # Atualizar cache para próxima etapa
            self.df_nfe = df_base
            
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
            # Executar diretamente em memória
            from pipelines.nfe.src.nfe_etapa03_limpeza import limpar_descricoes, salvar_dados_limpos

            if self.df_nfe is None:
                raise Exception("Etapas 1-2 não executadas em memória. Execute as etapas anteriores primeiro.")

            df_limpo = limpar_descricoes(self.df_nfe)

            # Salvar apenas o resultado final da etapa 3 (para etapas seguintes)
            arquivo_saida = salvar_dados_limpos(df_limpo)
            self.log_arquivo(arquivo_saida)

            # Atualizar cache para próximas etapas
            self.df_nfe = df_limpo
            
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
        print("ETAPA 4: ENRIQUECIMENTO COM DADOS DE MUNICIPIO")
        print("="*60)
        
        try:
            # Executar diretamente em memória (sem reabrir CSV intermediário)
            from pipelines.nfe.src.nfe_etapa04_enriquecimento import (
                verificar_arquivo_codigos,
                carregar_codigos_municipio,
                enriquecer_com_municipios,
            )

            if self.df_nfe is None:
                raise Exception("Etapas 1-3 não executadas em memória. Execute as etapas anteriores primeiro.")

            print("[VALIDANDO] Arquivo de códigos de município...")
            verificar_arquivo_codigos()
            df_codigos = carregar_codigos_municipio()

            df_enriquecido = enriquecer_com_municipios(self.df_nfe, df_codigos)

            # Salvar apenas o resultado final da etapa 4 (para etapas seguintes)
            os.makedirs("data/processed", exist_ok=True)
            arquivo_saida = os.path.join("data/processed", "nfe_etapa04_enriquecido.csv")
            df_enriquecido.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8')
            self.log_arquivo(arquivo_saida)

            # Validação rápida em memória
            print("\n[VALIDANDO] Dados enriquecidos (memória)...")
            if 'municipio' not in df_enriquecido.columns:
                raise Exception("Coluna 'municipio' não foi criada no enriquecimento")
            pct_match = (df_enriquecido['municipio'].notna().mean() * 100)
            if pct_match < 90:
                raise Exception(f"Enriquecimento baixo: {pct_match:.1f}% de municípios preenchidos")

            # Atualizar cache para próximas etapas
            self.df_nfe = df_enriquecido
            
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
            base_anvisa = self.project_root / "output" / "anvisa" / "baseANVISA.csv"
            dtypes_anvisa = self.project_root / "output" / "anvisa" / "baseANVISA_dtypes.json"

            if base_anvisa.exists() and dtypes_anvisa.exists():
                print("[INFO] Base ANVISA já preparada. Pulando reprocessamento.")
                duracao = (datetime.now() - inicio).total_seconds()
                self.log_etapa(5, "Carregamento da Base ANVISA (CMED)", "PULADO", duracao)
                return True

            # Executar script de carregamento da base ANVISA
            script_anvisa = (
                self.project_root / "pipelines" / "anvisa_base" / "scripts" / "processar_base_anvisa.py"
            )
            sucesso = self.executar_script(
                script_anvisa,
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
            # Executar diretamente em memória (sem reabrir CSV intermediário)
            from pipelines.nfe.src.nfe_etapa06_otimizacao_memoria import preparar_nfe_para_matching

            if self.df_nfe is None:
                raise Exception("Etapas 1-4 não executadas em memória. Execute as etapas anteriores primeiro.")

            self.df_nfe = preparar_nfe_para_matching(self.df_nfe)
            
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
            if self.df_nfe is None:
                raise Exception("Etapas 1-6 não executadas em memória. Execute as etapas anteriores primeiro.")

            from pipelines.nfe.src.nfe_etapa07_matching_anvisa import processar_matching_anvisa
            from pipelines.anvisa_base.src.anvisa_base import processar_base_anvisa

            print("[INFO] Carregando base ANVISA (CMED) em memória...")
            dfpre_anvisa = processar_base_anvisa()

            print("[INFO] Iniciando matching NFe x ANVISA em memória...")
            df_matched = processar_matching_anvisa(self.df_nfe, dfpre_anvisa)

            # Cache da base ANVISA para etapas futuras (12, 13, 15)
            self.df_anvisa_base = dfpre_anvisa

            # Atualizar cache para próximas etapas
            self.df_nfe = df_matched

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
            if self.df_nfe is None:
                raise Exception("Etapas 1-7 não executadas em memória. Execute as etapas anteriores primeiro.")

            from pipelines.nfe.src.nfe_etapa08_matching_manual import processar_matching_manual

            print("[INFO] Iniciando matching manual em memória...")
            df_manual, arquivo_saida = processar_matching_manual(self.df_nfe, exportar=True)

            # Atualizar cache para próximas etapas
            self.df_nfe = df_manual

            if arquivo_saida:
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
            # Executar script de separação (file-based)
            sucesso = self.executar_script(
                "scripts/processar_separacao.py",
                "Separação e Filtragem"
            )

            if not sucesso:
                raise Exception("Script de separação falhou")

            arquivo_completo = os.path.join("data/processed", "df_etapa09_completo.zip")
            arquivo_trabalhando = os.path.join("data/processed", "df_etapa09_trabalhando.zip")

            if os.path.exists(arquivo_completo):
                self.log_arquivo(arquivo_completo)
            if os.path.exists(arquivo_trabalhando):
                self.log_arquivo(arquivo_trabalhando)
                # Carregar em memória para etapas 10-11
                self.df_trabalhando = pd.read_csv(
                    arquivo_trabalhando,
                    sep=';'
                )

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
            from pipelines.nfe.src.nfe_etapa10_extracao_nomes import processar_extracao_nomes

            if self.df_trabalhando is None:
                arquivo_trabalhando = os.path.join("data/processed", "df_etapa09_trabalhando.zip")
                if not os.path.exists(arquivo_trabalhando):
                    raise Exception("Arquivo df_etapa09_trabalhando.zip não encontrado")
                self.df_trabalhando = pd.read_csv(arquivo_trabalhando, sep=';')

            df_resultado = processar_extracao_nomes(
                df_entrada=self.df_trabalhando,
                exportar=True,
                diretorio_saida="data/processed",
            )

            self.df_trabalhando_nomes = df_resultado
            self.df_trabalhando_refinado = df_resultado

            arquivo_saida = os.path.join("data/processed", "df_etapa10_trabalhando_nomes.zip")
            if os.path.exists(arquivo_saida):
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
            from pipelines.nfe.src.nfe_etapa11_refinamento_nomes import processar_refinamento_nomes

            if self.df_trabalhando_nomes is None:
                arquivo_nomes = os.path.join("data/processed", "df_etapa10_trabalhando_nomes.zip")
                if not os.path.exists(arquivo_nomes):
                    raise Exception("Arquivo df_etapa10_trabalhando_nomes.zip não encontrado")
                self.df_trabalhando_nomes = pd.read_csv(arquivo_nomes, sep=';')

            df_resultado = processar_refinamento_nomes(
                df_entrada=self.df_trabalhando_nomes,
                exportar=True,
                diretorio_saida="data/processed",
            )

            # Atualizar cache para a etapa 12 usar a versão realmente refinada
            self.df_trabalhando_refinado = df_resultado

            arquivo_saida = os.path.join("data/processed", "df_etapa11_trabalhando_refinado.zip")
            if os.path.exists(arquivo_saida):
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
            from pipelines.nfe.src.nfe_etapa12_unificacao_matching import processar_unificacao_matching

            df_entrada = self.df_trabalhando_refinado
            exportar = not self.modo_rapido
            df_final, df_no_match, output_path, no_match_path = processar_unificacao_matching(
                df_entrada=df_entrada,
                exportar=exportar,
                df_anvisa=self.df_anvisa_base,
            )

            self.df_etapa12_final_trabalhando = df_final
            self.df_etapa12_no_match = df_no_match

            if output_path is not None:
                self.log_arquivo(str(output_path))
            if no_match_path is not None:
                self.log_arquivo(str(no_match_path))

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(12, "Unificação e Matching Final", "SUCESSO", duracao)

            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(12, "Unificação e Matching Final", "ERRO", duracao)
            self.log_erro("Etapa 12", str(e))
            return False
    
    def etapa_13_matching_apresentacao_unica(self):
        """Etapa 13: Matching de produtos com apresentação única"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 13: MATCHING DE APRESENTAÇÃO ÚNICA")
        print("="*60)
        
        try:
            from pipelines.nfe.src.nfe_etapa13_matching_apresentacao_unica import processar_matching_apresentacao_unica

            df_entrada = self.df_etapa12_final_trabalhando
            exportar = not self.modo_rapido
            df_sucesso, df_restante, output_path_sucesso, output_path_trabalhando = (
                processar_matching_apresentacao_unica(
                    df_entrada=df_entrada,
                    exportar=exportar,
                    df_anvisa=self.df_anvisa_base,
                )
            )

            self.df_etapa13_match_apresentacao_unica = df_sucesso
            self.df_etapa13_trabalhando_restante = df_restante

            if output_path_sucesso is not None:
                self.log_arquivo(str(output_path_sucesso))
            if output_path_trabalhando is not None:
                self.log_arquivo(str(output_path_trabalhando))

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(13, "Matching de Apresentação Única", "SUCESSO", duracao)

            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(13, "Matching de Apresentação Única", "ERRO", duracao)
            self.log_erro("Etapa 13", str(e))
            return False
    
    def etapa_14_extracao_ia(self):
        """Etapa 14: Extração de atributos usando IA (Gemini)"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 14: EXTRAÇÃO DE ATRIBUTOS COM IA")
        print("="*60)
        
        try:
            from pipelines.nfe.src.nfe_etapa14_extracao_ia import processar_extracao_ia

            df_entrada = self.df_etapa13_trabalhando_restante
            exportar = not self.modo_rapido
            df_final = processar_extracao_ia(
                df_entrada=df_entrada,
                exportar=exportar,
            )

            self.df_etapa14_final_enriquecido = df_final

            arquivos_ia = glob.glob("data/processed/df_etapa14_extracao_ia.zip")
            arquivos_enriquecido = glob.glob("data/processed/df_etapa14_final_enriquecido.zip")
            
            if arquivos_ia:
                arquivo_ia = max(arquivos_ia, key=os.path.getmtime)
                self.log_arquivo(arquivo_ia)
            
            if arquivos_enriquecido:
                arquivo_enriquecido = max(arquivos_enriquecido, key=os.path.getmtime)
                self.log_arquivo(arquivo_enriquecido)

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(14, "Extração de Atributos com IA", "SUCESSO", duracao)

            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(14, "Extração de Atributos com IA", "ERRO", duracao)
            self.log_erro("Etapa 14", str(e))
            return False
    
    def etapa_15_matching_hibrido(self):
        """Etapa 15: Matching híbrido ponderado"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 15: MATCHING HIBRIDO PONDERADO")
        print("="*60)
        
        try:
            from pipelines.nfe.src.nfe_etapa15_matching_hibrido import processar_matching_hibrido

            df_entrada = self.df_etapa14_final_enriquecido
            exportar = not self.modo_rapido
            df_resultado = processar_matching_hibrido(
                df_entrada=df_entrada,
                df_anvisa=self.df_anvisa_base,
                exportar=exportar,
            )

            if df_resultado is None:
                raise Exception("Etapa 15 retornou sem resultado")

            self.df_etapa15_resultado_matching_hibrido = df_resultado

            arquivos_hibrido = glob.glob("data/processed/df_etapa15_resultado_matching_hibrido.zip")
            
            if arquivos_hibrido:
                arquivo_hibrido = max(arquivos_hibrido, key=os.path.getmtime)
                self.log_arquivo(arquivo_hibrido)

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(15, "Matching Híbrido Ponderado", "SUCESSO", duracao)

            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(15, "Matching Híbrido Ponderado", "ERRO", duracao)
            self.log_erro("Etapa 15", str(e))
            return False
    
    def etapa_16_finalizacao_pipeline(self):
        """Etapa 16: Finalização do pipeline NFe"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 16: FINALIZAÇÃO DO PIPELINE")
        print("="*60)
        
        try:
            from pipelines.nfe.src.nfe_etapa16_finalizacao_pipeline import processar_finalizacao

            df_entrada = self.df_etapa15_resultado_matching_hibrido
            exportar = not self.modo_rapido
            df_matched, df_restante, df_ia = processar_finalizacao(
                df_entrada=df_entrada,
                exportar=exportar,
            )

            if df_matched is None or df_restante is None:
                raise Exception("Etapa 16 retornou sem resultado")

            self.df_etapa16_matched_hibrido = df_matched
            self.df_etapa16_restante = df_restante
            self.df_etapa16_atributos_ia = df_ia

            arquivos_matched = glob.glob("data/processed/df_etapa16_matched_hibrido.zip")
            arquivos_restante = glob.glob("data/processed/df_etapa16_restante.zip")
            arquivos_ia = glob.glob("data/processed/df_etapa16_atributos_ia.zip")
            
            if arquivos_matched:
                arquivo_matched = max(arquivos_matched, key=os.path.getmtime)
                self.log_arquivo(arquivo_matched)
            
            if arquivos_restante:
                arquivo_restante = max(arquivos_restante, key=os.path.getmtime)
                self.log_arquivo(arquivo_restante)
            
            if arquivos_ia:
                arquivo_ia = max(arquivos_ia, key=os.path.getmtime)
                self.log_arquivo(arquivo_ia)

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(16, "Finalização do Pipeline", "SUCESSO", duracao)

            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(16, "Finalização do Pipeline", "ERRO", duracao)
            self.log_erro("Etapa 16", str(e))
            return False
    
    def etapa_17_consolidacao_final(self):
        """Etapa 17: Consolidação final de todos os resultados"""
        inicio = datetime.now()
        
        print("\n" + "="*60)
        print("ETAPA 17: CONSOLIDAÇÃO FINAL")
        print("="*60)
        
        try:
            artefato_etapa16 = self.project_root / "data" / "processed" / "df_etapa16_matched_hibrido.zip"
            artefato_etapa15 = self.project_root / "data" / "processed" / "df_etapa15_resultado_matching_hibrido.zip"
            artefato_etapa9 = self.project_root / "data" / "processed" / "df_etapa09_completo.zip"
            artefato_etapa13 = self.project_root / "data" / "processed" / "df_etapa13_match_apresentacao_unica.zip"

            # Auto-recovery para execucao retomada da etapa 17 sem artefatos da etapa 16.
            if self.df_etapa16_matched_hibrido is None and not artefato_etapa16.exists():
                print("[AVISO] Artefato da Etapa 16 ausente: df_etapa16_matched_hibrido.zip")
                if artefato_etapa15.exists():
                    print("[INFO] Regerando Etapa 16 automaticamente a partir da Etapa 15...")
                    from pipelines.nfe.src.nfe_etapa16_finalizacao_pipeline import processar_finalizacao

                    df_matched, df_restante, df_ia = processar_finalizacao(
                        df_entrada=self.df_etapa15_resultado_matching_hibrido,
                        exportar=True,
                    )
                    if df_matched is None or df_restante is None:
                        raise Exception("Auto-recovery da Etapa 16 falhou")

                    self.df_etapa16_matched_hibrido = df_matched
                    self.df_etapa16_restante = df_restante
                    self.df_etapa16_atributos_ia = df_ia
                else:
                    print("[AVISO] Nao foi possivel regerar Etapa 16: artefato da Etapa 15 ausente.")

            faltantes_criticos = []
            if not artefato_etapa9.exists():
                faltantes_criticos.append(("Etapa 9", artefato_etapa9))
            if self.df_etapa13_match_apresentacao_unica is None and not artefato_etapa13.exists():
                faltantes_criticos.append(("Etapa 13", artefato_etapa13))
            if self.df_etapa16_matched_hibrido is None and not artefato_etapa16.exists():
                faltantes_criticos.append(("Etapa 16", artefato_etapa16))

            if faltantes_criticos:
                detalhes = "\n".join(
                    f" - {etapa}: {str(caminho)}" for etapa, caminho in faltantes_criticos
                )
                raise FileNotFoundError(
                    "Etapa 17 abortada: artefatos criticos ausentes.\n"
                    f"{detalhes}\n"
                    "Execute/retome as etapas faltantes e tente novamente."
                )

            from pipelines.nfe.src.nfe_etapa17_consolidacao_final import processar_consolidacao_final

            exportar = not self.modo_rapido
            df_consolidado = processar_consolidacao_final(
                df_completo=None,
                df_apresentacao=self.df_etapa13_match_apresentacao_unica,
                df_hibrido=self.df_etapa16_matched_hibrido,
                exportar=exportar,
            )

            if df_consolidado is None:
                raise Exception("Etapa 17 retornou sem resultado")

            self.df_etapa17_consolidado = df_consolidado

            arquivos_consolidado = glob.glob("data/processed/df_etapa17_consolidado_final.zip")
            
            if arquivos_consolidado:
                arquivo_consolidado = max(arquivos_consolidado, key=os.path.getmtime)
                self.log_arquivo(arquivo_consolidado)

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(17, "Consolidação Final", "SUCESSO", duracao)

            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(17, "Consolidação Final", "ERRO", duracao)
            self.log_erro("Etapa 17", str(e))
            return False

    def etapa_18_sobrepreco(self):
        """Etapa 18: Análise de sobrepreço"""
        inicio = datetime.now()

        print("\n" + "="*60)
        print("ETAPA 18: ANALISE DE SOBREPREÇO")
        print("="*60)

        try:
            from pipelines.nfe.src.nfe_etapa17_5_conversao_unidade_caixa import (
                processar_conversao_unidade_caixa,
            )
            from pipelines.nfe.src.nfe_etapa18_sobrepreco import processar_sobrepreco

            # Etapa 18 usa a conversao economica da Etapa 17.5 quando disponivel.
            df_entrada = self.df_etapa17_5_unidade_caixa
            exportar = not self.modo_rapido
            if df_entrada is None:
                print("\n" + "="*60)
                print("ETAPA 17.5: CONVERSAO UNIDADE/CAIXA")
                print("="*60)
                df_entrada = processar_conversao_unidade_caixa(
                    df_entrada=self.df_etapa17_consolidado,
                    exportar=exportar,
                )
                self.df_etapa17_5_unidade_caixa = df_entrada
            df_resultado = processar_sobrepreco(
                df_entrada=df_entrada,
                exportar=exportar,
            )

            self.df_etapa18 = df_resultado

            arquivos = [
                "data/processed/df_etapa17_5_unidade_caixa.zip",
                "data/processed/df_etapa17_5_unidade_caixa_resumo.csv",
                "data/processed/df_etapa17_5_unidade_caixa_amostras.csv",
                "data/processed/df_etapa18_sobrepreco.zip",
                "data/processed/df_etapa18_sobrepreco_resumo.csv",
                "data/processed/df_etapa18_sobrepreco_stats.csv",
                "data/processed/df_etapa18_sobrepreco_resumo_conversao.csv",
            ]
            for arquivo in arquivos:
                if os.path.exists(arquivo):
                    self.log_arquivo(arquivo)

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(18, "Análise de Sobrepreço", "SUCESSO", duracao)
            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(18, "Análise de Sobrepreço", "ERRO", duracao)
            self.log_erro("Etapa 18", str(e))
            return False

    def etapa_19_ajuste_inflacionario(self):
        """Etapa 19: Ajuste inflacionário (IGP-DI)"""
        inicio = datetime.now()

        print("\n" + "="*60)
        print("ETAPA 19: AJUSTE INFLACIONARIO (IGP-DI)")
        print("="*60)

        try:
            from pipelines.nfe.src.nfe_etapa19_ajuste_inflacionario import processar_ajuste_inflacionario

            df_base = self.df_etapa18
            exportar = not self.modo_rapido
            df_resultado = processar_ajuste_inflacionario(
                df_entrada=df_base,
                exportar=exportar,
            )

            self.df_etapa19 = df_resultado

            arquivos = [
                "data/processed/df_etapa19_valores_ajustados.zip",
                "data/processed/df_etapa19_resumo_ajuste.csv",
            ]
            for arquivo in arquivos:
                if os.path.exists(arquivo):
                    self.log_arquivo(arquivo)

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(19, "Ajuste Inflacionário (IGP-DI)", "SUCESSO", duracao)
            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(19, "Ajuste Inflacionário (IGP-DI)", "ERRO", duracao)
            self.log_erro("Etapa 19", str(e))
            return False

    def etapa_20_classificacao_esfera(self):
        """Etapa 20: Classificação por esfera administrativa"""
        inicio = datetime.now()

        print("\n" + "="*60)
        print("ETAPA 20: CLASSIFICAÇÃO POR ESFERA")
        print("="*60)

        try:
            from pipelines.nfe.src.nfe_etapa20_classificacao_esfera import processar_classificacao_esfera

            df_base = self.df_etapa19
            exportar = not self.modo_rapido
            df_resultado = processar_classificacao_esfera(
                df_entrada=df_base,
                exportar=exportar,
            )

            self.df_etapa20 = df_resultado

            arquivos = [
                "data/processed/df_etapa20_classificacao_esfera.zip",
                "data/processed/df_etapa20_distribuicao_esfera.csv",
            ]
            for arquivo in arquivos:
                if os.path.exists(arquivo):
                    self.log_arquivo(arquivo)

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(20, "Classificação por Esfera", "SUCESSO", duracao)
            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(20, "Classificação por Esfera", "ERRO", duracao)
            self.log_erro("Etapa 20", str(e))
            return False

    def etapa_21_padronizacao_unidades(self):
        """Etapa 21: Padronização e inferência de unidades."""
        inicio = datetime.now()

        print("\n" + "="*60)
        print("ETAPA 21: PADRONIZAÇÃO DE UNIDADES")
        print("="*60)

        try:
            from pipelines.nfe.src.nfe_etapa21_padronizacao_unidades import processar_padronizacao_unidades

            df_base = self.df_etapa20
            # Etapa 21 sempre exporta no modo normal; no modo rápido só exporta se for a última antes do particionamento
            exportar = True  # Precisa exportar para etapa 22 (particionamento)
            df_resultado = processar_padronizacao_unidades(
                df_entrada=df_base,
                exportar=exportar,
            )

            self.df_etapa21 = df_resultado

            arquivos = [
                "data/processed/df_etapa21_unidades_padronizadas.zip",
                "data/processed/df_etapa21_unidades_resumo.csv",
                "data/processed/df_etapa21_unidades_metricas.csv",
            ]
            for arquivo in arquivos:
                if os.path.exists(arquivo):
                    self.log_arquivo(arquivo)

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(21, "Padronização de Unidades", "SUCESSO", duracao)
            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(21, "Padronização de Unidades", "ERRO", duracao)
            self.log_erro("Etapa 21", str(e))
            return False

    def etapa_22_particionamento(self):
        """Etapa 22: Particionamento de tabelas para QlikView."""
        inicio = datetime.now()

        print("\n" + "="*60)
        print("ETAPA 22: PARTICIONAMENTO QLIKVIEW")
        print("="*60)

        try:
            sucesso = self.executar_script(
                self.scripts_dir / "processar_etapa22_particionamento.py",
                "Particionamento QlikView"
            )

            if not sucesso:
                raise Exception("Script de particionamento falhou")

            arquivos = [
                "QlikView/df_central.csv",
                "QlikView/df_entidades.csv",
                "QlikView/df_valores_ajustados.csv",
                "QlikView/nfe_vencimento.csv",
                "QlikView/compact_parquet/df_central.parquet",
                "QlikView/compact_parquet/df_entidades.parquet",
                "QlikView/compact_parquet/df_valores_ajustados.parquet",
                "QlikView/compact_parquet/nfe_vencimento.parquet",
            ]
            for arquivo in arquivos:
                caminho = self.project_root / arquivo
                if caminho.exists():
                    self.log_arquivo(str(caminho))

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(22, "Particionamento QlikView", "SUCESSO", duracao)
            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(22, "Particionamento QlikView", "ERRO", duracao)
            self.log_erro("Etapa 22", str(e))
            return False

    def etapa_23_diagnostico_final(self):
        """Etapa 23: Diagnóstico final da base QlikView."""
        inicio = datetime.now()

        print("\n" + "="*60)
        print("ETAPA 23: DIAGNOSTICO FINAL DA BASE")
        print("="*60)

        try:
            sucesso = self.executar_script(
                self.scripts_dir / "processar_etapa23_diagnostico.py",
                "Diagnóstico Final QlikView",
                timeout_customizado=3600,
            )

            if not sucesso:
                raise Exception("Script de diagnóstico final falhou")

            arquivos = [
                "QlikView/etapa23_diagnostico_resumo.json",
                "QlikView/etapa23_diagnostico_colunas.csv",
                "QlikView/etapa23_diagnostico_alertas.csv",
                "QlikView/etapa23_diagnostico_log.txt",
            ]
            for arquivo in arquivos:
                caminho = self.project_root / arquivo
                if caminho.exists():
                    self.log_arquivo(str(caminho))

            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(23, "Diagnóstico Final QlikView", "SUCESSO", duracao)
            return True

        except Exception as e:
            duracao = (datetime.now() - inicio).total_seconds()
            self.log_etapa(23, "Diagnóstico Final QlikView", "ERRO", duracao)
            self.log_erro("Etapa 23", str(e))
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
            if status == "SUCESSO":
                status_symbol = "[OK]"
            elif status == "PULADO":
                status_symbol = "[SKIP]"
            else:
                status_symbol = "[ERRO]"
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

    def limpar_data_processed(self):
        """Remove todos os arquivos/diretórios dentro de data/processed.

        Importante: isso apenas limpa os *conteúdos* do diretório `data/processed`,
        não remove o diretório em si.
        """
        pasta = self.project_root / "data" / "processed"
        if not os.path.exists(pasta):
            print(f"[INFO] Pasta {pasta} não existe. Nada a limpar.")
            return

        print(f"\n[INFO] Limpando conteúdo de: {pasta}")
        try:
            for entry in os.listdir(pasta):
                caminho = os.path.join(pasta, entry)
                if os.path.isdir(caminho):
                    shutil.rmtree(caminho)
                else:
                    os.remove(caminho)
            print("[OK] Conteúdo de data/processed removido com sucesso.")
        except Exception as e:
            print(f"[ERRO] Falha ao limpar data/processed: {e}")
    
    def executar(self):
        """Executa o pipeline completo"""
        print("\n" + "#"*70)
        print("#" + " "*68 + "#")
        print("#" + " "*15 + "PIPELINE COMPLETO DE NOTAS FISCAIS (NFe)" + " "*12 + "#")
        print("#" + " "*68 + "#")
        print("#"*70 + "\n")
        print(f"[RUN_ID] {self.run_id}")
        self._emit_structured_log(
            "info",
            "pipeline_inicio",
            modo_rapido=self.modo_rapido,
            start_stage=self.start_stage,
        )
        
        print(f"Início: {self.inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
        if self.start_stage <= 1:
            # VERIFICAR SE INPUT MUDOU - Se sim, limpar tudo
            if self.verificar_input_mudou():
                print("[ACAO] Removendo TODOS os arquivos intermediários...")
                self.limpar_data_processed()
                print("[OK] Limpeza completa realizada. Pipeline iniciará do zero.\n")

            # Limpar arquivos antigos ANTES de começar
            self.limpar_arquivos_antigos()
        else:
            print(f"[RESUME] Execução retomada a partir da etapa {self.start_stage}.")
            print("[RESUME] Limpeza automática e validação de mudança de input foram ignoradas.")        
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
            ("Matching de Apresentação Única", self.etapa_13_matching_apresentacao_unica),
            ("Extração de Atributos com IA", self.etapa_14_extracao_ia),
            ("Matching Híbrido Ponderado", self.etapa_15_matching_hibrido),
            ("Finalização do Pipeline", self.etapa_16_finalizacao_pipeline),
            ("Consolidação Final", self.etapa_17_consolidacao_final),
            ("Análise de Sobrepreço", self.etapa_18_sobrepreco),
            ("Ajuste Inflacionário", self.etapa_19_ajuste_inflacionario),
            ("Classificação por Esfera", self.etapa_20_classificacao_esfera),
            ("Padronização de Unidades", self.etapa_21_padronizacao_unidades),
            ("Particionamento QlikView", self.etapa_22_particionamento),
            ("Diagnóstico Final QlikView", self.etapa_23_diagnostico_final),
        ]
        
        for idx, (nome, funcao) in enumerate(etapas, start=1):
            if idx < self.start_stage:
                self.log_etapa(idx, nome, "PULADO")
                self._emit_structured_log(
                    "info",
                    "etapa_pulada_resume",
                    etapa_numero=idx,
                    etapa_nome=nome,
                    start_stage=self.start_stage,
                )
                continue

            stop_heartbeat = self._iniciar_heartbeat_etapa(idx, nome)
            try:
                sucesso_etapa = funcao()
            except Exception as e:
                # Caso uma etapa lance exceção inesperada, registrar erro e interromper
                self.log_erro(nome, f"Exception ao executar etapa: {e}")
                sucesso_etapa = False
            finally:
                stop_heartbeat.set()

            if not sucesso_etapa:
                print(f"\n[AVISO] Pipeline interrompido em: {nome}")
                break

        # Gerar relatório final
        sucesso = self.gerar_relatorio()

        # Salvar timestamp do input para próxima execução somente se tudo ocorreu bem
        if sucesso:
            self.salvar_timestamp_input()
            print("\n[INFO] Timestamp do input salvo para detecção de mudanças futuras.")
            self._emit_structured_log("info", "pipeline_fim", sucesso=True, erros=len(self.erros))
        else:
            self._emit_structured_log("warning", "pipeline_fim", sucesso=False, erros=len(self.erros))

        return sucesso


def analisar_eans_sem_match(arquivo_matched, exportar=True):
    """
    [DEBUG] Analisa EANs que não tiveram match com a base ANVISA
    
    Parâmetros:
        arquivo_matched (str): Caminho do arquivo nfe_matched_*.csv
        exportar (bool): Se True, exporta os resultados em CSV
    """
    print("\n" + "="*80)
    print(" "*20 + "[DEBUG] ANALISE DE EANs SEM MATCH")
    print("="*80 + "\n")
    
    try:
        # Carregar arquivo
        print("[INFO] Carregando arquivo de matching...")
        df = pd.read_csv(arquivo_matched, sep=';', dtype={'codigo_ean': str})
        print(f"[OK] {len(df):,} registros carregados\n")
        
        # 1. Filtrar linhas onde 'PRODUTO' e nulo
        mask_nulo = df['PRODUTO'].isnull() | (df['PRODUTO'].astype(str).str.lower() == 'nan')
        df_produto_nulo = df.loc[mask_nulo].copy()
        
        total_sem_match = len(df_produto_nulo)
        pct_sem_match = (total_sem_match / len(df)) * 100
        
        print(f"[INFO] Registros sem PRODUTO (sem match): {total_sem_match:,} ({pct_sem_match:.2f}%)\n")
        
        if total_sem_match == 0:
            print("[OK] Nenhum EAN sem match encontrado!\n")
            return
        
        # 2. Contar frequencia de EANs
        ean_counts = (
            df_produto_nulo['codigo_ean']
            .value_counts(dropna=False)
            .rename('Frequencia')
        )
        
        # 3. Manter apenas a descricao mais frequente por EAN
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
        
        # 4. Unir com contagens de EAN
        resultado = (
            descricao_top
            .merge(ean_counts, left_on='codigo_ean', right_index=True, how='left')
            .sort_values('Frequencia', ascending=False)
            .reset_index(drop=True)
        )
        
        # 5. Agregar por EAN com metricas financeiras
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
        
        # 6. Exibir resultados
        print("="*80)
        print("TOP 50 EANs SEM MATCH - Ordenado por Frequencia")
        print("="*80)
        print(resultado.head(50).to_string(index=False))
        
        # 7. Exibir com metricas financeiras
        print("\n" + "="*80)
        print("TOP 50 EANs SEM MATCH - Ordenado por Frequencia e Valor Total")
        print("="*80 + "\n")
        
        def format_brl(x):
            if pd.isna(x):
                return 'N/A'
            return f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        
        top_ean_metricas_display = top_ean_metricas.head(50).copy()
        top_ean_metricas_display['Valor_Total'] = top_ean_metricas_display['Valor_Total'].apply(format_brl)
        top_ean_metricas_display['Valor_Medio'] = top_ean_metricas_display['Valor_Medio'].apply(format_brl)
        
        print(top_ean_metricas_display.to_string(index=False))
        
        # 8. Exportar para CSV
        if exportar:
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            
            # Exportar analise simples
            arquivo_saida1 = f"data/processed/debug_eans_sem_match_{timestamp}.csv"
            resultado.to_csv(arquivo_saida1, sep=';', index=False, encoding='utf-8')
            print(f"\n[OK] Analise simples exportada: {arquivo_saida1}")
            
            # Exportar com metricas financeiras
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


def run(
    debug_enabled: Optional[bool] = None,
    cleanup_processed: Optional[bool] = None,
    modo_rapido: Optional[bool] = None,
    start_stage: int = 1,
) -> bool:
    """Executa o pipeline completo de NFe.
    
    Args:
        debug_enabled: Ativa análise de EANs sem match (usa config se None)
        cleanup_processed: Limpa data/processed ao final (usa config se None)
        modo_rapido: Desativa exportações intermediárias (usa config se None)
        start_stage: Etapa inicial para retomar execução (1-23)
    """

    # Garante resolução consistente de caminhos relativos durante o run,
    # evitando acoplamento na importação do módulo.
    os.chdir(PROJECT_ROOT)

    # Guarda paranoica: bloqueia execução se houver qualquer sinal de encoding corrompido nas fontes.
    try:
        assert_no_encoding_corruption(PIPELINE_ROOT)
    except RuntimeError as exc:
        print(str(exc))
        return False

    arquivo_input = PROJECT_ROOT / "nfe" / "nfe.csv"

    # Verificar se arquivo de entrada existe
    if not arquivo_input.exists():
        print("[ERRO] Arquivo 'nfe/nfe.csv' nao encontrado!")
        print("\nColoque seu arquivo CSV de NFe em:")
        print("  nfe/nfe.csv")
        return False

    # Criar diretórios necessários
    os.makedirs(PROJECT_ROOT / "data" / "processed", exist_ok=True)
    os.makedirs(PROJECT_ROOT / "data" / "raw", exist_ok=True)

    if not (1 <= int(start_stage) <= 23):
        print(f"[ERRO] --start-stage inválido: {start_stage}. Use um valor entre 1 e 23.")
        return False

    # Executar pipeline com modo rápido se especificado
    pipeline = PipelineNFe(modo_rapido=modo_rapido, start_stage=int(start_stage))
    sucesso = pipeline.executar()

    debug_flag = debug_enabled
    if debug_flag is None:
        debug_flag = bool(get_toggle("pipeline", "debug_mode", default=False))

    cleanup_flag = cleanup_processed
    if cleanup_flag is None:
        cleanup_flag = bool(get_toggle("pipeline", "cleanup_processed", default=False))

    # [DEBUG] Executar análise de EANs sem match se toggle estiver ativo
    if debug_flag and sucesso:
        arquivos_matched = glob.glob(str(PROJECT_ROOT / "data" / "processed" / "nfe_etapa07_matched.csv"))
        if arquivos_matched:
            arquivo_recente = max(arquivos_matched, key=os.path.getmtime)
            print(f"\n[DEBUG] Analisando arquivo: {os.path.basename(arquivo_recente)}")
            analisar_eans_sem_match(arquivo_recente, exportar=True)

    # Limpeza opcional dos dados processados se toggle ativado e pipeline completo com sucesso
    if cleanup_flag and sucesso:
        print("\n[INFO] cleanup_processed ativado, limpando data/processed...")
        pipeline.limpar_data_processed()

    return sucesso


def main() -> None:
        """Retém compatibilidade com chamadas antigas do script.

        Agora aceita argumentos de linha de comando:
            --debug: ativa debug (aplica análise de eans sem match)
            --cleanup-processed: limpa data/processed ao final do pipeline (apenas em caso de sucesso)
            --modo-rapido: desativa exportações intermediárias para processamento mais veloz
            --start-stage: retoma execução a partir de uma etapa específica (1-23)
        """
        import argparse

        parser = argparse.ArgumentParser(description="Executa pipeline NFe")
        parser.add_argument("--debug", dest="debug", action="store_true", help="Ativa a análise de EANs sem match após o run")
        parser.add_argument("--no-debug", dest="debug", action="store_false", help="Desativa análise de EANs, sobrescrevendo o config")
        parser.add_argument("--cleanup-processed", dest="cleanup_processed", action="store_true", help="Limpa data/processed após execução bem-sucedida")
        parser.add_argument("--no-cleanup-processed", dest="cleanup_processed", action="store_false", help="Mantém data/processed, mesmo que o config peça limpeza")
        parser.add_argument("--modo-rapido", dest="modo_rapido", action="store_true", help="Desativa exportações intermediárias (100%% em memória)")
        parser.add_argument("--no-modo-rapido", dest="modo_rapido", action="store_false", help="Ativa exportações intermediárias (padrão)")
        parser.add_argument("--start-stage", dest="start_stage", type=int, default=1, help="Etapa inicial para execução/resume (1-23)")
        parser.set_defaults(debug=None, cleanup_processed=None, modo_rapido=None)
        args = parser.parse_args()

        sucesso = run(
            debug_enabled=args.debug,
            cleanup_processed=args.cleanup_processed,
            modo_rapido=args.modo_rapido,
            start_stage=args.start_stage,
        )
        sys.exit(0 if sucesso else 1)


if __name__ == "__main__":
    main()
