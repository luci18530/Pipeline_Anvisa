# -*- coding: utf-8 -*-
"""
Script automatizado para baixar, limpar e processar as listas de preÃ§os de medicamentos (PMVG) da Anvisa.
"""
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString
import re
import os
import importlib.util
import shutil
from datetime import datetime
from pathlib import Path
import time
import concurrent.futures
from tqdm import tqdm
import logging
import unicodedata
import numpy as np
import glob

PROJECT_ROOT = Path(__file__).resolve().parents[3]
from pipelines.anvisa_base.config_anvisa import (
    ANO_INICIO,
    MES_INICIO,
    ANO_FIM,
    MES_FIM,
    URL_ANVISA,
    MAX_DOWNLOAD_WORKERS,
    MAX_CLEANING_THREADS,
    DOWNLOAD_THREAD_MULTIPLIER,
    DOWNLOAD_MAX_THREADS,
    DOWNLOAD_CHUNK_SIZE,
    DOWNLOAD_RETRIES,
    DOWNLOAD_BACKOFF_SECONDS,
    PASTA_DOWNLOADS_BRUTOS,
    PASTA_ARQUIVOS_LIMPOS,
    ARQUIVO_CONSOLIDADO_TEMP,
    ARQUIVO_FINAL_VIGENCIAS
)

# ==============================================================================
#      CONFIGURAÃ‡Ã•ES GERAIS
# ==============================================================================
HAS_XLRD = importlib.util.find_spec("xlrd") is not None

# ==============================================================================
#      FUNÃ‡Ã•ES DO PIPELINE
# ==============================================================================

def scrape_anvisa_links():
    """Raspa a pÃ¡gina da Anvisa para encontrar os links dos arquivos de preÃ§os."""
    logging.info(f"Acessando {URL_ANVISA} para extrair links...")

    
    meses_map = {
        'janeiro': 1, 'fevereiro': 2, 'marÃ§o': 3, 'abril': 4, 'maio': 5, 'junho': 6,
        'julho': 7, 'agosto': 8, 'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
    }
    rx_mesctx = re.compile(r'\b(janeiro|fevereiro|mar[Ã§c]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s*/\s*(\d{2,4})\b', re.IGNORECASE)
    rx_full = re.compile(r'(\d{4})(\d{2})(\d{2})')
    rx_mid = re.compile(r'(\d{4})_(\d{2})_')
    rx_short = re.compile(r'(\d{4})(\d{2})_')

    def normalize_year(y: str) -> int:
        return int(y) if len(y) == 4 else 2000 + int(y)

    def month_name(idx: int) -> str:
        return list(meses_map.keys())[idx - 1]

    soup = BeautifulSoup(requests.get(URL_ANVISA, timeout=30).content, "html.parser")
    core = soup.find(id="content-core")
    if core is None:
        raise RuntimeError("div#content-core nÃ£o encontrada na pÃ¡gina da Anvisa!")

    dados = []
    ctx_year = ctx_month = None

    for node in core.descendants:
        if isinstance(node, NavigableString):
            m = rx_mesctx.search(node.strip().lower())
            if m:
                ctx_month = meses_map.get(m.group(1).lower().replace('Ã§', 'c'))
                ctx_year = normalize_year(m.group(2))
            continue

        if not (isinstance(node, Tag) and node.name == "a"):
            continue
        
        href = node.get("href", "").strip()
        if not href or "_reso_" in href.lower():
            continue

        txt_upper = node.get_text(" ", strip=True).upper()
        if "XLS" not in txt_upper:
            continue

        href_l = href.lower()
        if "xls_conformidade_gov" not in href_l:
            if not href_l.endswith("json-file-1") or not href_l.split("/")[-1].startswith("5"):
                continue

        ano = mes = None
        for rx in (rx_full, rx_mid, rx_short):
            mm = rx.search(href)
            if mm:
                ano, mes = int(mm.group(1)), int(mm.group(2))
                break
        if not (ano and mes):
            ano, mes = ctx_year, ctx_month

        if ano and mes:
            dados.append({"ano": ano, "mes": mes, "mes_nome": month_name(mes), "url": href})

    df_links = pd.DataFrame(dados).sort_values(["ano", "mes"]).drop_duplicates(["ano", "mes"])
    logging.info(f"Total de links capturados: {len(df_links)}")
    return df_links

def download_files(df_to_download):
    """Baixa os arquivos de uma lista de links em paralelo."""
    workers_download = min(
        MAX_DOWNLOAD_WORKERS * DOWNLOAD_THREAD_MULTIPLIER,
        DOWNLOAD_MAX_THREADS,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": "PipelineAnvisa/1.0"})

    retry = Retry(
        total=DOWNLOAD_RETRIES,
        backoff_factor=DOWNLOAD_BACKOFF_SECONDS,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=workers_download,
        pool_maxsize=workers_download,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    base_folder = Path(PASTA_DOWNLOADS_BRUTOS)
    base_folder.mkdir(exist_ok=True)

    def resolver_destino(row) -> Path:
        ano_cal, mes_cal = int(row.ano), int(row.mes)
        ano_fiscal = ano_cal - 1 if mes_cal <= 3 else ano_cal
        pasta = base_folder / f"anvisa_ano_fiscal_{ano_fiscal}"
        pasta.mkdir(parents=True, exist_ok=True)
        ext = Path(row.url).suffix or ".xls"
        nome = f"{ano_cal}_{mes_cal:02d}_{row.mes_nome}{ext}"
        return pasta / nome

    linhas = [row for _, row in df_to_download.iterrows()]
    pendentes = []
    ja_existentes = 0
    for row in linhas:
        destino = resolver_destino(row)
        if destino.exists():
            ja_existentes += 1
        else:
            pendentes.append(row)

    if ja_existentes:
        logging.info(f"Arquivos ja disponiveis localmente (skip): {ja_existentes}")
    if not pendentes:
        logging.info("Nenhum arquivo novo para download.")
        return

    def download_row(row):
        dest = resolver_destino(row)
        for attempt in range(DOWNLOAD_RETRIES):
            try:
                r = session.get(row.url, stream=True, timeout=60)
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(DOWNLOAD_CHUNK_SIZE):
                        f.write(chunk)
                return f"ok ({attempt + 1}): {dest.relative_to(base_folder)}"
            except requests.RequestException:
                time.sleep(DOWNLOAD_BACKOFF_SECONDS)
        return f"falhou: {row.url.split('/')[-1]}"

    workers_ativos = min(workers_download, len(pendentes))
    logging.info(
        f"Iniciando downloads em {workers_ativos} threads ({len(pendentes)} arquivos novos)..."
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers_ativos) as exe:
        resultados = list(
            tqdm(
                exe.map(download_row, pendentes),
                total=len(pendentes),
                desc="Baixando arquivos",
                ncols=100,
            )
        )

    ok = sum(r.startswith("ok") for r in resultados)
    fail = len(resultados) - ok
    logging.info(f"Resumo do download - Sucesso: {ok} | Falha: {fail}")
    if fail:
        for r in resultados:
            if r.startswith("falhou"):
                logging.error(f" - {r}")


def clean_downloaded_files(source_folder, target_folder):
    """Limpa e padroniza os arquivos Excel baixados em paralelo."""
    all_files = sorted(glob.glob(f"{source_folder}/anvisa_ano_fiscal_*/*.xls*"))
    if not all_files:
        logging.warning("Nenhum arquivo .xls/.xlsx encontrado para processar.")
        return

    TARGET_COLUMNS = ['PRINC\u00cdPIO ATIVO', 'SUBST\u00c2NCIA', 'CNPJ']

    def detect_excel_container(file_path):
        """
        Detecta o tipo de conteÃºdo do arquivo para escolher o engine correto.
        Retorna: html | zip | ole | unknown
        """
        with open(file_path, 'rb') as f:
            header_bytes = f.read(4096)

        # Alguns downloads HTML vÃªm com espaÃ§os/quebras antes do DOCTYPE.
        stripped = header_bytes.lstrip().lower()
        if stripped.startswith(b'<!doctype') or stripped.startswith(b'<html'):
            return "html"
        if header_bytes.startswith(b'PK\x03\x04'):
            return "zip"
        if header_bytes.startswith(b'\xD0\xCF\x11\xE0'):
            return "ole"
        return "unknown"

    def process_single_file(file_path):
        try:
            filename = os.path.basename(file_path)
            output_name = None
            output_path = None
            try:
                ano_ref, mes_ref = int(filename.split('_')[0]), int(filename.split('_')[1])
                output_name = f"ANVISA_LIMPO_{ano_ref}_{mes_ref:02d}.csv"
                output_path = os.path.join(target_folder, output_name)
                if os.path.exists(output_path) and os.path.getmtime(output_path) >= os.path.getmtime(file_path):
                    return f"SKIP: {filename} -> {output_name} (ja processado)"
            except Exception:
                pass

            file_kind = detect_excel_container(file_path)
            if file_kind == "html":
                return f"ERRO: {file_path} -> Arquivo HTML disfarÃ§ado de Excel (download invÃ¡lido)"
            
            ext = os.path.splitext(file_path)[1].lower()
            
            # Escolhe engines com base na assinatura real do arquivo.
            if file_kind == "zip":
                engines_to_try = ['openpyxl']
            elif file_kind == "ole":
                if not HAS_XLRD:
                    return (
                        f"ERRO: {file_path} -> Arquivo XLS binÃ¡rio detectado, "
                        "mas o pacote 'xlrd' nÃ£o estÃ¡ instalado no ambiente."
                    )
                engines_to_try = ['xlrd']
            elif ext == '.xlsx':
                engines_to_try = ['openpyxl']
            elif ext == '.xls':
                engines_to_try = ['xlrd', 'openpyxl'] if HAS_XLRD else ['openpyxl']
            else:
                engines_to_try = ['openpyxl']
            
            df_preview = None
            engine_used = None
            errors_by_engine = []
            
            for engine in engines_to_try:
                try:
                    df_preview = pd.read_excel(file_path, header=None, nrows=200, dtype=str, engine=engine)
                    engine_used = engine
                    break
                except Exception as e:
                    errors_by_engine.append(f"{engine}: {e}")
                    continue
            
            if df_preview is None:
                joined_errors = " | ".join(errors_by_engine) if errors_by_engine else "sem detalhes"
                return f"ERRO: {file_path} -> Nenhum engine funcionou. Erros: {joined_errors}"
            
            header_row_index = None
            for i, row in df_preview.iterrows():
                row_values = {str(v).strip().upper() for v in row.dropna()}
                if any(col in row_values for col in TARGET_COLUMNS):
                    header_row_index = i
                    break
            
            # Se nÃ£o encontrou, tentar detectar automaticamente (linha com muitos valores)
            if header_row_index is None and len(df_preview) > 0:
                for i, row in df_preview.iterrows():
                    non_null_count = row.notna().sum()
                    if non_null_count >= 5:  # Linha com pelo menos 5 valores
                        header_row_index = i
                        break
            
            if header_row_index is None:
                return f"AVISO: CabeÃ§alho nÃ£o encontrado -> {file_path}"
                
            df = pd.read_excel(file_path, header=None, skiprows=header_row_index + 1, dtype=str, engine=engine_used)
            header = df_preview.iloc[header_row_index].astype(str).str.strip().str.replace(r'\s+%', '%', regex=True).str.replace(r'\s+', ' ', regex=True).str.upper()
            df.columns = header

            ano_ref, mes_ref = int(filename.split('_')[0]), int(filename.split('_')[1])
            df['ANO_REF'], df['MES_REF'] = ano_ref, mes_ref
            
            cols_to_move = ['ANO_REF', 'MES_REF']
            df = df[cols_to_move + [c for c in df.columns if c not in cols_to_move]]
            
            output_name = output_name or f"ANVISA_LIMPO_{ano_ref}_{mes_ref:02d}.csv"
            os.makedirs(target_folder, exist_ok=True)
            output_path = output_path or os.path.join(target_folder, output_name)
            df.to_csv(output_path, sep=';', index=False, encoding='utf-8')
            
            engine_msg = f" (engine: {engine_used})" if ext == '.xls' else ""
            return f"OK: {filename} -> {output_name}{engine_msg}"
        except Exception as e:
            return f"ERRO: {file_path} -> {e}"

    workers_limpeza = min(MAX_CLEANING_THREADS * 2, 12)
    logging.info(f"Iniciando limpeza de {len(all_files)} arquivos com {workers_limpeza} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers_limpeza) as exe:
        resultados = list(tqdm(exe.map(process_single_file, all_files), total=len(all_files), desc="Limpando arquivos", ncols=100))
    
    logging.info("--- Resultados da Limpeza ---")
    for r in resultados: logging.info(f" -> {r}")

def consolidate_cleaned_files(source_folder, output_file):
    """Consolida todos os CSVs limpos em um Ãºnico arquivo."""
    csv_files = sorted(glob.glob(os.path.join(source_folder, "ANVISA_LIMPO_*.csv")))
    if not csv_files:
        logging.warning("Nenhum arquivo ANVISA_LIMPO_*.csv encontrado; usando fallback *.csv.")
        csv_files = sorted(glob.glob(os.path.join(source_folder, "*.csv")))
    if not csv_files:
        logging.warning("Nenhum arquivo CSV limpo encontrado para consolidar.")
        return None

    COLUNAS_PARA_MANTER = ['ANO_REF', 'MES_REF', 'PRINC\u00cdPIO ATIVO', 'LABORAT\u00d3RIO', 'C\u00d3DIGO GGREM', 'REGISTRO', 'EAN 1', 'EAN 2', 'EAN 3', 'PRODUTO', 'APRESENTA\u00c7\u00c3O', 'CLASSE TERAP\u00caUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PRE\u00c7O', 'PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%', 'ICMS 0%', 'CAP']
    
    def _normalizar_coluna(col):
        texto = unicodedata.normalize('NFKD', str(col).upper())
        texto = ''.join(ch for ch in texto if not unicodedata.combining(ch))
        texto = re.sub(r'[^A-Z0-9% ]+', ' ', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()
        return texto

    def _padronizar_colunas(df):
        rename_map = {}
        for col in df.columns:
            key = _normalizar_coluna(col)
            if key in ('PRINCIPIO ATIVO', 'SUBSTANCIA'):
                rename_map[col] = 'PRINC\u00cdPIO ATIVO'
            elif key == 'LABORATORIO':
                rename_map[col] = 'LABORAT\u00d3RIO'
            elif 'GGREM' in key or key == 'CODIGO GGREM':
                rename_map[col] = 'C\u00d3DIGO GGREM'
            elif key == 'APRESENTACAO':
                rename_map[col] = 'APRESENTA\u00c7\u00c3O'
            elif key == 'CLASSE TERAPEUTICA':
                rename_map[col] = 'CLASSE TERAP\u00caUTICA'
            elif key == 'REGIME DE PRECO':
                rename_map[col] = 'REGIME DE PRE\u00c7O'
            elif key == 'EAN1':
                rename_map[col] = 'EAN 1'
            elif key == 'EAN2':
                rename_map[col] = 'EAN 2'
            elif key == 'EAN3':
                rename_map[col] = 'EAN 3'
        if rename_map:
            df = df.rename(columns=rename_map)
        return df
    
    dfs = []
    for file in tqdm(csv_files, desc="Lendo CSVs limpos", ncols=100):
        try:
            df = pd.read_csv(file, sep=";", dtype=str, low_memory=False, engine='c')
            df.columns = df.columns.str.strip().str.upper()

            df = _padronizar_colunas(df)

            # Garante colunas críticas no consolidado, inclusive CÓDIGO GGREM.
            for col in COLUNAS_PARA_MANTER:
                if col not in df.columns:
                    df[col] = None

            if df['C\u00d3DIGO GGREM'].isna().all():
                logging.warning(f"Arquivo sem CÓDIGO GGREM identificável: {os.path.basename(file)}")

            dfs.append(df[COLUNAS_PARA_MANTER])
        except Exception as e:
            logging.error(f"Erro ao ler {file}: {e}")

    if not dfs:
        logging.error("Nenhum DataFrame vÃ¡lido foi carregado para consolidaÃ§Ã£o.")
        return None

    logging.info("Concatenando bases...")
    df_consolidado = pd.concat(dfs, ignore_index=True, sort=False).dropna(how="all")
    df_consolidado = df_consolidado.dropna(subset=['PRODUTO', 'PRINC\u00cdPIO ATIVO'])
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df_consolidado.to_csv(output_file, sep=";", index=False, encoding='utf-8')
    logging.info(f"ConsolidaÃ§Ã£o concluÃ­da. Arquivo salvo em: {os.path.abspath(output_file)}")
    return df_consolidado

def process_vigencias(df_consolidado):
    """Processa o dataframe consolidado para criar a tabela final de vigÃªncias."""
    logging.info("Iniciando fase de consolidaÃ§Ã£o de vigÃªncias...")
    df = df_consolidado.copy()

    # PASSO 1: PreparaÃ§Ã£o
    # Remover linhas com ANO_REF ou MES_REF invÃ¡lidos
    linhas_antes = len(df)
    df = df.dropna(subset=['ANO_REF', 'MES_REF'])
    df = df[(df['ANO_REF'] != '') & (df['MES_REF'] != '')]
    linhas_removidas = linhas_antes - len(df)
    if linhas_removidas > 0:
        logging.warning(f"Removidas {linhas_removidas} linhas com ANO_REF ou MES_REF invÃ¡lidos")
    
    cols_to_check = ['PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%', 'ICMS 0%', 'CAP']
    df['id_produto'] = df['REGISTRO'].astype(str).str.strip() + '-' + df['CÃ“DIGO GGREM'].astype(str).str.strip()
    df['DATA_REF'] = pd.to_datetime(df['ANO_REF'].astype(str) + '-' + df['MES_REF'].astype(str) + '-01')
    
    for col in ['PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%']:
        if col in df.columns:
            s = df[col].astype(str).str.replace(',', '.', regex=False).str.replace(r'\.(?=.*\.)', '', regex=True)
            df[col] = pd.to_numeric(s, errors='coerce')
    
    df.sort_values(['id_produto', 'DATA_REF'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # PASSO 2: DetecÃ§Ã£o de MudanÃ§as
    logging.info("Detectando mudanÃ§as de preÃ§os...")
    mudanca_valores = df[cols_to_check].ne(df[cols_to_check].shift(1)).any(axis=1)
    mudanca_produto = df['id_produto'] != df['id_produto'].shift(1)
    inicio_vigencia = mudanca_produto | mudanca_valores

    # PASSO 3: ConstruÃ§Ã£o de VigÃªncias
    logging.info("Construindo tabela de vigÃªncias...")
    df_vigencias = df[inicio_vigencia].copy()
    df_vigencias['VIG_INICIO'] = df_vigencias['DATA_REF']
    df_vigencias['VIG_FIM'] = df_vigencias.groupby('id_produto')['VIG_INICIO'].shift(-1) - pd.Timedelta(days=1)

    def calcular_vig_fim_final(vig_inicio_date):
        if pd.isna(vig_inicio_date): return None
        return pd.Timestamp(year=vig_inicio_date.year if vig_inicio_date.month <= 3 else vig_inicio_date.year + 1, month=4, day=15)
    
    last_vigencia_mask = df_vigencias['VIG_FIM'].isnull()
    df_vigencias.loc[last_vigencia_mask, 'VIG_FIM'] = df_vigencias.loc[last_vigencia_mask, 'VIG_INICIO'].apply(calcular_vig_fim_final)

    # PASSO 4: FinalizaÃ§Ã£o
    df_vigencias['id_preco'] = df_vigencias['id_produto'] + '_' + df_vigencias['VIG_INICIO'].dt.strftime('%Y%m%d')
    colunas_finais = ['id_preco', 'id_produto', 'VIG_INICIO', 'VIG_FIM', 'PRINCÃPIO ATIVO', 'LABORATÃ“RIO', 'CÃ“DIGO GGREM', 'REGISTRO', 'EAN 1', 'EAN 2', 'EAN 3', 'PRODUTO', 'APRESENTAÃ‡ÃƒO', 'CLASSE TERAPÃŠUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÃ‡O', 'PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%', 'ICMS 0%', 'CAP']
    df_vigencias_final = df_vigencias[[col for col in colunas_finais if col in df_vigencias.columns]].copy()
    
    # PASSO 5: Limpeza numÃ©rica final e preenchimento de preÃ§os
    def parse_num_seguro(x):
        if pd.isna(x): return np.nan
        s = re.sub(r"[^\d,.\-]", "", unicodedata.normalize("NFKC", str(x)))
        if "," in s and "." in s: s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
        elif "," in s: s = s.replace(",", ".")
        try: return float(s)
        except (ValueError, TypeError): return np.nan
        
    for c in ['PF 0%', 'PF 18%', 'PF 20%', 'PMVG 0%', 'PMVG 18%', 'PMVG 20%']:
        if c in df_vigencias_final.columns: df_vigencias_final[c] = df_vigencias_final[c].apply(parse_num_seguro)
            
    mask_pf = df_vigencias_final['PF 20%'].isnull() & df_vigencias_final['PF 0%'].notnull()
    df_vigencias_final.loc[mask_pf, 'PF 20%'] = (df_vigencias_final.loc[mask_pf, 'PF 0%'] * 1.25).round(2)
    mask_pmvg = df_vigencias_final['PMVG 20%'].isnull() & df_vigencias_final['PMVG 0%'].notnull()
    df_vigencias_final.loc[mask_pmvg, 'PMVG 20%'] = (df_vigencias_final.loc[mask_pmvg, 'PMVG 0%'] * 1.25).round(2)

    # PASSO 5.1: Fallback para preÃ§os com ICMS 18% (quando nÃ£o disponÃ­vel no arquivo original)
    # FÃ³rmula CMED: PreÃ§o com ICMS = PreÃ§o 0% / (1 - alÃ­quota)
    # Para 18%: PF 18% = PF 0% / 0.82 â‰ˆ PF 0% Ã— 1.2195122
    FATOR_ICMS_18 = 1 / (1 - 0.18)  # 1.2195122
    
    # Calcular PF 18% somente se coluna nÃ£o existir ou estiver nula
    if 'PF 18%' not in df_vigencias_final.columns:
        df_vigencias_final['PF 18%'] = np.nan
    mask_pf18 = df_vigencias_final['PF 18%'].isnull() & df_vigencias_final['PF 0%'].notnull()
    df_vigencias_final.loc[mask_pf18, 'PF 18%'] = (df_vigencias_final.loc[mask_pf18, 'PF 0%'] * FATOR_ICMS_18).round(2)
    
    # Calcular PMVG 18% somente se coluna nÃ£o existir ou estiver nula
    if 'PMVG 18%' not in df_vigencias_final.columns:
        df_vigencias_final['PMVG 18%'] = np.nan
    mask_pmvg18 = df_vigencias_final['PMVG 18%'].isnull() & df_vigencias_final['PMVG 0%'].notnull()
    df_vigencias_final.loc[mask_pmvg18, 'PMVG 18%'] = (df_vigencias_final.loc[mask_pmvg18, 'PMVG 0%'] * FATOR_ICMS_18).round(2)

    # PASSO 6: PadronizaÃ§Ã£o de atributos
    logging.info("Padronizando atributos de texto pela Ãºltima vigÃªncia...")
    cols_to_standardize = ['PRINCÃPIO ATIVO', 'LABORATÃ“RIO', 'PRODUTO', 'APRESENTAÃ‡ÃƒO', 'CLASSE TERAPÃŠUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÃ‡O']
    latest_data = df_vigencias_final.sort_values('VIG_INICIO').drop_duplicates(subset='id_produto', keep='last').set_index('id_produto')
    for col in [c for c in cols_to_standardize if c in df_vigencias_final.columns]:
        df_vigencias_final[col] = df_vigencias_final['id_produto'].map(latest_data[col])
        
    for col in df_vigencias_final.select_dtypes(include=['object']).columns:
        df_vigencias_final[col] = df_vigencias_final[col].str.upper()

    # PASSO 7: RemoÃ§Ã£o de duplicatas
    logging.info("Removendo duplicatas da chave final...")
    df_vigencias_final['quality_score'] = df_vigencias_final.notna().sum(axis=1)
    df_vigencias_final.sort_values(by=['id_produto', 'VIG_INICIO', 'quality_score'], ascending=[True, True, False], inplace=True)
    df_vigencias_final.drop_duplicates(subset=['id_produto', 'VIG_INICIO'], keep='first', inplace=True)
    df_vigencias_final.drop(columns=['quality_score'], inplace=True)
    
    return df_vigencias_final

def main():
    """FunÃ§Ã£o principal que orquestra todo o pipeline."""
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
    logging.info(f"PerÃ­odo de coleta: {MES_INICIO:02d}/{ANO_INICIO} atÃ© {MES_FIM:02d}/{ANO_FIM}")
    
    # 1. Limpeza Inicial
    logging.info("Iniciando pipeline de atualizaÃ§Ã£o da base da Anvisa.")
    
    # Limpar apenas pasta de downloads brutos (ZIPs da ANVISA)
    if os.path.exists(PASTA_DOWNLOADS_BRUTOS):
        shutil.rmtree(PASTA_DOWNLOADS_BRUTOS)
        logging.info(f"Pasta antiga '{PASTA_DOWNLOADS_BRUTOS}' removida.")
    
    # Criar estrutura de pastas necessÃ¡rias (sem apagar arquivos de outras pipelines)
    os.makedirs(PASTA_DOWNLOADS_BRUTOS, exist_ok=True)
    os.makedirs(os.path.join(PASTA_ARQUIVOS_LIMPOS, "anvisa"), exist_ok=True)
    logging.info("Estrutura de pastas criada.")

    # 2. Raspagem de Links
    try:
        df_links = scrape_anvisa_links()
    except Exception as e:
        logging.error(f"Falha ao raspar os links da Anvisa: {e}")
        return

    # 3. Filtragem e Download
    data_inicio = datetime(ANO_INICIO, MES_INICIO, 1)
    data_fim = datetime(ANO_FIM, MES_FIM, 1)
    df_to_download = df_links[df_links.apply(lambda row: data_inicio <= datetime(row['ano'], row['mes'], 1) <= data_fim, axis=1)]

    if df_to_download.empty:
        logging.warning("Nenhum arquivo novo encontrado para o perÃ­odo selecionado. Encerrando.")
        return
        
    download_files(df_to_download)

    # 4. Limpeza e ConsolidaÃ§Ã£o
    clean_downloaded_files(PASTA_DOWNLOADS_BRUTOS, PASTA_ARQUIVOS_LIMPOS)
    df_consolidado = consolidate_cleaned_files(PASTA_ARQUIVOS_LIMPOS, ARQUIVO_CONSOLIDADO_TEMP)
    
    if df_consolidado is None:
        logging.error("A consolidaÃ§Ã£o falhou. NÃ£o Ã© possÃ­vel continuar.")
        return

    # 5. Processamento de VigÃªncias
    df_vigencias_final = process_vigencias(df_consolidado)

    # 6. Salvar o Resultado Final
    df_vigencias_final.to_csv(ARQUIVO_FINAL_VIGENCIAS, sep=';', index=False, encoding='utf-8')
    logging.info(f"[OK] Pipeline concluido! Arquivo final salvo em: {os.path.abspath(ARQUIVO_FINAL_VIGENCIAS)}")
    logging.info(f"Tamanho final do DataFrame: {len(df_vigencias_final):,} linhas.")
    
    # 7. Garantir compatibilidade: copiar para output/anvisa/ (se necessÃ¡rio)
    output_anvisa_dir = PROJECT_ROOT / 'output' / 'anvisa'
    os.makedirs(output_anvisa_dir, exist_ok=True)
    output_copy_path = output_anvisa_dir / 'baseANVISA.csv'
    
    # Apenas copiar se o arquivo nÃ£o existir ou for mais antigo
    deve_copiar = True
    if os.path.exists(output_copy_path):
        time_output = os.path.getmtime(output_copy_path)
        time_source = os.path.getmtime(ARQUIVO_FINAL_VIGENCIAS)
        deve_copiar = time_source > time_output
    
    if deve_copiar:
        import shutil as sh
        sh.copy2(ARQUIVO_FINAL_VIGENCIAS, output_copy_path)
        logging.info(f"[INFO] Base copiada para: {output_copy_path}")
    else:
        logging.info(f"[INFO] Base em output/anvisa/ jÃ¡ estÃ¡ atualizada.")

if __name__ == "__main__":
    main()
