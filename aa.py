# -*- coding: utf-8 -*-
"""
Script automatizado para baixar, limpar e processar as listas de preços de medicamentos (PMVG) da Anvisa.
"""
import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString
import re
import os
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
# ==============================================================================
#      CONFIGURAÇÕES GERAIS E LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# --- PARÂMETROS DE EXECUÇÃO ---
# Define o início fixo do período de busca dos arquivos
ANO_INICIO = 2025
MES_INICIO = 1
# Calcula dinamicamente a data final como sendo o mês e ano atuais
hoje = datetime.now()
ANO_FIM = hoje.year
MES_FIM = hoje.month

# --- CONFIGURAÇÃO DE CAMINHOS ---
PASTA_DOWNLOADS_BRUTOS = "anvisa_pmvg_brutos"
PASTA_ARQUIVOS_LIMPOS = "anvisa_dados_limpos"
ARQUIVO_CONSOLIDADO_TEMP = "anvisa_pmvg_consolidado_temp.csv"
ARQUIVO_FINAL_VIGENCIAS = "base_anvisa_precos_vigencias.csv"

# --- PARÂMETROS DE DOWNLOAD E PROCESSAMENTO ---
URL_ANVISA = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/anos-anteriores/anos-anteriores"
MAX_DOWNLOAD_WORKERS = 6
MAX_CLEANING_THREADS = min(8, os.cpu_count() or 1)

# ==============================================================================
#      FUNÇÕES DO PIPELINE
# ==============================================================================

def scrape_anvisa_links():
    """Raspa a página da Anvisa para encontrar os links dos arquivos de preços."""
    logging.info(f"Acessando {URL_ANVISA} para extrair links...")
    
    meses_map = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4, 'maio': 5, 'junho': 6,
        'julho': 7, 'agosto': 8, 'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
    }
    rx_mesctx = re.compile(r'\b(janeiro|fevereiro|mar[çc]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s*/\s*(\d{2,4})\b', re.IGNORECASE)
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
        raise RuntimeError("div#content-core não encontrada na página da Anvisa!")

    dados = []
    ctx_year = ctx_month = None

    for node in core.descendants:
        if isinstance(node, NavigableString):
            m = rx_mesctx.search(node.strip().lower())
            if m:
                ctx_month = meses_map.get(m.group(1).lower().replace('ç', 'c'))
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
    session = requests.Session()
    session.headers.update({"User-Agent": "Python Automated Downloader"})
    BASE_FOLDER = Path(PASTA_DOWNLOADS_BRUTOS)

    def download_row(row):
        ano_cal, mes_cal = int(row.ano), int(row.mes)
        ano_fiscal = ano_cal - 1 if mes_cal <= 3 else ano_cal
        pasta = BASE_FOLDER / f"anvisa_ano_fiscal_{ano_fiscal}"
        pasta.mkdir(parents=True, exist_ok=True)
        ext = Path(row.url).suffix or ".xls"
        nome = f"{ano_cal}_{mes_cal:02d}_{row.mes_nome}{ext}"
        dest = pasta / nome

        if dest.exists():
            return f"✓ já existe: {dest.relative_to(BASE_FOLDER)}"
        
        for attempt in range(4):
            try:
                r = session.get(row.url, stream=True, timeout=30)
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(1024 * 128):
                        f.write(chunk)
                return f"✓ ok ({attempt+1}): {dest.relative_to(BASE_FOLDER)}"
            except requests.RequestException:
                time.sleep(5)
        return f"✗ falhou: {row.url.split('/')[-1]}"

    BASE_FOLDER.mkdir(exist_ok=True)
    logging.info(f"Iniciando downloads em {MAX_DOWNLOAD_WORKERS} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as exe:
        resultados = list(tqdm(exe.map(download_row, [row for _, row in df_to_download.iterrows()]), total=len(df_to_download), desc="Baixando arquivos"))
    
    ok = sum(r.startswith("✓") for r in resultados)
    fail = len(resultados) - ok
    logging.info(f"Resumo do download - Sucesso: {ok} | Falha: {fail}")
    if fail:
        for r in resultados:
            if r.startswith("✗"): logging.error(f" • {r}")

def clean_downloaded_files(source_folder, target_folder):
    """Limpa e padroniza os arquivos Excel baixados em paralelo."""
    all_files = sorted(glob.glob(f"{source_folder}/anvisa_ano_fiscal_*/*.xls*"))
    if not all_files:
        logging.warning("Nenhum arquivo .xls/.xlsx encontrado para processar.")
        return

    TARGET_COLUMNS = ['PRINCÍPIO ATIVO', 'SUBSTÂNCIA', 'CNPJ']

    def process_single_file(file_path):
        try:
            ext = os.path.splitext(file_path)[1].lower()
            engine = 'openpyxl' if ext == '.xlsx' else 'xlrd'
            df_preview = pd.read_excel(file_path, header=None, nrows=100, dtype=str, engine=engine)
            
            header_row_index = None
            for i, row in df_preview.iterrows():
                row_values = {str(v).strip().upper() for v in row.dropna()}
                if any(col in row_values for col in TARGET_COLUMNS):
                    header_row_index = i
                    break
            
            if header_row_index is None:
                return f"AVISO: Cabeçalho não encontrado -> {file_path}"
                
            df = pd.read_excel(file_path, header=None, skiprows=header_row_index + 1, dtype=str, engine=engine)
            header = df_preview.iloc[header_row_index].astype(str).str.strip().str.replace(r'\s+%', '%', regex=True).str.replace(r'\s+', ' ', regex=True).str.upper()
            df.columns = header

            filename = os.path.basename(file_path)
            ano_ref, mes_ref = int(filename.split('_')[0]), int(filename.split('_')[1])
            df['ANO_REF'], df['MES_REF'] = ano_ref, mes_ref
            
            cols_to_move = ['ANO_REF', 'MES_REF']
            df = df[cols_to_move + [c for c in df.columns if c not in cols_to_move]]
            
            output_name = f"ANVISA_LIMPO_{ano_ref}_{mes_ref:02d}.csv"
            df.to_csv(os.path.join(target_folder, output_name), sep=';', index=False)
            return f"OK: {filename} -> {output_name}"
        except Exception as e:
            return f"ERRO: {file_path} -> {e}"

    logging.info(f"Iniciando limpeza de {len(all_files)} arquivos com {MAX_CLEANING_THREADS} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CLEANING_THREADS) as exe:
        resultados = list(tqdm(exe.map(process_single_file, all_files), total=len(all_files), desc="Limpando arquivos"))
    
    logging.info("--- Resultados da Limpeza ---")
    for r in resultados: logging.info(f" -> {r}")

def consolidate_cleaned_files(source_folder, output_file):
    """Consolida todos os CSVs limpos em um único arquivo."""
    csv_files = sorted(glob.glob(os.path.join(source_folder, "*.csv")))
    if not csv_files:
        logging.warning("Nenhum arquivo CSV limpo encontrado para consolidar.")
        return None

    COLUNAS_PARA_MANTER = ['ANO_REF', 'MES_REF', 'PRINCÍPIO ATIVO', 'LABORATÓRIO', 'CÓDIGO GGREM', 'REGISTRO', 'EAN 1', 'EAN 2', 'EAN 3', 'PRODUTO', 'APRESENTAÇÃO', 'CLASSE TERAPÊUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO', 'PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%', 'ICMS 0%', 'CAP']
    VARIANTES_PRINCIPIO = ['PRINCIPIO ATIVO', 'PRINCÍPIO ATIVO', 'SUBSTÂNCIA', 'SUBSTANCIA']
    
    dfs = []
    for file in tqdm(csv_files, desc="Lendo CSVs limpos", ncols=100):
        try:
            df = pd.read_csv(file, sep=";", dtype=str)
            df.columns = df.columns.str.strip().str.upper()
            
            col_principio = next((c for c in df.columns if c in VARIANTES_PRINCIPIO), None)
            if col_principio and col_principio != "PRINCÍPIO ATIVO":
                df.rename(columns={col_principio: "PRINCÍPIO ATIVO"}, inplace=True)
            elif "PRINCÍPIO ATIVO" not in df.columns:
                df["PRINCÍPIO ATIVO"] = None
            
            colunas_existentes = [c for c in COLUNAS_PARA_MANTER if c in df.columns]
            dfs.append(df[colunas_existentes])
        except Exception as e:
            logging.error(f"Erro ao ler {file}: {e}")

    if not dfs:
        logging.error("Nenhum DataFrame válido foi carregado para consolidação.")
        return None

    logging.info("Concatenando bases...")
    df_consolidado = pd.concat(dfs, ignore_index=True, sort=False).dropna(how="all")
    df_consolidado = df_consolidado.dropna(subset=['PRODUTO', 'PRINCÍPIO ATIVO'])
    df_consolidado.to_csv(output_file, sep=";", index=False)
    logging.info(f"Consolidação concluída. Arquivo salvo em: {os.path.abspath(output_file)}")
    return df_consolidado

def process_vigencias(df_consolidado):
    """Processa o dataframe consolidado para criar a tabela final de vigências."""
    logging.info("Iniciando fase de consolidação de vigências...")
    df = df_consolidado.copy()

    # PASSO 1: Preparação
    cols_to_check = ['PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%', 'ICMS 0%', 'CAP']
    df['id_produto'] = df['REGISTRO'].astype(str).str.strip() + '-' + df['CÓDIGO GGREM'].astype(str).str.strip()
    df['DATA_REF'] = pd.to_datetime(df['ANO_REF'].astype(str) + '-' + df['MES_REF'].astype(str) + '-01')
    
    for col in ['PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%']:
        if col in df.columns:
            s = df[col].astype(str).str.replace(',', '.', regex=False).str.replace(r'\.(?=.*\.)', '', regex=True)
            df[col] = pd.to_numeric(s, errors='coerce')
    
    df.sort_values(['id_produto', 'DATA_REF'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # PASSO 2: Detecção de Mudanças
    logging.info("Detectando mudanças de preços...")
    mudanca_valores = df[cols_to_check].ne(df[cols_to_check].shift(1)).any(axis=1)
    mudanca_produto = df['id_produto'] != df['id_produto'].shift(1)
    inicio_vigencia = mudanca_produto | mudanca_valores

    # PASSO 3: Construção de Vigências
    logging.info("Construindo tabela de vigências...")
    df_vigencias = df[inicio_vigencia].copy()
    df_vigencias['VIG_INICIO'] = df_vigencias['DATA_REF']
    df_vigencias['VIG_FIM'] = df_vigencias.groupby('id_produto')['VIG_INICIO'].shift(-1) - pd.Timedelta(days=1)

    def calcular_vig_fim_final(vig_inicio_date):
        if pd.isna(vig_inicio_date): return None
        return pd.Timestamp(year=vig_inicio_date.year if vig_inicio_date.month <= 3 else vig_inicio_date.year + 1, month=4, day=15)
    
    last_vigencia_mask = df_vigencias['VIG_FIM'].isnull()
    df_vigencias.loc[last_vigencia_mask, 'VIG_FIM'] = df_vigencias.loc[last_vigencia_mask, 'VIG_INICIO'].apply(calcular_vig_fim_final)

    # PASSO 4: Finalização
    df_vigencias['id_preco'] = df_vigencias['id_produto'] + '_' + df_vigencias['VIG_INICIO'].dt.strftime('%Y%m%d')
    colunas_finais = ['id_preco', 'id_produto', 'VIG_INICIO', 'VIG_FIM', 'PRINCÍPIO ATIVO', 'LABORATÓRIO', 'CÓDIGO GGREM', 'REGISTRO', 'EAN 1', 'EAN 2', 'EAN 3', 'PRODUTO', 'APRESENTAÇÃO', 'CLASSE TERAPÊUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO', 'PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%', 'ICMS 0%', 'CAP']
    df_vigencias_final = df_vigencias[[col for col in colunas_finais if col in df_vigencias.columns]]
    
    # PASSO 5: Limpeza numérica final e preenchimento de preços
    def parse_num_seguro(x):
        if pd.isna(x): return np.nan
        s = re.sub(r"[^\d,.\-]", "", unicodedata.normalize("NFKC", str(x)))
        if "," in s and "." in s: s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
        elif "," in s: s = s.replace(",", ".")
        try: return float(s)
        except (ValueError, TypeError): return np.nan
        
    for c in ['PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%']:
        if c in df_vigencias_final.columns: df_vigencias_final[c] = df_vigencias_final[c].apply(parse_num_seguro)
            
    mask_pf = df_vigencias_final['PF 20%'].isnull() & df_vigencias_final['PF 0%'].notnull()
    df_vigencias_final.loc[mask_pf, 'PF 20%'] = (df_vigencias_final.loc[mask_pf, 'PF 0%'] * 1.25).round(2)
    mask_pmvg = df_vigencias_final['PMVG 20%'].isnull() & df_vigencias_final['PMVG 0%'].notnull()
    df_vigencias_final.loc[mask_pmvg, 'PMVG 20%'] = (df_vigencias_final.loc[mask_pmvg, 'PMVG 0%'] * 1.25).round(2)

    # PASSO 6: Padronização de atributos
    logging.info("Padronizando atributos de texto pela última vigência...")
    cols_to_standardize = ['PRINCÍPIO ATIVO', 'LABORATÓRIO', 'PRODUTO', 'APRESENTAÇÃO', 'CLASSE TERAPÊUTICA', 'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO']
    latest_data = df_vigencias_final.sort_values('VIG_INICIO').drop_duplicates(subset='id_produto', keep='last').set_index('id_produto')
    for col in [c for c in cols_to_standardize if c in df_vigencias_final.columns]:
        df_vigencias_final[col] = df_vigencias_final['id_produto'].map(latest_data[col])
        
    for col in df_vigencias_final.select_dtypes(include=['object']).columns:
        df_vigencias_final[col] = df_vigencias_final[col].str.upper()

    # PASSO 7: Remoção de duplicatas
    logging.info("Removendo duplicatas da chave final...")
    df_vigencias_final['quality_score'] = df_vigencias_final.notna().sum(axis=1)
    df_vigencias_final.sort_values(by=['id_produto', 'VIG_INICIO', 'quality_score'], ascending=[True, True, False], inplace=True)
    df_vigencias_final.drop_duplicates(subset=['id_produto', 'VIG_INICIO'], keep='first', inplace=True)
    df_vigencias_final.drop(columns=['quality_score'], inplace=True)
    
    return df_vigencias_final

def main():
    """Função principal que orquestra todo o pipeline."""
    
    # 1. Limpeza Inicial
    logging.info("Iniciando pipeline de atualização da base da Anvisa.")
    for folder in [PASTA_DOWNLOADS_BRUTOS, PASTA_ARQUIVOS_LIMPOS]:
        if os.path.exists(folder):
            shutil.rmtree(folder)
            logging.info(f"Pasta antiga '{folder}' removida.")
    os.makedirs(PASTA_ARQUIVOS_LIMPOS, exist_ok=True)

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
        logging.warning("Nenhum arquivo novo encontrado para o período selecionado. Encerrando.")
        return
        
    download_files(df_to_download)

    # 4. Limpeza e Consolidação
    clean_downloaded_files(PASTA_DOWNLOADS_BRUTOS, PASTA_ARQUIVOS_LIMPOS)
    df_consolidado = consolidate_cleaned_files(PASTA_ARQUIVOS_LIMPOS, ARQUIVO_CONSOLIDADO_TEMP)
    
    if df_consolidado is None:
        logging.error("A consolidação falhou. Não é possível continuar.")
        return

    # 5. Processamento de Vigências
    df_vigencias_final = process_vigencias(df_consolidado)

    # 6. Salvar o Resultado Final
    df_vigencias_final.to_csv(ARQUIVO_FINAL_VIGENCIAS, sep=';', index=False)
    logging.info(f"✅ Pipeline concluído! Arquivo final salvo em: {os.path.abspath(ARQUIVO_FINAL_VIGENCIAS)}")
    logging.info(f"Tamanho final do DataFrame: {len(df_vigencias_final):,} linhas.")

if __name__ == "__main__":
    main()