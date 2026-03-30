"""
Script: processar_separacao.py
Descrição: Executa a Etapa 9 - Separação e Filtragem de NFe
Autor: Pipeline ANVISA
Data: 2025-11-13
"""

import sys
import os
import glob
import json
import re
import zipfile
from pathlib import Path
from datetime import datetime

import pandas as pd

# Adicionar caminhos da pipeline ao path
CURRENT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = CURRENT_DIR.parent
PROJECT_ROOT = PIPELINE_ROOT.parent.parent
SRC_DIR = PIPELINE_ROOT / "src"

for path in (PROJECT_ROOT, PIPELINE_ROOT, SRC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from pipelines.nfe.src.nfe_etapa09_separacao import processar_separacao_e_filtragem
from pipelines.nfe.src.paths import SUPPORT_DIR


def _carregar_listas_filtro() -> tuple[list[str], list[str]]:
    palavras_path = SUPPORT_DIR / "palavras_remocao.json"
    termos_path = SUPPORT_DIR / "termos_remocao.json"

    palavras = []
    termos = []
    if palavras_path.exists():
        with palavras_path.open("r", encoding="utf-8") as f:
            palavras = json.load(f).get("palavras_a_remover", [])
    if termos_path.exists():
        with termos_path.open("r", encoding="utf-8") as f:
            termos = json.load(f).get("termos_a_remover", [])

    return palavras, termos


def _comprimir_csv_temporario(tmp_csv: str, destino_zip: str, nome_interno_csv: str) -> None:
    with zipfile.ZipFile(destino_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(tmp_csv, arcname=nome_interno_csv)


def _processar_separacao_streaming(arquivo_entrada: str, diretorio_dados: str) -> bool:
    print("\n[INFO] Ativando modo STREAMING para evitar MemoryError...")

    palavras, termos = _carregar_listas_filtro()
    padrao_palavras = (
        r"\b(" + "|".join(re.escape(p) for p in palavras) + r")\b"
        if palavras else None
    )
    padrao_termos = "|".join(re.escape(t) for t in termos) if termos else None

    tmp_completo = os.path.join(diretorio_dados, "_tmp_df_etapa09_completo.csv")
    tmp_trabalhando = os.path.join(diretorio_dados, "_tmp_df_etapa09_trabalhando.csv")
    zip_completo = os.path.join(diretorio_dados, "df_etapa09_completo.zip")
    zip_trabalhando = os.path.join(diretorio_dados, "df_etapa09_trabalhando.zip")

    for caminho in (tmp_completo, tmp_trabalhando):
        if os.path.exists(caminho):
            os.remove(caminho)

    header_completo = True
    header_trabalhando = True
    total_linhas = 0
    total_completo = 0
    total_trabalhando = 0
    total_removidas = 0

    chunk_size = 25_000
    for idx, chunk in enumerate(
        pd.read_csv(
            arquivo_entrada,
            sep=";",
            encoding="utf-8-sig",
            dtype=str,
            low_memory=True,
            chunksize=chunk_size,
        ),
        start=1,
    ):
        if "PRODUTO" not in chunk.columns:
            print("[ERRO] Coluna 'PRODUTO' nao encontrada no arquivo de entrada.")
            return False

        total_linhas += len(chunk)
        df_completo_chunk = chunk[chunk["PRODUTO"].notna()]
        df_trabalhando_chunk = chunk[chunk["PRODUTO"].isna()]

        antes_filtro = len(df_trabalhando_chunk)
        if "descricao_produto" in df_trabalhando_chunk.columns:
            if padrao_palavras:
                mask_palavras = df_trabalhando_chunk["descricao_produto"].str.contains(
                    padrao_palavras, case=False, na=False, regex=True
                )
                df_trabalhando_chunk = df_trabalhando_chunk[~mask_palavras]
            if padrao_termos:
                mask_termos = df_trabalhando_chunk["descricao_produto"].str.contains(
                    padrao_termos, case=False, na=False, regex=True
                )
                df_trabalhando_chunk = df_trabalhando_chunk[~mask_termos]
        total_removidas += (antes_filtro - len(df_trabalhando_chunk))

        total_completo += len(df_completo_chunk)
        total_trabalhando += len(df_trabalhando_chunk)

        if not df_completo_chunk.empty:
            df_completo_chunk.to_csv(
                tmp_completo,
                sep=";",
                index=False,
                mode="a",
                header=header_completo,
                encoding="utf-8-sig",
            )
            header_completo = False

        if not df_trabalhando_chunk.empty:
            df_trabalhando_chunk.to_csv(
                tmp_trabalhando,
                sep=";",
                index=False,
                mode="a",
                header=header_trabalhando,
                encoding="utf-8-sig",
            )
            header_trabalhando = False

        if idx % 20 == 0:
            print(
                f"[INFO] Chunk {idx}: linhas={total_linhas:,} | "
                f"completo={total_completo:,} | trabalhando={total_trabalhando:,}"
            )

    if not os.path.exists(tmp_completo):
        pd.DataFrame().to_csv(tmp_completo, sep=";", index=False, encoding="utf-8-sig")
    if not os.path.exists(tmp_trabalhando):
        pd.DataFrame().to_csv(tmp_trabalhando, sep=";", index=False, encoding="utf-8-sig")

    _comprimir_csv_temporario(tmp_completo, zip_completo, "df_etapa09_completo.csv")
    _comprimir_csv_temporario(tmp_trabalhando, zip_trabalhando, "df_etapa09_trabalhando.csv")

    os.remove(tmp_completo)
    os.remove(tmp_trabalhando)

    print("\n[OK] Processamento streaming concluido!")
    print(f"   Total linhas processadas: {total_linhas:,}")
    print(f"   df_completo:              {total_completo:,}")
    print(f"   df_trabalhando filtrado:  {total_trabalhando:,}")
    print(f"   Removidas por filtro:     {total_removidas:,}")
    print(f"   Arquivo: {os.path.basename(zip_completo)}")
    print(f"   Arquivo: {os.path.basename(zip_trabalhando)}")

    return True

def main():
    """Função principal para executar separação e filtragem."""
    
    print("\n" + "="*80)
    print("ETAPA 9: SEPARACAO E FILTRAGEM DE NFe")
    print("="*80)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*80)
    
    inicio_total = datetime.now()
    
    # ============================================================
    # 1. LOCALIZAR ARQUIVO DE ENTRADA
    # ============================================================
    
    diretorio_dados = "data/processed"
    
    print("\n[INFO] Procurando arquivo de entrada...")
    # Busca primeiro arquivo SEM timestamp
    arquivo_entrada = os.path.join(diretorio_dados, "nfe_etapa08_matched_manual.csv")
    
    if not os.path.exists(arquivo_entrada):
        # Fallback: procura com timestamp
        arquivos = sorted([
            f for f in os.listdir(diretorio_dados)
            if f.startswith("nfe_matched_manual_") and f.endswith(".csv")
        ], reverse=True)
        
        if not arquivos:
            print("[ERRO] Nenhum arquivo 'nfe_etapa08_matched_manual.csv' encontrado.")
            print("   Execute primeiro as Etapas 1-8 do pipeline.")
            return False
        
        arquivo_entrada = os.path.join(diretorio_dados, arquivos[0])
    tamanho_mb = os.path.getsize(arquivo_entrada) / (1024 * 1024)
    
    print(f"[OK] Arquivo encontrado:")
    print(f"   Nome: {os.path.basename(arquivo_entrada)}")
    print(f"   Tamanho: {tamanho_mb:.2f} MB")
    
    # ============================================================
    # 2. CARREGAR DADOS
    # ============================================================
    
    print(f"\n[INFO] Carregando dados...")
    try:
        # Evita inferencia agressiva de tipos (pico de memoria no parser do pandas).
        df = pd.read_csv(
            arquivo_entrada,
            sep=';',
            encoding='utf-8-sig',
            dtype=str,
            low_memory=True,
        )
        print(f"   [OK] Carregado com sucesso!")
        print(f"   Shape: {df.shape}")
        print(f"   Memoria: {df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")
    except MemoryError:
        return _processar_separacao_streaming(arquivo_entrada, diretorio_dados)
    except Exception as e:
        print(f"[ERRO] Erro ao carregar arquivo: {e}")
        return False
    
    # ============================================================
    # 3. PROCESSAR SEPARAÇÃO E FILTRAGEM
    # ============================================================
    
    try:
        df_completo, df_trabalhando = processar_separacao_e_filtragem(
            df_entrada=df,
            exportar=True,
            diretorio=diretorio_dados
        )
        
        if df_completo is None or df_trabalhando is None:
            print("[ERRO] Erro no processamento. Verifique os logs acima.")
            return False
            
    except Exception as e:
        print(f"\n[ERRO] Erro durante o processamento: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # ============================================================
    # 4. RESUMO FINAL
    # ============================================================
    
    duracao_total = (datetime.now() - inicio_total).total_seconds()
    
    print("\n" + "="*80)
    print("[SUCESSO] ETAPA 9 CONCLUIDA!")
    print("="*80)
    print(f"\n[INFO] Resumo dos Resultados:")
    print(f"   df_completo (matched):      {len(df_completo):,} registros")
    print(f"   df_trabalhando (filtrado):  {len(df_trabalhando):,} registros")
    print(f"\n[INFO] Tempo total de execucao: {duracao_total:.2f}s")
    print("="*80)
    
    # Lista arquivos gerados
    print("\n[INFO] Arquivos gerados:")
    arquivos_gerados = sorted([
        f for f in os.listdir(diretorio_dados)
        if (f.startswith("df_completo_") or f.startswith("df_trabalhando_")) 
        and f.endswith(".zip")
    ], reverse=True)
    
    for arquivo in arquivos_gerados[:4]:  # Mostra ultimos 2 de cada
        caminho = os.path.join(diretorio_dados, arquivo)
        tamanho = os.path.getsize(caminho) / (1024 * 1024)
        print(f"   - {arquivo} ({tamanho:.2f} MB)")
    
    print("\n" + "="*80)
    
    return True


if __name__ == "__main__":
    sucesso = main()
    sys.exit(0 if sucesso else 1)
