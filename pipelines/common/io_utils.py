"""
Utilitários centralizados de I/O para leitura/escrita de CSV e ZIP.
Reduz duplicação de código e erros entre etapas do pipeline.
"""
from __future__ import annotations

import gc
import io
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Optional, Union

import pandas as pd

# Encodings padrão para tentativa de leitura
DEFAULT_ENCODINGS = ["utf-8-sig", "utf-8", "latin1", "cp1252", "iso-8859-1"]

# Configuração padrão de chunk para leitura de arquivos grandes
DEFAULT_CHUNK_SIZE = 100_000


def ler_csv(
    caminho: Union[str, Path],
    sep: str = ";",
    encoding: Optional[str] = None,
    low_memory: bool = False,
    dtype: Optional[dict] = None,
    usecols: Optional[list] = None,
    nrows: Optional[int] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Lê um arquivo CSV com tratamento robusto de encoding.
    
    Args:
        caminho: Caminho para o arquivo CSV
        sep: Separador de colunas (padrão: ";")
        encoding: Encoding específico (tenta vários se None)
        low_memory: Se True, usa leitura otimizada para memória
        dtype: Dicionário de tipos de coluna
        usecols: Lista de colunas a carregar
        nrows: Número máximo de linhas a ler
        **kwargs: Argumentos adicionais para pd.read_csv
    
    Returns:
        DataFrame com os dados carregados
    
    Raises:
        FileNotFoundError: Se o arquivo não existir
        ValueError: Se nenhum encoding funcionar
    """
    caminho = Path(caminho)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
    
    encodings = [encoding] if encoding else DEFAULT_ENCODINGS
    
    for enc in encodings:
        try:
            df = pd.read_csv(
                caminho,
                sep=sep,
                encoding=enc,
                low_memory=low_memory,
                dtype=dtype,
                usecols=usecols,
                nrows=nrows,
                **kwargs,
            )
            return df
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            # Se não for erro de encoding, propaga
            if "codec" not in str(e).lower() and "decode" not in str(e).lower():
                raise
    
    raise ValueError(f"Não foi possível ler {caminho} com nenhum encoding: {encodings}")


def ler_csv_chunked(
    caminho: Union[str, Path],
    sep: str = ";",
    encoding: str = "utf-8",
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    log_progresso: bool = True,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Lê um CSV grande em chunks para evitar MemoryError.
    
    Args:
        caminho: Caminho para o arquivo CSV
        sep: Separador de colunas
        encoding: Encoding do arquivo
        chunk_size: Tamanho de cada chunk
        log_progresso: Se True, exibe progresso
        **kwargs: Argumentos adicionais para pd.read_csv
    
    Returns:
        DataFrame concatenado de todos os chunks
    """
    caminho = Path(caminho)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
    
    chunks = []
    for i, chunk in enumerate(pd.read_csv(
        caminho,
        sep=sep,
        encoding=encoding,
        chunksize=chunk_size,
        low_memory=False,
        **kwargs,
    )):
        chunks.append(chunk)
        if log_progresso and (i + 1) % 10 == 0:
            print(f"[INFO] Carregados {(i + 1) * chunk_size:,} registros...")
    
    df = pd.concat(chunks, ignore_index=True)
    if log_progresso:
        print(f"[OK] Total carregado: {len(df):,} registros")
    return df


def ler_zip_csv(
    caminho_zip: Union[str, Path],
    sep: str = ";",
    encoding: str = "utf-8",
    chunk_size: Optional[int] = DEFAULT_CHUNK_SIZE,
    log_progresso: bool = True,
    nome_csv: Optional[str] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Lê um CSV compactado em ZIP, opcionalmente em chunks.
    
    Args:
        caminho_zip: Caminho para o arquivo ZIP
        sep: Separador de colunas
        encoding: Encoding do CSV
        chunk_size: Tamanho de chunk (None para leitura direta)
        log_progresso: Se True, exibe progresso
        nome_csv: Nome do CSV dentro do ZIP (auto-detecta se None)
        **kwargs: Argumentos adicionais para pd.read_csv
    
    Returns:
        DataFrame com os dados carregados
    
    Raises:
        FileNotFoundError: Se o ZIP não existir
        ValueError: Se nenhum CSV for encontrado no ZIP
    """
    caminho_zip = Path(caminho_zip)
    if not caminho_zip.exists():
        raise FileNotFoundError(f"Arquivo ZIP não encontrado: {caminho_zip}")
    
    with zipfile.ZipFile(caminho_zip, "r") as zf:
        # Auto-detectar nome do CSV
        if nome_csv is None:
            csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_files:
                raise ValueError(f"Nenhum CSV encontrado em {caminho_zip}")
            nome_csv = csv_files[0]
        
        if chunk_size:
            # Leitura em chunks via arquivo temporário
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", text=False)
            try:
                with os.fdopen(tmp_fd, "wb") as tmp_file:
                    with zf.open(nome_csv) as csv_source:
                        tmp_file.write(csv_source.read())
                
                df = ler_csv_chunked(
                    tmp_path,
                    sep=sep,
                    encoding=encoding,
                    chunk_size=chunk_size,
                    log_progresso=log_progresso,
                    **kwargs,
                )
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
        else:
            # Leitura direta em memória
            with zf.open(nome_csv) as csv_source:
                df = pd.read_csv(
                    io.BytesIO(csv_source.read()),
                    sep=sep,
                    encoding=encoding,
                    low_memory=False,
                    **kwargs,
                )
            if log_progresso:
                print(f"[OK] Carregados {len(df):,} registros de {nome_csv}")
    
    return df


def salvar_csv(
    df: pd.DataFrame,
    caminho: Union[str, Path],
    sep: str = ";",
    encoding: str = "utf-8",
    index: bool = False,
    **kwargs: Any,
) -> Path:
    """
    Salva um DataFrame como CSV.
    
    Args:
        df: DataFrame a salvar
        caminho: Caminho de destino
        sep: Separador de colunas
        encoding: Encoding do arquivo
        index: Se True, inclui índice
        **kwargs: Argumentos adicionais para to_csv
    
    Returns:
        Path do arquivo salvo
    """
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(caminho, sep=sep, encoding=encoding, index=index, **kwargs)
    return caminho


def salvar_zip_csv(
    df: pd.DataFrame,
    caminho_zip: Union[str, Path],
    nome_csv: Optional[str] = None,
    sep: str = ";",
    encoding: str = "utf-8",
    compression: int = zipfile.ZIP_DEFLATED,
    log_progresso: bool = True,
    **kwargs: Any,
) -> Path:
    """
    Salva um DataFrame como CSV compactado em ZIP.
    Usa arquivo temporário para evitar MemoryError em DataFrames grandes.
    
    Args:
        df: DataFrame a salvar
        caminho_zip: Caminho de destino para o ZIP
        nome_csv: Nome do CSV dentro do ZIP (derivado do ZIP se None)
        sep: Separador de colunas
        encoding: Encoding do arquivo
        compression: Tipo de compressão ZIP
        log_progresso: Se True, exibe progresso
        **kwargs: Argumentos adicionais para to_csv
    
    Returns:
        Path do arquivo ZIP salvo
    """
    caminho_zip = Path(caminho_zip)
    caminho_zip.parent.mkdir(parents=True, exist_ok=True)
    
    # Derivar nome do CSV do nome do ZIP
    if nome_csv is None:
        nome_csv = caminho_zip.stem + ".csv"
    
    # Usar arquivo temporário para evitar MemoryError
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding=encoding
    ) as tmp_file:
        tmp_path = tmp_file.name
        if log_progresso:
            print("[INFO] Salvando CSV temporário...")
        df.to_csv(tmp_file, sep=sep, index=False, **kwargs)
    
    try:
        if log_progresso:
            print("[INFO] Comprimindo arquivo...")
        with zipfile.ZipFile(caminho_zip, "w", compression) as zf:
            zf.write(tmp_path, nome_csv)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
    
    if log_progresso:
        tamanho_mb = caminho_zip.stat().st_size / (1024 * 1024)
        print(f"[OK] Arquivo salvo: {caminho_zip.name} ({tamanho_mb:.2f} MB)")
    
    return caminho_zip


def exportar_condicional(
    df: pd.DataFrame,
    caminho: Union[str, Path],
    exportar: bool = True,
    formato: Literal["csv", "zip"] = "zip",
    sep: str = ";",
    encoding: str = "utf-8",
    log_progresso: bool = True,
    **kwargs: Any,
) -> Optional[Path]:
    """
    Exporta um DataFrame condicionalmente (para modo pipeline rápido).
    
    Args:
        df: DataFrame a exportar
        caminho: Caminho de destino
        exportar: Se False, não exporta (modo rápido)
        formato: "csv" ou "zip"
        sep: Separador de colunas
        encoding: Encoding do arquivo
        log_progresso: Se True, exibe progresso
        **kwargs: Argumentos adicionais
    
    Returns:
        Path do arquivo salvo, ou None se exportar=False
    """
    if not exportar:
        if log_progresso:
            print("[INFO] Exportação desativada (modo pipeline rápido)")
        return None
    
    caminho = Path(caminho)
    
    if formato == "zip":
        return salvar_zip_csv(
            df, caminho, sep=sep, encoding=encoding,
            log_progresso=log_progresso, **kwargs
        )
    else:
        return salvar_csv(
            df, caminho, sep=sep, encoding=encoding, **kwargs
        )


def carregar_ou_processar(
    caminho_cache: Union[str, Path],
    funcao_processamento: Callable[[], pd.DataFrame],
    forcar_reprocessamento: bool = False,
    formato: Literal["csv", "zip"] = "zip",
    sep: str = ";",
    log_progresso: bool = True,
) -> pd.DataFrame:
    """
    Carrega dados de cache ou executa processamento se não existir.
    Útil para evitar reprocessamento desnecessário.
    
    Args:
        caminho_cache: Caminho do arquivo de cache
        funcao_processamento: Função que retorna DataFrame se cache não existir
        forcar_reprocessamento: Se True, ignora cache
        formato: Formato do cache ("csv" ou "zip")
        sep: Separador de colunas
        log_progresso: Se True, exibe progresso
    
    Returns:
        DataFrame carregado do cache ou processado
    """
    caminho_cache = Path(caminho_cache)
    
    if not forcar_reprocessamento and caminho_cache.exists():
        if log_progresso:
            print(f"[CACHE] Carregando de {caminho_cache.name}...")
        
        if formato == "zip":
            return ler_zip_csv(caminho_cache, sep=sep, log_progresso=log_progresso)
        else:
            return ler_csv(caminho_cache, sep=sep)
    
    if log_progresso:
        print("[INFO] Executando processamento...")
    
    df = funcao_processamento()
    
    # Salvar no cache
    if formato == "zip":
        salvar_zip_csv(df, caminho_cache, sep=sep, log_progresso=log_progresso)
    else:
        salvar_csv(df, caminho_cache, sep=sep)
    
    return df


def limpar_memoria(*dataframes: pd.DataFrame) -> None:
    """
    Limpa DataFrames da memória e força garbage collection.
    
    Args:
        *dataframes: DataFrames a limpar
    """
    for df in dataframes:
        if df is not None:
            del df
    gc.collect()
