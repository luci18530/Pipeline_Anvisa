# -*- coding: utf-8 -*-
"""
Modulo para processamento e normalizacao da coluna 'LABORATORIO'.
Remove siglas empresariais e padroniza nomes de laboratorios.
"""
import pandas as pd
import re
import unicodedata


def _remover_acentos(texto: str) -> str:
    if not isinstance(texto, str):
        return ""
    texto_normalizado = unicodedata.normalize("NFD", texto)
    return "".join(ch for ch in texto_normalizado if unicodedata.category(ch) != "Mn")


def _normalizar_nome_laboratorio(texto: str) -> str:
    nome = _remover_acentos(str(texto)).upper().strip()
    nome = re.sub(r"[.,;:/\\()'\"]", " ", nome)
    nome = re.sub(r"[-_]+", " ", nome)
    nome = re.sub(r"\bS\s*/\s*A\b", "", nome)
    nome = re.sub(r"\bS\.?\s*A\.?\b", "", nome)
    nome = re.sub(r"\b(SOCIEDADE\s+ANONIMA|LTDA|LIMITADA|LT|EIRELI|EPP|ME)\b", "", nome)

    # Unificacao explicita para evitar fragmentacao por sufixo operacional.
    nome = re.sub(
        r"\bLABORATORIOS\s+B\s*BRAUN\s+UNIDADE\s+DE\s+RECONDICIONAMENTO\b",
        "LABORATORIOS B BRAUN",
        nome,
    )

    nome = re.sub(r"\s+", " ", nome).strip()
    return nome


def _chave_laboratorio(nome_normalizado: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(nome_normalizado))


def processar_laboratorio(df):
    """
    Processa e normaliza a coluna LABORATORIO.
    
    Remove siglas empresariais comuns (LTDA, SA, EIRELI, EPP, etc.)
    e padroniza o formato dos nomes de laboratorios.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna 'LABORATORIO'
        
    Returns:
        pandas.DataFrame: DataFrame com coluna LABORATORIO normalizada
            e LABORATORIO_ORIGINAL criada como backup
    """
    print("\n" + "=" * 80)
    print("PROCESSAMENTO DE LABORATORIO")
    print("=" * 80)
    
    if 'LABORATORIO' not in df.columns:
        print("[AVISO] Coluna 'LABORATORIO' nao encontrada. Pulando processamento.")
        return df
    
    # Criar backup antes da normalizacao
    if 'LABORATORIO_ORIGINAL' not in df.columns:
        print("Criando backup 'LABORATORIO_ORIGINAL'...")
        df['LABORATORIO_ORIGINAL'] = df['LABORATORIO'].str.upper()
    
    # Contar valores unicos antes
    unicos_antes = df['LABORATORIO'].nunique()
    print(f"Laboratorios unicos antes da normalizacao: {unicos_antes:,}")
    
    # Aplicar limpeza de siglas empresariais e normalizacao canonica
    print("Removendo siglas empresariais e padronizando...")
    laboratorios_normalizados = df['LABORATORIO'].astype(str).apply(_normalizar_nome_laboratorio)

    # Unificacao por chave: elimina variacoes apenas de pontuacao/espacamento/sigla societaria
    chaves = laboratorios_normalizados.apply(_chave_laboratorio)
    tmp = pd.DataFrame({
        'LABORATORIO_NORMALIZADO': laboratorios_normalizados,
        'CHAVE_LAB': chaves,
    })
    tmp = tmp[tmp['CHAVE_LAB'] != ""]
    freq = (
        tmp.groupby(['CHAVE_LAB', 'LABORATORIO_NORMALIZADO'])
        .size()
        .reset_index(name='FREQ')
        .sort_values(['CHAVE_LAB', 'FREQ', 'LABORATORIO_NORMALIZADO'], ascending=[True, False, True])
    )
    mapa_canonico = (
        freq.drop_duplicates(subset=['CHAVE_LAB'], keep='first')
        .set_index('CHAVE_LAB')['LABORATORIO_NORMALIZADO']
    )

    df['LABORATORIO'] = laboratorios_normalizados
    mask_chave_valida = chaves.isin(mapa_canonico.index)
    df.loc[mask_chave_valida, 'LABORATORIO'] = chaves[mask_chave_valida].map(mapa_canonico)
    
    # Contar valores unicos depois
    unicos_depois = df['LABORATORIO'].nunique()
    reducao = unicos_antes - unicos_depois
    percentual = (reducao / unicos_antes * 100) if unicos_antes > 0 else 0
    
    print(f"\n[OK] Processamento de LABORATORIO concluido!")
    print(f"Laboratorios unicos apos normalizacao: {unicos_depois:,}")
    print(f"Reducao: {reducao:,} laboratorios ({percentual:.1f}%)")
    
    return df


if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")
