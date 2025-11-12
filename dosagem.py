# -*- coding: utf-8 -*-
"""
Modulo para extracao de quantidades e dosagens de medicamentos.
Extrai informacoes estruturadas da coluna de apresentacao.
"""
import pandas as pd
import numpy as np
import re


def extrair_quantidades_medicamentos(df: pd.DataFrame, 
                                    coluna_apresentacao: str = "APRESENTACAO_NORMALIZADA", 
                                    debug: bool = False) -> pd.DataFrame:
    """
    Extrai informacoes estruturadas da coluna de apresentacao de medicamentos.

    Esta funcao processa uma string de apresentacao para extrair:
    - QUANTIDADE UNIDADES: O numero de itens primarios (frascos, ampolas, blisters, etc.).
    - QUANTIDADE MG: A soma de todas as dosagens em Miligramas (MG), convertendo G e MCG.
    - QUANTIDADE ML: A soma de todos os volumes em Mililitros (ML).
    - QUANTIDADE UI: A soma de todas as Unidades Internacionais (UI).

    A funcao utiliza uma hierarquia de regras para determinar a quantidade de unidades,
    priorizando os padroes mais explicitos e confiaveis.

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados.
        coluna_apresentacao (str): O nome da coluna com as strings de apresentacao.
        debug (bool): Se True, mantem a coluna 'UNIDADES_RULE' para fins de depuracao.

    Returns:
        pd.DataFrame: O DataFrame original com as novas colunas adicionadas:
            - QUANTIDADE UNIDADES (Int64)
            - QUANTIDADE MG (float)
            - QUANTIDADE ML (float)
            - QUANTIDADE UI (float)
    
    Exemplo:
        >>> df = extrair_quantidades_medicamentos(df)
        >>> print(df[['APRESENTACAO_NORMALIZADA', 'QUANTIDADE UNIDADES', 'QUANTIDADE MG']].head())
    """
    # Nomes das colunas de saida para garantir idempotencia (poder rodar varias vezes sem erro)
    colunas_saida = [
        "QUANTIDADE UNIDADES",
        "QUANTIDADE MG",
        "QUANTIDADE ML",
        "QUANTIDADE UI",
        "UNIDADES_RULE"
    ]
    # Remove colunas antigas se existirem para evitar duplicacao
    df = df.drop(columns=[c for c in colunas_saida if c in df.columns], errors="ignore")

    # --- DEFINICAO DAS EXPRESSOES REGULARES (REGEX) ---

    # Regex para identificar os tipos de "unidades" ou "itens" (frasco, ampola, etc.)
    tipos_item_regex = r'\b(FA|SER|ENV|AMP|CARP?|CART|BL|FR|BG|CAPS?|CX|CT|BOLSAS?|SACHES?|TUBOS?|XPE)\b'

    # Regex para unidades de medida que NAO devem ser contadas como "unidade de item"
    unidades_medida_regex = r'\b(ML|MG|MCG|G|UI|MM|MEQ|L)\b'

    # --- Regras para QUANTIDADE UNIDADES (em ordem de prioridade) ---
    # 1. Padrao mais confiavel: "CX 10 FA", "CT 50 AMP" (Caixa com X itens)
    re_cx_num_item = re.compile(r'\b(?:CX|CT)\s+(\d+)\s+' + tipos_item_regex, re.IGNORECASE)
    # 2. Padrao muito confiavel: "50 FA", "10 SACHES" (Numero seguido de item)
    re_num_item = re.compile(r'(\d+)\s+' + tipos_item_regex, re.IGNORECASE)
    # 3. Padrao "CX 50", "CT 20" (Caixa com um numero, sem especificar o item)
    re_cx_simples = re.compile(r'\b(?:CX|CT)\s+(\d+)\b', re.IGNORECASE)
    # 4. Padrao "X <NUMERO>", desde que nao seja uma unidade de medida. Ex: "BL X 30"
    re_x_generico = re.compile(r'X\s+(\d+)(?!\s+' + unidades_medida_regex + r')', re.IGNORECASE)
    # 5. Verifica se existe qualquer palavra que indique um item (para o fallback para 1)
    re_qualquer_item = re.compile(tipos_item_regex, re.IGNORECASE)

    # --- Regras para DOSAGENS e VOLUMES ---
    # Captura dosagens (MG, G, MCG), incluindo somas como "(50 + 12.5) MG"
    re_dosagem = re.compile(r'((?:\(?\s*\d+(?:[.,]\d+)?\s*(?:\+\s*)?)+\)?)\s*(MG|G|MCG)\b', re.IGNORECASE)
    # Captura volumes em ML
    re_ml = re.compile(r'(\d+(?:[.,]\d+)?)\s*ML\b', re.IGNORECASE)
    # Captura Unidades Internacionais (UI), tratando numeros como "25 000" e a sigla "U I"
    re_ui = re.compile(r'(\d+(?:[.,\s]\d{3})*)\s*(?:UI|U\s*I)\b', re.IGNORECASE)

    resultados = []
    
    for apresentacao in df[coluna_apresentacao].fillna(''):
        # --- PREPARACAO DO TEXTO ---
        texto = str(apresentacao).upper()
        # Remove separadores de milhar (ex: 1.000 -> 1000)
        texto = re.sub(r'(?<=\d)\.(?=\d{3}\b)', '', texto)
        # Padroniza virgula para ponto decimal
        texto = texto.replace(',', '.')

        # --- EXTRACAO DE DOSAGENS (MG, G, MCG) ---
        total_mg = 0.0
        dosagens_encontradas = False
        for match in re_dosagem.finditer(texto):
            valor_str, unidade = match.groups()
            # Extrai todos os numeros do trecho encontrado (para tratar somas)
            numeros = re.findall(r'\d+(?:\.\d+)?', valor_str)
            for num_str in numeros:
                try:
                    num = float(num_str)
                    if unidade == 'G':
                        total_mg += num * 1000.0
                    elif unidade == 'MCG':
                        total_mg += num / 1000.0
                    else:  # MG
                        total_mg += num
                    dosagens_encontradas = True
                except ValueError:
                    continue
        quantidade_mg = total_mg if dosagens_encontradas else np.nan

        # Tratamento especial para BISNAGA
        if "BISNAGA" in texto or re.search(r'\bBG\b', texto):
            # Procura o ultimo numero seguido de "G" (ex: '50 G', '10 G', etc.)
            matches_g = re.findall(r'(\d+(?:\.\d+)?)\s*G\b', texto)
            if matches_g:
                try:
                    ultimo_g = float(matches_g[-1])
                    quantidade_mg = ultimo_g * 1000.0  # converte G para mg
                except ValueError:
                    pass

        # --- EXTRACAO DE VOLUME (ML) ---
        ml_encontrados = re_ml.findall(texto)
        if ml_encontrados:
            quantidade_ml = sum(float(v) for v in ml_encontrados if v)
        else:
            quantidade_ml = np.nan

        # --- EXTRACAO DE UNIDADES INTERNACIONAIS (UI) ---
        ui_encontrados = re_ui.findall(texto)
        if ui_encontrados:
            total_ui = 0
            for valor_str in ui_encontrados:
                # Limpa o numero (remove espacos, pontos) antes de converter
                num_limpo = valor_str.replace(' ', '').replace('.', '')
                try:
                    total_ui += float(num_limpo)
                except ValueError:
                    continue
            quantidade_ui = total_ui if total_ui > 0 else np.nan
        else:
            quantidade_ui = np.nan

        # --- EXTRACAO DA QUANTIDADE DE UNIDADES (LOGICA HIERARQUICA) ---
        unidades = np.nan
        regra_aplicada = None

        # Sanitizacao: remove trechos como "X 100 ML" para nao confundir com "100 unidades"
        texto_unidades = re.sub(r'X\s+\d+(?:\.\d+)?\s+' + unidades_medida_regex, '', texto)

        # Aplica as regras em ordem de prioridade
        match_cx_num_item = re_cx_num_item.search(texto_unidades)
        if match_cx_num_item:
            unidades = int(match_cx_num_item.group(1))
            regra_aplicada = "CX_NUM_ITEM"
        else:
            match_num_item = re_num_item.search(texto_unidades)
            if match_num_item:
                unidades = int(match_num_item.group(1))
                regra_aplicada = "NUM_ITEM"
            else:
                match_cx_simples = re_cx_simples.search(texto_unidades)
                if match_cx_simples:
                    unidades = int(match_cx_simples.group(1))
                    regra_aplicada = "CX_SIMPLES"
                else:
                    match_x_generico = re_x_generico.search(texto_unidades)
                    if match_x_generico:
                        unidades = int(match_x_generico.group(1))
                        regra_aplicada = "X_GENERICO"
                    # Se nenhuma regra numerica funcionou, mas a apresentacao contem
                    # uma palavra de item (como 'SACHES' ou 'XPE'), assume-se que e 1 unidade.
                    elif pd.isna(unidades) and re_qualquer_item.search(texto_unidades):
                        unidades = 1
                        regra_aplicada = "FALLBACK_1_ITEM"

        resultados.append({
            "QUANTIDADE UNIDADES": unidades,
            "QUANTIDADE MG": quantidade_mg,
            "QUANTIDADE ML": quantidade_ml,
            "QUANTIDADE UI": quantidade_ui,
            "UNIDADES_RULE": regra_aplicada
        })

    # --- FINALIZACAO ---
    df_resultados = pd.DataFrame(resultados, index=df.index)
    df = pd.concat([df, df_resultados], axis=1)

    # Converte a coluna de unidades para inteiro, permitindo valores nulos (NaN)
    try:
        df["QUANTIDADE UNIDADES"] = df["QUANTIDADE UNIDADES"].astype("Int64")
    except Exception:
        pass  # Mantem como float se a conversao falhar

    # Preenche NaN com 1 (assume 1 unidade se nao detectado)
    df['QUANTIDADE UNIDADES'] = df['QUANTIDADE UNIDADES'].fillna(1)

    if not debug:
        df = df.drop(columns=["UNIDADES_RULE"], errors='ignore')

    return df


def processar_dosagem(df, debug=False):
    """
    Processa extracao de quantidades e dosagens para todo o DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame com coluna APRESENTACAO_NORMALIZADA
        debug (bool): Se True, mantem coluna UNIDADES_RULE para depuracao
        
    Returns:
        pandas.DataFrame: DataFrame com novas colunas de quantidades criadas
    """
    print("\n" + "=" * 80)
    print("EXTRACAO DE QUANTIDADES E DOSAGENS")
    print("=" * 80)
    
    if 'APRESENTACAO_NORMALIZADA' not in df.columns:
        print("[AVISO] Coluna 'APRESENTACAO_NORMALIZADA' nao encontrada. Pulando processamento.")
        return df
    
    # Extrair quantidades
    print("Extraindo quantidades e dosagens...")
    df = extrair_quantidades_medicamentos(df, coluna_apresentacao="APRESENTACAO_NORMALIZADA", debug=debug)
    
    # Estatisticas
    print(f"\n[OK] Extracao concluida!")
    
    print(f"\nEstatisticas de QUANTIDADE UNIDADES:")
    print(df['QUANTIDADE UNIDADES'].describe())
    
    if 'QUANTIDADE MG' in df.columns:
        mg_count = df['QUANTIDADE MG'].notna().sum()
        print(f"\nRegistros com QUANTIDADE MG: {mg_count:,} ({mg_count/len(df)*100:.1f}%)")
    
    if 'QUANTIDADE ML' in df.columns:
        ml_count = df['QUANTIDADE ML'].notna().sum()
        print(f"Registros com QUANTIDADE ML: {ml_count:,} ({ml_count/len(df)*100:.1f}%)")
    
    if 'QUANTIDADE UI' in df.columns:
        ui_count = df['QUANTIDADE UI'].notna().sum()
        print(f"Registros com QUANTIDADE UI: {ui_count:,} ({ui_count/len(df)*100:.1f}%)")
    
    return df


if __name__ == "__main__":
    print("Este modulo deve ser importado e usado em conjunto com outros modulos.")
    print("Para executar o pipeline completo, use o arquivo 'processar_dados.py'.")
