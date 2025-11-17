"""
Módulo de carregamento e pré-processamento de dados de Notas Fiscais (NFe)
Adaptado do pipeline Colab para ambiente local
"""

import os
import pandas as pd
import glob
from datetime import datetime


# Configurações do módulo
EXPECTED_CSV_HEADER = [
    'id_descricao', 'descricao_produto', 'id_medicamento', 'cod_anvisa',
    'codigo_municipio_destinatario', 'data_emissao', 'codigo_ncm', 'codigo_ean',
    'valor_produtos', 'valor_unitario', 'quantidade', 'unidade',
    'cpf_cnpj_emitente', 'chave_codigo', 'cpf_cnpj', 'razao_social_emitente',
    'nome_fantasia_emitente', 'razao_social_destinatario', 'nome_fantasia_destinatario',
    'id_data_fabricacao', 'id_data_validade'
]

# Unidades a serem removidas
UNIDADES_INVALIDAS = {
    'BLOCO', 'TESTE', 'TES', 'T', 'TS', 'TST', 'KT', 'DZ', 'TBL',
    'BOMB', 'BD', 'JG', 'FD18', 'CXA1', 'BD38'
}

# Encodings a testar
ENCODINGS_TENTATIVAS = [
    "utf-8-sig",
    "utf-8",
    "latin1",
    "cp1252",
    "utf-16",
    "iso-8859-1",
]


def verificar_e_adicionar_cabecalho(caminho_csv):
    """
    Verifica se o CSV tem cabeçalho e adiciona se necessário.
    
    Parâmetros:
    - caminho_csv: caminho para o arquivo CSV
    
    Retorna:
    - True se já tinha cabeçalho, False se foi adicionado
    """
    header_string = ';'.join(EXPECTED_CSV_HEADER)
    
    # Ler primeira linha para verificar
    with open(caminho_csv, 'r', encoding='latin1', errors='replace') as f:
        try:
            first_line = f.readline().strip('\n\r')
        except Exception:
            first_line = ''
    
    # Se primeira linha é exatamente o cabeçalho esperado
    if first_line == header_string:
        print("[INFO] Arquivo já possui cabeçalho correto")
        return True
    
    # Verificar se primeira linha parece um cabeçalho (contém nomes de colunas conhecidos)
    primeira_linha_lower = first_line.lower()
    tem_colunas_conhecidas = any(col in primeira_linha_lower for col in 
                                  ['descricao_produto', 'data_emissao', 'razao_social', 'cod_anvisa'])
    
    if tem_colunas_conhecidas:
        print("[INFO] Arquivo parece ter cabeçalho (mas diferente do esperado)")
        return True
    
    # Verificar se primeira linha parece dados (só números e poucos caracteres)
    # Se tem muitos números separados por ponto-e-vírgula, provavelmente é dado
    partes = first_line.split(';')
    
    # Verificar se tem o número esperado de colunas
    if len(partes) == len(EXPECTED_CSV_HEADER):
        print(f"[INFO] Primeira linha tem {len(partes)} campos (número correto de colunas)")
        
        # Verificar se parece dados: primeira coluna é número, tem CPF/CNPJ, etc
        if partes[0].isdigit() or (len(partes) > 12 and len(partes[12]) in [11, 14]):
            print("[AVISO] Arquivo não possui cabeçalho. Adicionando cabeçalho...")
            
            # Ler todo o arquivo
            with open(caminho_csv, 'r', encoding='latin1', errors='replace') as f:
                lines = f.readlines()
            
            # Inserir cabeçalho no início
            lines.insert(0, header_string + '\n')
            
            # Reescrever arquivo
            with open(caminho_csv, 'w', encoding='latin1', errors='replace') as f:
                f.writelines(lines)
            
            print("[OK] Cabeçalho adicionado com sucesso")
            return False
    
    print(f"[AVISO] Não foi possível determinar se arquivo tem cabeçalho (colunas: {len(partes)})")
    print("[INFO] Assumindo que primeira linha é cabeçalho")
    return True


def carregar_csv_nfe(caminho_csv, encoding=None):
    """
    Carrega arquivo CSV de NFe com tratamento robusto de encoding.
    Detecta e adiciona cabeçalho se necessário.
    
    Parâmetros:
    - caminho_csv: caminho para o arquivo CSV
    - encoding: encoding específico (opcional, tenta vários se não especificado)
    
    Retorna:
    - DataFrame pandas com os dados carregados
    """
    if not os.path.exists(caminho_csv):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_csv}")
    
    print(f"[INFO] Carregando arquivo: {caminho_csv}")
    
    # Verificar se arquivo tem cabeçalho
    tem_cabecalho = verificar_e_adicionar_cabecalho(caminho_csv)
    
    df = None
    encodings = [encoding] if encoding else ENCODINGS_TENTATIVAS
    
    for enc in encodings:
        try:
            print(f"[INFO] Tentando encoding: {enc}")
            df = pd.read_csv(
                caminho_csv,
                sep=';',
                encoding=enc,
                low_memory=False,
                dtype=str
            )
            print(f"[OK] CSV carregado com sucesso usando encoding: {enc}")
            break
        except Exception as e:
            print(f"[AVISO] Falhou com {enc}: {str(e)[:100]}")
            continue
    
    if df is None:
        raise ValueError(
            "Não foi possível ler o CSV com os encodings testados. "
            "Verifique o arquivo manualmente."
        )
    
    return df


def normalizar_colunas(df):
    """
    Normaliza nomes de colunas removendo BOMs e espaços.
    Remove caracteres especiais de todas as células também.
    
    Parâmetros:
    - df: DataFrame pandas
    
    Retorna:
    - DataFrame com colunas normalizadas
    """
    print("[INFO] Normalizando nomes de colunas...")
    df.columns = [
        c.strip().replace('\ufeff', '').replace('\xa0', '').replace('ï»¿', '')
        for c in df.columns
    ]
    
    # Remover BOM e caracteres especiais de todas as células de string
    print("[INFO] Removendo BOMs e caracteres especiais de células...")
    for col in df.columns:
        if df[col].dtype == 'object':  # Colunas string
            # Remover BOM (UTF-8 BOM é EF BB BF, que em latin1 é ï»¿)
            df[col] = df[col].astype(str).str.replace('ï»¿', '', regex=False)
            df[col] = df[col].astype(str).str.replace('\ufeff', '', regex=False)
            df[col] = df[col].astype(str).str.replace('\xa0', ' ', regex=False)
            # Limpar espaços extras
            df[col] = df[col].astype(str).str.strip()
    
    return df


def processar_data_emissao(df, data_minima='2020-01-01'):
    """
    Processa coluna data_emissao: converte para datetime e filtra datas antigas.
    
    Parâmetros:
    - df: DataFrame pandas
    - data_minima: data mínima aceita (formato YYYY-MM-DD)
    
    Retorna:
    - DataFrame filtrado e com colunas de data processadas
    """
    if 'data_emissao' not in df.columns:
        print("[AVISO] Coluna 'data_emissao' não encontrada no CSV.")
        return df
    
    print("[INFO] Processando data_emissao...")
    
    # Backup da coluna original
    df['data_emissao_original'] = df['data_emissao']
    
    # Tentar converter para datetime
    try:
        df['data_emissao'] = pd.to_datetime(df['data_emissao'], errors='coerce', dayfirst=False)
    except Exception:
        df['data_emissao'] = pd.to_datetime(df['data_emissao'], errors='coerce')
    
    # Contar e reportar NaT
    nat_count = df['data_emissao'].isna().sum()
    total = len(df)
    print(f"[INFO] data_emissao -> NaT: {nat_count} ({nat_count/total*100:.2f}%)")
    
    if nat_count > 0:
        exemplos = df[df['data_emissao'].isna()]['data_emissao_original'].unique()[:5]
        print(f"[INFO] Exemplos de datas inválidas: {list(exemplos)}")
    
    # Filtrar por data mínima
    tamanho_antes = len(df)
    df = df[df['data_emissao'] >= data_minima]
    removidos = tamanho_antes - len(df)
    print(f"[INFO] Removidas {removidos} linhas com data anterior a {data_minima}")
    
    # Criar colunas de ano e mês
    if not df['data_emissao'].isna().all():
        df['ano_emissao'] = df['data_emissao'].dt.year
        df['mes_emissao'] = df['data_emissao'].dt.month
        print("[OK] Colunas ano_emissao e mes_emissao criadas")
    else:
        df['ano_emissao'] = pd.NA
        df['mes_emissao'] = pd.NA
        print("[AVISO] Todas as datas são inválidas")
    
    return df


def filtrar_unidades_invalidas(df):
    """
    Remove registros com unidades inválidas.
    
    Parâmetros:
    - df: DataFrame pandas
    
    Retorna:
    - DataFrame filtrado
    """
    if 'unidade' not in df.columns:
        print("[AVISO] Coluna 'unidade' não encontrada.")
        return df
    
    print("[INFO] Filtrando unidades inválidas...")
    tamanho_antes = len(df)
    df = df[~df['unidade'].isin(UNIDADES_INVALIDAS)]
    removidos = tamanho_antes - len(df)
    print(f"[INFO] Removidas {removidos} linhas com unidades inválidas")
    
    return df


def converter_colunas_numericas(df):
    """
    Converte colunas numéricas para tipo apropriado.
    
    Parâmetros:
    - df: DataFrame pandas
    
    Retorna:
    - DataFrame com colunas numéricas convertidas
    """
    print("[INFO] Convertendo colunas numéricas...")
    
    cols_to_numeric = ['valor_produtos', 'valor_unitario', 'quantidade']
    
    for col in cols_to_numeric:
        if col in df.columns:
            antes = df[col].dtype
            df[col] = pd.to_numeric(df[col], errors='coerce')
            nulos = df[col].isna().sum()
            print(f"[INFO] {col}: {antes} -> {df[col].dtype} (NaN: {nulos})")
    
    return df


def preprocessar_nfe(df, data_minima='2020-01-01'):
    """
    Pipeline completo de pré-processamento de dados NFe.
    
    Parâmetros:
    - df: DataFrame pandas com dados brutos
    - data_minima: data mínima aceita (formato YYYY-MM-DD)
    
    Retorna:
    - DataFrame processado
    """
    print("="*60)
    print("[INICIO] Pré-processamento de dados NFe")
    print("="*60)
    print(f"[INFO] Shape inicial: {df.shape}")
    
    # Pipeline de transformações
    df = normalizar_colunas(df)
    df = processar_data_emissao(df, data_minima)
    df = filtrar_unidades_invalidas(df)
    df = converter_colunas_numericas(df)
    
    print(f"[INFO] Shape final: {df.shape}")
    print("="*60)
    print("[SUCESSO] Pré-processamento concluído")
    print("="*60)
    
    return df


def carregar_e_processar_nfe(caminho_csv, data_minima='2020-01-01', encoding=None):
    """
    Função principal: carrega e processa arquivo CSV de NFe.
    
    Parâmetros:
    - caminho_csv: caminho para o arquivo CSV
    - data_minima: data mínima aceita (formato YYYY-MM-DD)
    - encoding: encoding específico (opcional)
    
    Retorna:
    - DataFrame pandas processado
    """
    # Carregar
    df = carregar_csv_nfe(caminho_csv, encoding)
    
    # Processar
    df = preprocessar_nfe(df, data_minima)
    
    # Exibir informações finais
    print("\n[INFO] Resumo do dataset:")
    print(f"  - Total de registros: {len(df):,}")
    print(f"  - Total de colunas: {len(df.columns)}")
    
    if 'data_emissao' in df.columns:
        data_min = df['data_emissao'].min()
        data_max = df['data_emissao'].max()
        print(f"  - Período: {data_min} a {data_max}")
    
    if 'ano_emissao' in df.columns and 'mes_emissao' in df.columns:
        print(f"\n[INFO] Distribuição por ano:")
        print(df['ano_emissao'].value_counts().sort_index())
    
    return df


def salvar_dados_processados(df, diretorio='data/processed', formato='csv'):
    """
    Salva DataFrame processado em CSV.
    
    Parâmetros:
    - df: DataFrame pandas
    - diretorio: diretório de destino
    - formato: sempre 'csv' (parquet removido)
    
    Retorna:
    - Caminho do arquivo salvo
    """
    os.makedirs(diretorio, exist_ok=True)
    
    arquivo = f"nfe_etapa01_processado.csv"
    caminho = os.path.join(diretorio, arquivo)
    
    df.to_csv(caminho, sep=';', index=False, encoding='utf-8')
    
    print(f"[OK] Dados salvos em: {caminho}")
    return caminho


# Exemplo de uso
if __name__ == "__main__":
    # Caminho do arquivo de entrada
    arquivo_entrada = "nfe/nfe.csv"
    
    # Carregar e processar
    df = carregar_e_processar_nfe(arquivo_entrada)
    
    # Mostrar primeiras linhas
    print("\n[INFO] Primeiras 5 linhas:")
    print(df.head())
    
    # Salvar dados processados
    caminho_saida = salvar_dados_processados(df, formato='csv')
    
    print("\n[SUCESSO] Processamento concluído!")
