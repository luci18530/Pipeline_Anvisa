# -*- coding: utf-8 -*-
"""
ETAPA 14: EXTRAÇÃO DE ATRIBUTOS VIA API DE IA (GEMINI)

Para as linhas que permaneceram sem correspondência após todas as etapas,
usa um LLM (Gemini) para extrair atributos estruturados da descricao_produto.

Input:  df_etapa13_trabalhando_restante.zip
Output: df_etapa14_extracao_ia.zip
        df_etapa14_final_enriquecido.zip
"""

import pandas as pd
import numpy as np
import zipfile
import os
import time
import re
import io
from pathlib import Path
import sys

# Adicionar path do projeto
PROJECT_ROOT = Path(__file__).resolve().parents[3]
PIPELINE_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = PROJECT_ROOT
SRC_DIR = PIPELINE_ROOT / "src"
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(SRC_DIR))

from pipeline_config import get_toggle
from paths import SUPPORT_DIR

# ==============================================================================
#      CONFIGURAÇÕES
# ==============================================================================

# --- TOGGLE DE CONTROLE ---
# Controlado externamente por pipeline_config.json
USAR_GEMINI_API = bool(get_toggle("etapa14", "usar_gemini_api", False))

# Configurações de processamento
BATCH_SIZE = 50
MAX_PARALLEL_REQUESTS = 3
CSV_SEPARATOR = ';'

# Caminhos
INPUT_ZIP = BASE_DIR / 'data' / 'processed' / 'df_etapa13_trabalhando_restante.zip'
OUTPUT_DIR = BASE_DIR / 'data' / 'processed'
OUTPUT_IA_ZIP = OUTPUT_DIR / 'df_etapa14_extracao_ia.zip'
OUTPUT_FINAL_ZIP = OUTPUT_DIR / 'df_etapa14_final_enriquecido.zip'

# Arquivos de suporte (já presentes no projeto)
DICT_LABS_PATH = SUPPORT_DIR / 'dicionario_labs_para_revisao.csv'
IA_RESULTS_PATH = SUPPORT_DIR / 'extracao_ia_medicamentos.csv'

# Colunas da IA
COLUNAS_IA = [
    'IA_PRODUTO',
    'IA_LABORATORIO',
    'IA_TIPO DA UNIDADE',
    'IA_QUANTIDADE MG (POR UNIDADE/ML)',
    'IA_QUANTIDADE ML',
    'IA_QUANTIDADE UI',
    'IA_QUANTIDADE UNIDADES'
]

COLUNAS_CSV = ['descricao_produto'] + COLUNAS_IA


# ==============================================================================
#      FUNÇÕES DE PREPARAÇÃO
# ==============================================================================

def carregar_dados_etapa13():
    """
    Carrega o DataFrame da etapa 13 (trabalhando restante).
    """
    print("\n" + "="*80)
    print("CARREGANDO DADOS DA ETAPA 13")
    print("="*80)
    
    if not INPUT_ZIP.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {INPUT_ZIP}")
    
    with zipfile.ZipFile(INPUT_ZIP, 'r') as z:
        csv_name = 'df_etapa13_trabalhando_restante.csv'
        with z.open(csv_name) as f:
            df = pd.read_csv(f, sep=';')
    
    print(f"[OK] Carregado: {len(df):,} registros")
    print(f"[OK] Colunas: {list(df.columns)}")
    
    return df


def preparar_dados_para_ia(df):
    """
    Prepara os dados para envio à API:
    - Trunca descrições em separadores (LOTE, VENC, etc.)
    - Remove produtos veterinários/não medicinais
    - Limpeza final
    """
    print("\n" + "="*80)
    print("PREPARANDO DADOS PARA A IA")
    print("="*80)
    
    df_prep = df.copy()
    linhas_iniciais = len(df_prep)
    
    # 1. Truncar em separadores comuns
    print("\n[1/3] Truncando descrições em separadores...")
    separadores = [
        'DESCONTINUADO', 'VALIDADE', 'LOTE', 'LOT', 'VENC', 'VAL', 'FAB',
        r'L\s+[0-9]', r'V\s+[0-9]'
    ]
    regex_pattern = r'\s+(' + '|'.join(separadores) + r')'
    df_prep['descricao_produto'] = df_prep['descricao_produto'].str.split(
        regex_pattern, n=1, expand=True
    )[0].str.strip()
    
    # 2. Remover produtos não medicinais
    print("[2/3] Removendo produtos veterinarios e nao medicinais...")
    keywords_to_remove = [
        "IVERMIN", "EQUINOS", "IVOMEC", "XILAZIN", "VERMIDOG", "CHEMITRI", "TERRACAM",
        "REPEL", "EXPOSIS", "COPO DESCARTAVEIS", "REFRESCO EM PO", "APROMAZIN", "VERMIFUGO"
    ]
    pattern_remove = '|'.join(re.escape(kw) for kw in keywords_to_remove)
    df_prep = df_prep[~df_prep['descricao_produto'].str.contains(
        pattern_remove, case=False, na=False
    )]
    removidos = linhas_iniciais - len(df_prep)
    print(f"  -> {removidos:,} linhas removidas")
    
    # 3. Limpeza final
    print("[3/3] Limpeza final...")
    df_prep['descricao_produto'] = df_prep['descricao_produto'].str.replace(
        r'\s+', ' ', regex=True
    ).str.strip()
    
    # Remover linhas marcadas como DELETAR (se houver coluna NOME_PRODUTO_LIMPO)
    if 'NOME_PRODUTO_LIMPO' in df_prep.columns:
        df_prep = df_prep[df_prep['NOME_PRODUTO_LIMPO'] != 'DELETAR']
    
    print(f"\n[OK] DataFrame preparado: {len(df_prep):,} registros")
    
    return df_prep


# ==============================================================================
#      PROCESSAMENTO COM GEMINI (OPCIONAL)
# ==============================================================================

def processar_com_gemini(df_para_ia):
    """
    Processa descrições com a API do Gemini.
    Esta função só é chamada se USAR_GEMINI_API = True.
    """
    print("\n" + "="*80)
    print("PROCESSAMENTO COM API DO GEMINI")
    print("="*80)
    
    try:
        import google.generativeai as genai
    except ImportError:
        print("[ERRO] Biblioteca google-generativeai não instalada.")
        print("Execute: pip install google-generativeai")
        return None
    
    # Configurar API Key
    api_key = os.environ.get('GOOGLE_API_KEY')
    if not api_key:
        print("[ERRO] GOOGLE_API_KEY não encontrada nas variáveis de ambiente.")
        print("Configure com: export GOOGLE_API_KEY='sua_chave_aqui'")
        return None
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    
    # Template do prompt
    prompt_template = f"""
Você é um robô de extração de dados farmacêuticos. Sua única tarefa é preencher uma tabela CSV a partir de um texto, seguindo as regras de forma literal e sem desvios.

**REGRAS DE OURO (PRIORIDADE MÁXIMA):**
1.  **FORMATO CSV**: Responda APENAS com o texto CSV, usando '{CSV_SEPARATOR}' como separador. Nenhuma outra palavra.
2.  **CABEÇALHO EXATO**: A primeira linha DEVE ser este cabeçalho:
    `{CSV_SEPARATOR.join(COLUNAS_CSV)}`
3.  **ANTI-ERRO DE 'MG'**: Sua tarefa mais importante é preencher a coluna `IA_QUANTIDADE MG (POR UNIDADE/ML)`. **NUNCA DEIXE ESTA COLUNA EM BRANCO** se a descrição contiver "MG". A dosagem em "MG" pode estar perto do nome do laboratório (ex: "METOPROLOL 25 MG ACCORD"). Extraia o número (`25`) para a coluna de MG.
4.  **SEM INVENÇÃO**: Se uma informação não existe, deixe o campo VAZIO. Não insira textos como "não disponível" ou caracteres aleatórios.

**GUIA DE EXTRAÇÃO COLUNA POR COLUNA:**

*   `descricao_produto`: Copie a descrição original exatamente como foi fornecida.
*   `IA_PRODUTO`: O nome do produto ou princípio ativo. Remova dosagens e quantidades.
*   `IA_LABORATORIO`: O laboratório (ex: ACCORD, EMS, TEUTO). Ele geralmente está no final da descrição.
*   `IA_TIPO DA UNIDADE`: A forma farmacêutica (`COMPRIMIDO`, `CAPSULA`, `SOLUCAO`, `INJETAVEL`, etc.).
    *   **Regra**: Se não houver um tipo claro, mas houver uma quantidade de unidades (ex: "COM 30"), use `UNIDADE`. Se for um líquido (ex: "100 ML"), use `SOLUCAO`.
*   `IA_QUANTIDADE MG (POR UNIDADE/ML)`: **A dosagem em miligramas (mg)**. APENAS o número.
    *   **Regra de Texto Colado**: Reconheça dosagens mesmo que estejam coladas no texto, como em `50MGACCORD`. Extraia `50`.
    *   **Regra de Conversão**: `1 G` = `1000 MG`; `100 MCG` = `0.1 MG`.
    *   **Regra de Soma**: Em dosagens como `875+125 MG`, some os valores (`1000`).
*   `IA_QUANTIDADE ML`: O volume em mililitros (ml). APENAS o número.
*   `IA_QUANTIDADE UI`: A dosagem em Unidades Internacionais (UI). APENAS o número.
*   `IA_QUANTIDADE UNIDADES`: A quantidade total de itens (comprimidos, ampolas, etc.). Procure por números associados a `COMPRIMIDOS`, `CAPSULAS`, `COM`, `C/`, `CX`.

**EXEMPLOS-CHAVE QUE VOCÊ DEVE SEGUIR:**

ENTRADA:
METOPROLOL 25 MG 30 COMPRIMIDOS ACCORD
DEXMEDETOMIDINA 100 MCG / ML 2 ML C/ 25 ACCORD
RISPERIDONA 1MGACCORD
MICOFENOL MOF 500 MG COMPRIMIDOS 50 ACCORD GEN
AMOXICILINA + CLAV 875+125MG COM 14

SAÍDA ESPERADA:
descricao_produto{CSV_SEPARATOR}IA_PRODUTO{CSV_SEPARATOR}IA_LABORATORIO{CSV_SEPARATOR}IA_TIPO DA UNIDADE{CSV_SEPARATOR}IA_QUANTIDADE MG (POR UNIDADE/ML){CSV_SEPARATOR}IA_QUANTIDADE ML{CSV_SEPARATOR}IA_QUANTIDADE UI{CSV_SEPARATOR}IA_QUANTIDADE UNIDADES
METOPROLOL 25 MG 30 COMPRIMIDOS ACCORD{CSV_SEPARATOR}METOPROLOL{CSV_SEPARATOR}ACCORD{CSV_SEPARATOR}COMPRIMIDO{CSV_SEPARATOR}25{CSV_SEPARATOR}{CSV_SEPARATOR}{CSV_SEPARATOR}30
DEXMEDETOMIDINA 100 MCG / ML 2 ML C/ 25 ACCORD{CSV_SEPARATOR}DEXMEDETOMIDINA{CSV_SEPARATOR}ACCORD{CSV_SEPARATOR}INJETAVEL{CSV_SEPARATOR}0.1{CSV_SEPARATOR}2{CSV_SEPARATOR}{CSV_SEPARATOR}25
RISPERIDONA 1MGACCORD{CSV_SEPARATOR}RISPERIDONA{CSV_SEPARATOR}ACCORD{CSV_SEPARATOR}UNIDADE{CSV_SEPARATOR}1{CSV_SEPARATOR}{CSV_SEPARATOR}{CSV_SEPARATOR}
MICOFENOL MOF 500 MG COMPRIMIDOS 50 ACCORD GEN{CSV_SEPARATOR}MICOFENOLATO DE MOFETILA{CSV_SEPARATOR}ACCORD{CSV_SEPARATOR}COMPRIMIDO{CSV_SEPARATOR}500{CSV_SEPARATOR}{CSV_SEPARATOR}{CSV_SEPARATOR}50
AMOXICILINA + CLAV 875+125MG COM 14{CSV_SEPARATOR}AMOXICILINA + ACIDO CLAVULANICO{CSV_SEPARATOR}{CSV_SEPARATOR}COMPRIMIDO{CSV_SEPARATOR}1000{CSV_SEPARATOR}{CSV_SEPARATOR}{CSV_SEPARATOR}14

Agora, processe a seguinte lista de produtos, seguindo TODAS as regras acima de forma literal:
---
"""
    
    # Processar em lotes
    descricoes = df_para_ia['descricao_produto'].unique().tolist()
    total_lotes = (len(descricoes) + BATCH_SIZE - 1) // BATCH_SIZE
    
    resultados = []
    
    for i in range(0, len(descricoes), BATCH_SIZE):
        batch = descricoes[i:i + BATCH_SIZE]
        lote_num = (i // BATCH_SIZE) + 1
        
        print(f"\n>> [Lote {lote_num}/{total_lotes}] Processando {len(batch)} descrições...")
        
        prompt_completo = prompt_template + "\n".join(batch)
        
        try:
            start = time.time()
            response = model.generate_content(prompt_completo)
            duration = time.time() - start
            
            print(f"<< [Lote {lote_num}] Resposta recebida em {duration:.2f}s")
            
            # Parsear resposta
            csv_text = response.text.strip().replace('```csv', '').replace('```', '').strip()
            
            if not csv_text:
                print(f"!! [Lote {lote_num}] Resposta vazia. Pulando.")
                continue
            
            df_batch = pd.read_csv(io.StringIO(csv_text), sep=CSV_SEPARATOR, on_bad_lines='warn')
            resultados.append(df_batch)
            
            print(f"✓ [Lote {lote_num}] Processado com sucesso ({len(df_batch)} linhas)")
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            print(f"!! ERRO no Lote {lote_num}: {type(e).__name__} - {e}")
            continue
    
    if not resultados:
        print("\n[AVISO] Nenhum resultado foi processado.")
        return None
    
    df_resultados = pd.concat(resultados, ignore_index=True)
    print(f"\n[OK] Total processado: {len(df_resultados):,} registros")
    
    return df_resultados


# ==============================================================================
#      PÓS-PROCESSAMENTO E JUNÇÃO
# ==============================================================================

def carregar_resultados_ia():
    """
    Carrega resultados da IA do arquivo de suporte.
    """
    print("\n" + "="*80)
    print("CARREGANDO RESULTADOS DA IA")
    print("="*80)
    
    if not IA_RESULTS_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {IA_RESULTS_PATH}")
    
    # Tentar detectar separador automaticamente
    df_ia = pd.read_csv(IA_RESULTS_PATH, sep=',')  # Arquivo usa vírgula
    print(f"[OK] Carregado: {len(df_ia):,} registros")
    print(f"[OK] Colunas: {list(df_ia.columns)[:5]}...")  # Mostra primeiras 5
    
    return df_ia


def criar_resultados_ia_vazios(df_base: pd.DataFrame) -> pd.DataFrame:
    """Gera DataFrame vazio com as colunas esperadas para a IA."""
    df_vazio = df_base[["descricao_produto"]].drop_duplicates().copy()
    for coluna in COLUNAS_IA:
        df_vazio[coluna] = pd.NA
    return df_vazio


def aplicar_correcoes_laboratorio(df_ia):
    """
    Aplica correções nos nomes de laboratórios usando dicionário.
    """
    print("\n" + "="*80)
    print("APLICANDO CORREÇÕES DE LABORATÓRIO")
    print("="*80)
    
    if not DICT_LABS_PATH.exists():
        print(f"[AVISO] Dicionario nao encontrado: {DICT_LABS_PATH}")
        print("Pulando correcoes de laboratorio.")
        return df_ia
    
    # Tentar diferentes encodings
    for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
        try:
            df_dict = pd.read_csv(DICT_LABS_PATH, sep=';', encoding=encoding)
            print(f"[OK] Dicionario carregado com encoding: {encoding}")
            break
        except UnicodeDecodeError:
            continue
    else:
        print("[AVISO] Nao foi possivel carregar o dicionario. Pulando correcoes.")
        return df_ia
    
    # Criar mapa de correções (ignorando NaN)
    df_dict_clean = df_dict.dropna(subset=['IA_LABORATORIO', 'IA_LABORATORIO_CORRIGIDO'])
    mapa_labs = dict(zip(
        df_dict_clean['IA_LABORATORIO'],
        df_dict_clean['IA_LABORATORIO_CORRIGIDO']
    ))
    
    # Aplicar correções
    if 'IA_LABORATORIO' not in df_ia.columns:
        print("[AVISO] Coluna IA_LABORATORIO nao encontrada no DataFrame da IA")
        return df_ia
    
    corrigidos_antes = df_ia['IA_LABORATORIO'].notna().sum()
    df_ia['IA_LABORATORIO'] = df_ia['IA_LABORATORIO'].map(mapa_labs).fillna(df_ia['IA_LABORATORIO'])
    corrigidos_depois = df_ia['IA_LABORATORIO'].notna().sum()
    
    print(f"[OK] Laboratorios corrigidos usando dicionario: {len(mapa_labs):,} mapeamentos")
    print(f"  -> Antes: {corrigidos_antes:,} preenchidos")
    print(f"  -> Depois: {corrigidos_depois:,} preenchidos")
    
    return df_ia


def juntar_resultados_ia(df_trabalhando, df_ia):
    """
    Junta os resultados da IA de volta ao DataFrame principal.
    """
    print("\n" + "="*80)
    print("JUNTANDO RESULTADOS DA IA")
    print("="*80)
    
    # Preparar DataFrame da IA
    colunas_para_juntar = ['descricao_produto'] + COLUNAS_IA
    df_ia_prep = df_ia[colunas_para_juntar].drop_duplicates(subset=['descricao_produto'])
    
    # Remover colunas que serão trazidas (para evitar conflitos)
    colunas_a_remover = [c for c in COLUNAS_IA if c in df_trabalhando.columns]
    df_trabalho = df_trabalhando.drop(columns=colunas_a_remover, errors='ignore')
    
    # Executar merge
    df_final = pd.merge(
        df_trabalho,
        df_ia_prep,
        on='descricao_produto',
        how='left'
    )
    
    # Estatísticas
    total = len(df_final)
    com_ia = df_final[COLUNAS_IA].notna().any(axis=1).sum()
    pct = (com_ia / total * 100) if total > 0 else 0
    
    print(f"[OK] Merge concluido:")
    print(f"  -> Total de registros: {total:,}")
    print(f"  -> Com dados da IA: {com_ia:,} ({pct:.1f}%)")
    print(f"  -> Sem dados da IA: {total - com_ia:,} ({100-pct:.1f}%)")
    
    return df_final


# ==============================================================================
#      EXPORTAÇÃO
# ==============================================================================

def exportar_resultados(df_ia, df_final):
    """
    Exporta os DataFrames finais.
    """
    print("\n" + "="*80)
    print("EXPORTANDO RESULTADOS")
    print("="*80)
    
    # Criar pasta se não existir
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Exportar extração da IA
    print(f"\n[1/2] Salvando: {OUTPUT_IA_ZIP.name}")
    with zipfile.ZipFile(OUTPUT_IA_ZIP, 'w', zipfile.ZIP_DEFLATED) as z:
        csv_buffer = io.StringIO()
        df_ia.to_csv(csv_buffer, sep=';', index=False)
        z.writestr('df_etapa14_extracao_ia.csv', csv_buffer.getvalue())
    
    tamanho_ia = OUTPUT_IA_ZIP.stat().st_size / (1024 * 1024)
    print(f"  -> {len(df_ia):,} registros, {tamanho_ia:.2f} MB")
    
    # 2. Exportar DataFrame final enriquecido
    print(f"\n[2/2] Salvando: {OUTPUT_FINAL_ZIP.name}")
    with zipfile.ZipFile(OUTPUT_FINAL_ZIP, 'w', zipfile.ZIP_DEFLATED) as z:
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, sep=';', index=False)
        z.writestr('df_etapa14_final_enriquecido.csv', csv_buffer.getvalue())
    
    tamanho_final = OUTPUT_FINAL_ZIP.stat().st_size / (1024 * 1024)
    print(f"  -> {len(df_final):,} registros, {tamanho_final:.2f} MB")
    
    print("\n[OK] Exportação concluída!")


# ==============================================================================
#      FUNÇÃO PRINCIPAL
# ==============================================================================

def processar_extracao_ia():
    """
    Orquestra toda a etapa 14.
    """
    print("\n" + "="*80)
    print("ETAPA 14: EXTRAÇÃO DE ATRIBUTOS VIA IA (GEMINI)")
    print("="*80)
    print(f"\nModo: {'PROCESSAR COM GEMINI' if USAR_GEMINI_API else 'USAR RESULTADOS EXISTENTES'}")
    
    inicio = time.time()
    
    try:
        # 1. Carregar dados da etapa 13
        df_trabalhando = carregar_dados_etapa13()
        
        # 2. Preparar dados para IA
        df_para_ia = preparar_dados_para_ia(df_trabalhando)
        
        # 3. Processar com Gemini OU carregar resultados existentes
        if USAR_GEMINI_API:
            df_ia = processar_com_gemini(df_para_ia)
            if df_ia is None:
                raise RuntimeError("Processamento com Gemini falhou")
        else:
            if IA_RESULTS_PATH.exists():
                df_ia = carregar_resultados_ia()
            else:
                print(f"[AVISO] {IA_RESULTS_PATH.name} não encontrado. Gerando estrutura vazia.")
                df_ia = criar_resultados_ia_vazios(df_para_ia)
                try:
                    IA_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
                    df_ia.to_csv(IA_RESULTS_PATH, index=False)
                    print(f"[OK] Placeholder salvo em {IA_RESULTS_PATH}")
                except Exception as exc:
                    print(f"[AVISO] Não foi possível salvar placeholder em disco: {exc}")
        
        # 4. Aplicar correções de laboratório
        df_ia = aplicar_correcoes_laboratorio(df_ia)
        
        # 5. Juntar resultados ao DataFrame principal
        df_final = juntar_resultados_ia(df_trabalhando, df_ia)
        
        # 6. Exportar
        exportar_resultados(df_ia, df_final)
        
        duracao = time.time() - inicio
        print("\n" + "="*80)
        print(f"[SUCESSO] ETAPA 14 CONCLUÍDA EM {duracao:.1f}s")
        print("="*80)
        
        return df_final
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"[ERRO] Falha na Etapa 14: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        return None


# ==============================================================================
#      EXECUÇÃO
# ==============================================================================

if __name__ == "__main__":
    df_final = processar_extracao_ia()
    
    if df_final is not None:
        print(f"\n✓ DataFrame final disponível com {len(df_final):,} registros")
        print(f"✓ Colunas: {len(df_final.columns)}")
