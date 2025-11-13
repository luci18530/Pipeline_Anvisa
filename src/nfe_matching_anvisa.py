"""
Módulo de Matching e Enriquecimento NFe com Base ANVISA (CMED)
Realiza join por EAN/Registro e busca de preços vigentes (merge_asof)
"""

import pandas as pd
import numpy as np
import ast
import gc
from datetime import datetime


# ============================================================
# FUNÇÕES AUXILIARES - NORMALIZAÇÃO DE CHAVES
# ============================================================

def ean_norm(col: pd.Series) -> pd.Series:
    """Normaliza códigos EAN para 13 dígitos"""
    s = (col.astype("string[pyarrow]").fillna("").str.strip()
           .str.replace(r"\D", "", regex=True).replace("", np.nan))
    s = s.where(s.str.len() != 14, s.str[-13:])
    s = s.str.zfill(13).where(s.str.len() == 13)
    s = s.replace("0000000000000", pd.NA)
    return s.astype("string")


def reg_norm(col: pd.Series) -> pd.Series:
    """Normaliza registros ANVISA para 13 dígitos"""
    s = (col.astype("string[pyarrow]").fillna("").str.strip()
           .str.replace(r"\D", "", regex=True).replace("", np.nan))
    s = s.str.slice(0, 13).str.zfill(13).replace("0000000000000", pd.NA)
    return s.astype("string")


# ============================================================
# FUNÇÃO PRINCIPAL - ENRIQUECIMENTO COM METADADOS CMED
# ============================================================

def enriquecer_dataframe_com_cmed(df: pd.DataFrame, dfpre_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece df (NF-e) com metadados da CMED usando dfpre (base única: produtos + preços).
    1) Cascata via EAN/REG para montar metadados e ID_CMED_PRODUTO_LIST (lista de IDs do dfpre).
    2) Retorna df enriquecido (sem ROW_ID).
    """
    
    print("\n" + "="*60)
    print("[INICIO] Enriquecimento com Metadados CMED")
    print("="*60 + "\n")
    
    # -------- Normaliza dfpre para colunas esperadas --------
    meds = dfpre_raw.copy()
    
    rename_map = {
        'ID_PRODUTO': 'ID_CMED_PRODUTO',
        'CÓDIGO GGREM': 'GGREM',
        'PRINCÍPIO ATIVO': 'PRINCIPIO ATIVO',
        'LABORATÓRIO': 'LABORATORIO',
        'APRESENTAÇÃO': 'APRESENTACAO',
        'CLASSE TERAPEUTICA': 'CLASSE TERAPEUTICA',
        'EAN 1': 'EAN_1',
        'EAN 2': 'EAN_2',
        'EAN 3': 'EAN_3'
    }
    meds.rename(columns={k: v for k, v in rename_map.items() if k in meds.columns}, inplace=True)
    
    # Garante existência e tipos corretos (ID_CMED_PRODUTO é STRING!)
    for c in ['ID_CMED_PRODUTO','GGREM','REGISTRO','EAN_1','EAN_2','EAN_3','CLASSE TERAPEUTICA','GRUPO TERAPEUTICO',
              'APRESENTACAO','PRINCIPIO ATIVO','PRODUTO','STATUS','TIPO DE PRODUTO',
              'QUANTIDADE UNIDADES','QUANTIDADE MG','QUANTIDADE ML','QUANTIDADE UI',
              'LABORATORIO','GRUPO ANATOMICO','CAP','ICMS 0%']:
        if c not in meds.columns:
            meds[c] = pd.NA
    
    meds['ID_CMED_PRODUTO'] = meds['ID_CMED_PRODUTO'].astype("string")
    meds["EAN1_KEY"] = ean_norm(meds.get("EAN_1"))
    meds["EAN2_KEY"] = ean_norm(meds.get("EAN_2"))
    meds["EAN3_KEY"] = ean_norm(meds.get("EAN_3"))
    meds["REG_KEY"]  = reg_norm(meds.get("REGISTRO"))
    
    meta_cols = [
        "ID_CMED_PRODUTO","GRUPO ANATOMICO","PRINCIPIO ATIVO","PRODUTO","STATUS","APRESENTACAO",
        "TIPO DE PRODUTO","QUANTIDADE UNIDADES","QUANTIDADE MG","QUANTIDADE ML","QUANTIDADE UI",
        "LABORATORIO","CLASSE TERAPEUTICA","GRUPO TERAPEUTICO","GGREM","EAN_1","EAN_2","EAN_3","REGISTRO"
    ]
    meta_cols = [c for c in meta_cols if c in meds.columns]
    
    # -------- Cópia leve do df principal + limpeza de resíduos (ANTES das chaves) --------
    m = df.copy(deep=False)
    
    out_cols = ['ID_CMED_PRODUTO_LIST','match_via','EAN_KEY','REG_KEY'] + [c for c in meta_cols if c != 'ID_CMED_PRODUTO']
    cols_to_drop = [c for c in out_cols if c in m.columns]
    if cols_to_drop:
        m.drop(columns=cols_to_drop, inplace=True)
    
    # Agora cria chaves e colunas auxiliares
    m["EAN_KEY"] = ean_norm(m.get("codigo_ean"))
    m["REG_KEY"] = reg_norm(m.get("cod_anvisa"))
    m["match_via"] = pd.NA
    m["ROW_ID"] = m.index
    
    sem_gtin = m.get("codigo_ean").astype("string").str.fullmatch(r"(?i)\s*sem\s*gtin\s*").fillna(False)
    
    # Prepara colunas alvo
    for col in ['ID_CMED_PRODUTO_LIST'] + [c for c in meta_cols if c != 'ID_CMED_PRODUTO']:
        if col not in m.columns:
            m[col] = (pd.Series(dtype='object', index=m.index) if col == 'ID_CMED_PRODUTO_LIST' else pd.NA)
    
    # -------- Lookups por EAN/REG -> agregam lista de ID_CMED_PRODUTO (strings) --------
    def create_lookup_agg(df_, key_col):
        if key_col not in df_.columns or df_[key_col].isna().all():
            return pd.DataFrame()
        agg_ops = {'ID_CMED_PRODUTO': list}
        for col in meta_cols:
            if col != 'ID_CMED_PRODUTO':
                agg_ops[col] = 'first'
        lookup = (df_.dropna(subset=[key_col])
                     .groupby(key_col, sort=False, observed=True)
                     .agg(agg_ops)
                     .reset_index())
        lookup["ID_CMED_PRODUTO"] = lookup["ID_CMED_PRODUTO"].apply(lambda x: sorted(set([str(v) for v in x if pd.notna(v)])))
        return lookup.rename(columns={'ID_CMED_PRODUTO': 'ID_CMED_PRODUTO_LIST', key_col: '__JOIN__'})
    
    print("[INFO] Criando lookups por EAN e Registro...")
    lk_e1 = create_lookup_agg(meds, "EAN1_KEY")
    lk_e2 = create_lookup_agg(meds, "EAN2_KEY")
    lk_e3 = create_lookup_agg(meds, "EAN3_KEY")
    lk_reg = create_lookup_agg(meds, "REG_KEY")
    
    # -------- Cascata (preenche ID_CMED_PRODUTO_LIST e metadados) --------
    def step_merge(left_df, key_col, right_df, via_tag, _sem_gtin_mask):
        if right_df.empty:
            return
        mask = left_df[key_col].notna() & left_df['ID_CMED_PRODUTO_LIST'].isna()
        if "EAN" in key_col:
            mask &= ~_sem_gtin_mask
        idx = left_df.index[mask]
        if idx.empty:
            return
        tmp = (left_df.loc[idx, ["ROW_ID", key_col]]
                        .merge(right_df, left_on=key_col, right_on="__JOIN__", how="left")
                        .set_index("ROW_ID"))
        matched = tmp['__JOIN__'].notna()
        if matched.any():
            cols_upd = [c for c in right_df.columns if c != '__JOIN__']
            left_df.update(tmp.loc[matched, cols_upd])
            left_df.loc[tmp.index[matched], "match_via"] = via_tag
    
    print("[INFO] Executando cascata de matching (EAN1 -> EAN2 -> EAN3 -> REG)...")
    step_merge(m, "EAN_KEY", lk_e1, "ean1", sem_gtin)
    step_merge(m, "EAN_KEY", lk_e2, "ean2", sem_gtin)
    step_merge(m, "EAN_KEY", lk_e3, "ean3", sem_gtin)
    step_merge(m, "REG_KEY", lk_reg, "reg",  sem_gtin)
    
    print("\n[Auditoria] Contagem de matches por via:")
    print(m["match_via"].value_counts(dropna=False).rename("contagem"))
    print(f"\nTotal de linhas com lista de ID_CMED_PRODUTO: {m['ID_CMED_PRODUTO_LIST'].notna().sum():,}")
    
    print("\n" + "="*60)
    print("[SUCESSO] Enriquecimento com metadados concluído")
    print("="*60)
    
    return m.drop(columns=["ROW_ID"])


# ============================================================
# PREPARAÇÃO DA BASE DE PREÇOS
# ============================================================

def preparar_base_precos(dfpre: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara base de preços CMED para junção as-of
    Aplica regras de seleção de preço máximo (CAP, ICMS)
    """
    
    print("\n" + "="*60)
    print("[INICIO] Preparação da Base de Preços CMED")
    print("="*60 + "\n")
    
    dfpre_proc = dfpre.copy()
    
    dfpre_proc.rename(columns={
        'ID_PRODUTO': 'ID_CMED_PRODUTO',
        'CÓDIGO GGREM': 'GGREM',
        'PRINCÍPIO ATIVO': 'PRINCIPIO ATIVO',
        'LABORATÓRIO': 'LABORATORIO',
        'APRESENTAÇÃO': 'APRESENTACAO',
        'CLASSE TERAPEUTICA': 'CLASSE TERAPEUTICA',
        'EAN 1': 'EAN_1',
        'EAN 2': 'EAN_2',
        'EAN 3': 'EAN_3'
    }, inplace=True)
    
    dfpre_proc['ID_CMED_PRODUTO'] = dfpre_proc['ID_CMED_PRODUTO'].astype("string")
    dfpre_proc['VIG_INICIO'] = pd.to_datetime(dfpre_proc['VIG_INICIO'], errors='coerce')
    dfpre_proc['VIG_FIM']    = pd.to_datetime(dfpre_proc['VIG_FIM'],    errors='coerce')
    
    def _to_num(x):
        """Converte strings monetárias para float"""
        if pd.isna(x):
            return np.nan
        try:
            return float(x)
        except Exception:
            return np.nan
    
    print("[INFO] Convertendo colunas de preço...")
    for col in ['PF 0%', 'PF 20%', 'PMVG 0%', 'PMVG 20%']:
        if col in dfpre_proc.columns:
            dfpre_proc[col] = dfpre_proc[col].map(_to_num)
    
    # Flags CAP e ICMS 0%
    print("[INFO] Criando flags CAP e ICMS 0%...")
    dfpre_proc['CAP_FLAG'] = (
        dfpre_proc.get('CAP', pd.Series(index=dfpre_proc.index))
        .astype(str).str.upper().eq('SIM').astype('Int8')
    )
    
    dfpre_proc['ICMS0_FLAG'] = (
        dfpre_proc.get('ICMS 0%', pd.Series(index=dfpre_proc.index))
        .astype(str).str.upper().eq('SIM').astype('Int8')
        if 'ICMS 0%' in dfpre_proc.columns
        else pd.Series(0, index=dfpre_proc.index, dtype='Int8')
    )
    
    # Seleção do preço máximo refinado (regra CMED)
    print("[INFO] Aplicando regras de seleção de preço máximo...")
    dfpre_proc['PRECO_MAXIMO_REFINADO'] = np.select(
        [
            (dfpre_proc['CAP_FLAG'] == 1) & (dfpre_proc['ICMS0_FLAG'] == 1),  # CAP + ICMS 0%
            (dfpre_proc['CAP_FLAG'] == 1) & (dfpre_proc['ICMS0_FLAG'] == 0),  # CAP + ICMS 20%
            (dfpre_proc['CAP_FLAG'] == 0) & (dfpre_proc['ICMS0_FLAG'] == 1),  # sem CAP + ICMS 0%
            (dfpre_proc['CAP_FLAG'] == 0) & (dfpre_proc['ICMS0_FLAG'] == 0)   # sem CAP + ICMS 20%
        ],
        [
            dfpre_proc['PMVG 0%'],
            dfpre_proc['PMVG 20%'],
            dfpre_proc['PF 0%'],
            dfpre_proc['PF 20%']
        ],
        default=np.nan
    )
    
    # Limpeza e ordenação
    print("[INFO] Limpando e ordenando base de preços...")
    dfpre_proc = dfpre_proc.dropna(subset=['ID_CMED_PRODUTO', 'VIG_INICIO']).copy()
    dfpre_proc.sort_values(['ID_CMED_PRODUTO', 'VIG_INICIO'], inplace=True)
    
    # Completa VIG_FIM com base no próximo início
    next_vig_inicio = dfpre_proc.groupby('ID_CMED_PRODUTO')['VIG_INICIO'].shift(-1)
    dfpre_proc['VIG_FIM'] = dfpre_proc['VIG_FIM'].fillna(next_vig_inicio - pd.Timedelta(days=1))
    
    # Mantém apenas colunas relevantes
    dfpre_proc = dfpre_proc[['ID_CMED_PRODUTO', 'VIG_INICIO', 'VIG_FIM',
                             'PRECO_MAXIMO_REFINADO', 'CAP_FLAG', 'ICMS0_FLAG']].copy()
    dfpre_proc.sort_values('VIG_INICIO', inplace=True)
    
    print(f"[OK] Base de preços preparada: {len(dfpre_proc):,} registros")
    print("="*60)
    print("[SUCESSO] Preparação da base de preços concluída")
    print("="*60)
    
    del next_vig_inicio
    gc.collect()
    
    return dfpre_proc


# ============================================================
# JUNÇÃO DE PREÇOS (MERGE AS-OF)
# ============================================================

def juntar_precos_vigentes(df_enriquecido: pd.DataFrame, dfpre_proc: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza junção as-of para encontrar preços vigentes na data de emissão da NFe
    """
    
    print("\n" + "="*60)
    print("[INICIO] Junção de Preços Vigentes (merge as-of)")
    print("="*60 + "\n")
    
    df_main = df_enriquecido.copy()
    df_main['data_emissao'] = pd.to_datetime(df_main['data_emissao'], errors='coerce')
    df_main['ROW_ID'] = df_main.index
    
    # Garante que ID_CMED_PRODUTO_LIST seja lista real de strings
    def _ensure_list(v):
        if isinstance(v, list):
            return [str(x) for x in v]
        if pd.isna(v):
            return np.nan
        s = str(v).strip()
        if s.startswith('[') and s.endswith(']'):
            try:
                parsed = ast.literal_eval(s)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed]
            except Exception:
                pass
            s = s[1:-1]
            parts = [p.strip().strip("'\"") for p in s.split(',') if p.strip()]
            return parts if parts else np.nan
        return [s]
    
    print("[INFO] Normalizando listas de ID_CMED_PRODUTO...")
    df_main['ID_CMED_PRODUTO_LIST'] = df_main['ID_CMED_PRODUTO_LIST'].apply(_ensure_list)
    
    lean_candidates = df_main[['ROW_ID','data_emissao','ID_CMED_PRODUTO_LIST']].copy()
    lean_candidates.dropna(subset=['ID_CMED_PRODUTO_LIST','data_emissao'], inplace=True)
    
    print(f"[INFO] Encontradas {len(lean_candidates):,} linhas candidatas com listas de IDs")
    
    # Explodir listas de IDs
    df_exploded = lean_candidates.explode('ID_CMED_PRODUTO_LIST', ignore_index=False)\
                                 .rename(columns={'ID_CMED_PRODUTO_LIST':'ID_CMED_PRODUTO'})
    
    df_exploded['ID_CMED_PRODUTO'] = df_exploded['ID_CMED_PRODUTO'].astype('string')
    df_exploded.dropna(subset=['ID_CMED_PRODUTO'], inplace=True)
    
    df_exploded.sort_values('data_emissao', inplace=True)
    dfpre_proc.sort_values('VIG_INICIO', inplace=True)
    
    print(f"[INFO] DataFrame 'explodido' para {len(df_exploded):,} linhas para junção")
    
    # Merge as-of
    print("[INFO] Executando merge_asof...")
    merged_candidates = pd.merge_asof(
        left=df_exploded,
        right=dfpre_proc,
        left_on='data_emissao',
        right_on='VIG_INICIO',
        by='ID_CMED_PRODUTO',
        direction='backward',
        allow_exact_matches=True
    )
    print("[OK] Junção 'as-of' concluída")
    
    del df_exploded
    gc.collect()
    
    # Filtrar preços válidos
    print("[INFO] Filtrando preços válidos dentro da vigência...")
    is_valid_match = (merged_candidates['VIG_FIM'].isna()) | (merged_candidates['data_emissao'] <= merged_candidates['VIG_FIM'])
    valid_prices = merged_candidates[is_valid_match].copy()
    first_valid_price = valid_prices.sort_values(['ROW_ID']).drop_duplicates(subset='ROW_ID', keep='first')
    
    print(f"[OK] Encontrados {len(first_valid_price):,} preços válidos únicos")
    
    del merged_candidates, valid_prices
    gc.collect()
    
    # Juntar de volta ao DataFrame principal
    cols_to_join = ['ROW_ID','PRECO_MAXIMO_REFINADO','CAP_FLAG','ICMS0_FLAG']
    result_to_join = first_valid_price[cols_to_join].rename(columns={
        'CAP_FLAG': 'CAP_FLAG_CORRIGIDO',
        'ICMS0_FLAG': 'ICMS0_FLAG_CORRIGIDO'
    })
    
    for col in ['PRECO_MAXIMO_REFINADO','CAP_FLAG_CORRIGIDO','ICMS0_FLAG_CORRIGIDO']:
        if col in df_main.columns:
            df_main.drop(columns=col, inplace=True)
    
    print("[INFO] Juntando resultados ao DataFrame principal...")
    df_final = df_main.merge(result_to_join, on='ROW_ID', how='left')
    df_final.drop(columns='ROW_ID', inplace=True)
    
    print("="*60)
    print("[SUCESSO] Junção de preços concluída")
    print("="*60)
    
    return df_final


# ============================================================
# FUNÇÃO PRINCIPAL - PIPELINE COMPLETO DE MATCHING
# ============================================================

def processar_matching_anvisa(df_nfe: pd.DataFrame, dfpre_anvisa: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline completo de matching entre NFe e base ANVISA
    
    1. Enriquece com metadados CMED (produto, laboratório, classe terapêutica)
    2. Prepara base de preços com regras CAP/ICMS
    3. Junta preços vigentes via merge_asof
    
    Retorna DataFrame enriquecido com colunas ANVISA
    """
    
    print("\n" + "#"*70)
    print("#" + " "*68 + "#")
    print("#" + " "*15 + "PIPELINE DE MATCHING NFe x ANVISA (CMED)" + " "*14 + "#")
    print("#" + " "*68 + "#")
    print("#"*70 + "\n")
    
    inicio = datetime.now()
    
    # Etapa 1: Enriquecimento com metadados
    df_enriquecido = enriquecer_dataframe_com_cmed(df_nfe, dfpre_anvisa)
    
    # Conversões numéricas
    for col in ['ano_emissao', 'mes_emissao', 'QUANTIDADE UNIDADES']:
        if col in df_enriquecido.columns:
            df_enriquecido[col] = pd.to_numeric(df_enriquecido[col], errors='coerce').astype('Int64')
    
    # Etapa 2: Preparação da base de preços
    dfpre_proc = preparar_base_precos(dfpre_anvisa)
    
    # Etapa 3: Junção de preços vigentes
    df_final = juntar_precos_vigentes(df_enriquecido, dfpre_proc)
    
    # Relatório final
    duracao = (datetime.now() - inicio).total_seconds()
    
    print("\n" + "="*70)
    print(" "*20 + "RESULTADO DA JUNÇÃO DE PREÇOS")
    print("="*70)
    
    total_rows = len(df_final)
    matches = df_final['PRECO_MAXIMO_REFINADO'].notna().sum()
    num_candidates = df_final['ID_CMED_PRODUTO_LIST'].notna().sum()
    match_rate = (matches / num_candidates) * 100 if num_candidates > 0 else 0
    
    produtos_sem_nome = df_final["PRODUTO"].isna().sum()
    proporcao_null = produtos_sem_nome / total_rows if total_rows > 0 else 0
    
    print(f"\nTotal de linhas no DataFrame final: {total_rows:,}")
    print(f"Linhas candidatas (com lista de IDs): {num_candidates:,}")
    print(f"Linhas com preço válido encontrado: {matches:,}")
    print(f"Taxa de sucesso sobre os candidatos: {match_rate:.2f}%")
    print(f"\nProdutos sem nome (PRODUTO): {produtos_sem_nome:,} ({proporcao_null:.2%})")
    print(f"\nTempo de processamento: {duracao:.1f}s")
    
    print("="*70)
    print("[SUCESSO] Pipeline de matching concluído!")
    print("="*70 + "\n")
    
    del df_enriquecido, dfpre_proc
    gc.collect()
    
    return df_final
