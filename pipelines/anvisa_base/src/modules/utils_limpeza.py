"""
Módulo de Utilitários de Limpeza de Dados
Funções defensivas para evitar corrupção por colunas duplicadas
"""
import pandas as pd
from typing import List, Tuple


def limpar_colunas_duplicadas(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Remove colunas com sufixo .1, .2, .3 etc (indicativo de merge mal feito).
    
    Args:
        df: DataFrame a limpar
        verbose: Se True, imprime log das colunas removidas
        
    Returns:
        DataFrame limpo (sem colunas .1/.2/.3)
    """
    colunas_duplicadas = [
        col for col in df.columns 
        if '.1' in col or '.2' in col or '.3' in col
    ]
    
    if colunas_duplicadas:
        if verbose:
            print(f"[AVISO] {len(colunas_duplicadas)} colunas duplicadas detectadas:")
            for col in sorted(colunas_duplicadas)[:10]:
                print(f"  - {col}")
            if len(colunas_duplicadas) > 10:
                print(f"  ... e {len(colunas_duplicadas) - 10} mais")
        
        df = df.drop(columns=colunas_duplicadas, errors='ignore')
        
        if verbose:
            print(f"[OK] Colunas duplicadas removidas. Total agora: {len(df.columns)}")
    
    return df


def merge_seguro(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_on: str = None,
    right_on: str = None,
    on: str = None,
    how: str = 'left',
    remover_conflitantes_do_right: bool = True,
    verbose: bool = True
) -> pd.DataFrame:
    """
    Merge seguro que evita colunas .1 através de:
    1. Remoção de colunas conflitantes do right DataFrame
    2. Uso de suffixes explícitos
    3. Limpeza defensiva pós-merge
    
    Args:
        left: DataFrame left
        right: DataFrame right
        left_on: Coluna key no left
        right_on: Coluna key no right
        on: Coluna key (se mesma nos dois)
        how: Tipo de join ('left', 'inner', 'outer', 'right')
        remover_conflitantes_do_right: Se True, remove colunas conflitantes do right antes do merge
        verbose: Se True, imprime log
        
    Returns:
        DataFrame resultante do merge (limpo)
    """
    if verbose:
        print(f"[INFO] Merge seguro: {len(left)} x {len(right)} registros")
    
    # Identificar colunas conflitantes
    colunas_conflitantes = set(left.columns) & set(right.columns)
    
    # Remover key columns do conjunto de conflitantes
    if on:
        colunas_conflitantes.discard(on)
    if left_on:
        colunas_conflitantes.discard(left_on)
    if right_on:
        colunas_conflitantes.discard(right_on)
    
    if verbose and colunas_conflitantes:
        print(f"[INFO] {len(colunas_conflitantes)} colunas conflitantes identificadas")
        print(f"       Primeiras: {sorted(list(colunas_conflitantes))[:5]}")
    
    # Estratégia: remover conflitantes do right
    if remover_conflitantes_do_right and colunas_conflitantes:
        right_clean = right.drop(columns=list(colunas_conflitantes), errors='ignore')
        if verbose:
            print(f"[INFO] Colunas removidas do right: {len(colunas_conflitantes)}")
    else:
        right_clean = right
    
    # Executar merge com suffixes explícitos
    merge_kwargs = {
        'how': how,
        'suffixes': ('', '_DROP')  # Sufixo para colunas inesperadas
    }
    
    if on:
        merge_kwargs['on'] = on
    else:
        if left_on:
            merge_kwargs['left_on'] = left_on
        if right_on:
            merge_kwargs['right_on'] = right_on
    
    df_resultado = pd.merge(left, right_clean, **merge_kwargs)
    
    # Limpeza pós-merge
    # 1. Remover colunas _DROP
    colunas_drop = [c for c in df_resultado.columns if c.endswith('_DROP')]
    if colunas_drop:
        df_resultado = df_resultado.drop(columns=colunas_drop)
        if verbose:
            print(f"[INFO] Colunas _DROP removidas: {len(colunas_drop)}")
    
    # 2. Remover colunas .1/.2 (defesa final)
    df_resultado = limpar_colunas_duplicadas(df_resultado, verbose=verbose)
    
    if verbose:
        print(f"[OK] Merge concluído: {len(df_resultado)} registros, {len(df_resultado.columns)} colunas")
    
    return df_resultado


def validar_integridade_colunas(df: pd.DataFrame, etapa: str = "", raise_on_error: bool = False) -> bool:
    """
    Valida integridade das colunas (detecta duplicadas, nomes estranhos, etc).
    
    Args:
        df: DataFrame a validar
        etapa: Nome da etapa (para log)
        raise_on_error: Se True, lança exceção se encontrar problemas
        
    Returns:
        True se passou na validação, False caso contrário
    """
    problemas = []
    
    # 1. Colunas duplicadas (.1/.2)
    colunas_dupl = [c for c in df.columns if '.1' in c or '.2' in c or '.3' in c]
    if colunas_dupl:
        problemas.append(f"Colunas duplicadas: {len(colunas_dupl)} ({colunas_dupl[:3]}...)")
    
    # 2. Colunas com sufixo _DROP
    colunas_drop = [c for c in df.columns if c.endswith('_DROP')]
    if colunas_drop:
        problemas.append(f"Colunas _DROP: {len(colunas_drop)} ({colunas_drop[:3]}...)")
    
    # 3. Colunas vazias
    colunas_vazias = [c for c in df.columns if df[c].isna().all()]
    if colunas_vazias:
        problemas.append(f"Colunas 100% vazias: {len(colunas_vazias)}")
    
    # Log e ação
    if problemas:
        msg = f"[VALIDACAO] Etapa '{etapa}' - {len(problemas)} problema(s):"
        for p in problemas:
            msg += f"\n  - {p}"
        
        if raise_on_error:
            raise ValueError(msg)
        else:
            print(msg)
            return False
    else:
        print(f"[VALIDACAO] Etapa '{etapa}' - ✓ Integridade OK ({len(df.columns)} colunas)")
        return True


# Exemplo de uso
if __name__ == "__main__":
    print("Modulo de utilidades de limpeza carregado.")
    print("\nFunções disponíveis:")
    print("  1. limpar_colunas_duplicadas(df)")
    print("  2. merge_seguro(left, right, ...)")
    print("  3. validar_integridade_colunas(df, etapa)")
    print("\nImporte com: from utils_limpeza import *")
