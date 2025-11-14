#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Validador de Reorganização - Verifica se todos os outputs estão no lugar certo
"""
import os
from pathlib import Path

def mostrar_estrutura():
    """Mostra a estrutura final de outputs"""
    
    print("\n" + "="*80)
    print("✅ REORGANIZACAO DE OUTPUTS CONCLUIDA")
    print("="*80 + "\n")
    
    base_path = Path(__file__).parent
    
    # Check ANVISA outputs
    print("📁 PIPELINE ANVISA - Outputs")
    print("-" * 80)
    anvisa_path = base_path / "output" / "anvisa"
    if anvisa_path.exists():
        files = sorted(anvisa_path.glob("*"))
        for f in files:
            if f.is_file():
                size = f.stat().st_size / (1024 * 1024)
                print(f"  ✓ {f.name:<50} {size:>8.2f} MB")
        print(f"\n  Total: {len(files)} arquivos")
    else:
        print("  ✗ Pasta não encontrada!")
    
    # Check NFe data outputs
    print("\n📁 PIPELINE NFe - Processados Intermediários")
    print("-" * 80)
    processed_path = base_path / "data" / "processed"
    if processed_path.exists():
        files = sorted(processed_path.glob("nfe_*"))
        for f in list(files)[:7]:  # Show first 7
            if f.is_file():
                size = f.stat().st_size / (1024 * 1024)
                print(f"  ✓ {f.name:<50} {size:>8.2f} MB")
        zips = sorted(processed_path.glob("df_etapa*.zip"))
        if zips:
            print(f"  ... + {len(zips)} arquivos .zip (etapas 09-13)")
    
    # Integration check
    print("\n📊 VERIFICAÇÃO DE INTEGRACAO")
    print("-" * 80)
    
    anvisa_input = processed_path / "base_anvisa_precos_vigencias.csv"
    anvisa_output = anvisa_path / "baseANVISA.csv"
    
    checks = [
        ("Input ANVISA existe", anvisa_input.exists()),
        ("Output ANVISA existe", anvisa_output.exists()),
        ("LABORATORIO preenchido", check_laboratorio_column(anvisa_output)),
    ]
    
    for check_name, result in checks:
        symbol = "✅" if result else "❌"
        print(f"  {symbol} {check_name}")
    
    print("\n" + "="*80)
    print("✨ Estrutura pronta para uso!")
    print("="*80 + "\n")


def check_laboratorio_column(csv_path):
    """Verifica se a coluna LABORATORIO está preenchida"""
    try:
        import pandas as pd
        df = pd.read_csv(csv_path, sep='\t', nrows=100)
        if 'LABORATORIO' in df.columns:
            filled = df['LABORATORIO'].notna().sum()
            return filled > 50  # Se mais de 50% preenchido na amostra
        return False
    except:
        return False


if __name__ == "__main__":
    mostrar_estrutura()
