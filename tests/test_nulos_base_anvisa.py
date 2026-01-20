#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Teste rápido: conta nulos no arquivo output/anvisa/baseANVISA.csv
Uso: python tests/test_nulos_base_anvisa.py
"""
import pandas as pd
from pathlib import Path
import csv

FILE = Path('output/anvisa/baseANVISA.csv')
if not FILE.exists():
    print(f"Arquivo não encontrado: {FILE}")
    raise SystemExit(1)

print(f"Analisando: {FILE} \n")

# Detectar delimitador com csv.Sniffer (amostra)
def detect_delimiter(path, sample_bytes=8192):
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            sample = f.read(sample_bytes)
            dialect = csv.Sniffer().sniff(sample, delimiters=';,\t|')
            return dialect.delimiter
    except Exception:
        return ';'  # fallback

sep = detect_delimiter(FILE)
print(f"Usando separador detectado: '{sep}'\n")

# Tentar ler em chunks com engine python e pular linhas problemáticas
total = 0
counts = None
read_success = False
for encoding in ('utf-8', 'latin1'):
    try:
        for chunk in pd.read_csv(FILE, sep=sep, encoding=encoding, engine='python', on_bad_lines='skip', chunksize=200000):
            n = len(chunk)
            total += n
            nulls = chunk.isna().sum()
            if counts is None:
                counts = nulls
            else:
                counts = counts.add(nulls, fill_value=0)
        read_success = True
        break
    except Exception as e:
        print(f"Falha ao ler com encoding {encoding}: {e}")
        total = 0
        counts = None

if not read_success:
    print("Não foi possível ler o arquivo com os métodos padrões. Tente inspecionar manualmente.")
    raise SystemExit(1)

print(f"Total de linhas lidas: {int(total):,}\n")
print("Nulos por coluna (ordem decrescente):")
print(counts.sort_values(ascending=False).to_string())
