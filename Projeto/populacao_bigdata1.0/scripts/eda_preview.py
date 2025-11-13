#!/usr/bin/env python


import pandas as pd
from pathlib import Path

base = Path(".")
silver = base/"data"/"silver"/"population_clean.parquet"
raw_csv = base/"data"/"raw"/"worldbank_population.csv"

try:
    df = pd.read_parquet(silver)
except Exception:
    df = pd.read_csv(raw_csv)

# Pequeno resumo para terminal
print("Linhas:", len(df))
print("Colunas:", list(df.columns))
print("Anos:", df["year"].astype(int).min(), "→", df["year"].astype(int).max())
print("Países (top 5):", df["country_name"].dropna().unique()[:5])
