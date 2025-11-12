"""Exemplo de ingestão via API (World Bank) para população total por país.
Salva CSV em data/raw e Parquet em data/bronze (requer pyarrow instalado).
"""
import os
import requests
from typing import List
from dotenv import load_dotenv
import pandas as pd
from src.utils.logger import get_logger

log = get_logger("api_ingest")

def fetch_worldbank_population(country_codes: List[str], per_page: int = 20000) -> pd.DataFrame:
    base = os.getenv("WORLD_BANK_BASE", "https://api.worldbank.org/v2")
    indicator = "SP.POP.TOTL"
    url = f"{base}/country/{';'.join(country_codes)}/indicator/{indicator}?format=json&per_page={per_page}"
    log.info(f"GET {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    rows = data[1] if isinstance(data, list) and len(data) > 1 else []
    df = pd.json_normalize(rows)
    df = df.rename(columns={
        'countryiso3code':'country_code',
        'country.value':'country_name',
        'date':'year',
        'value':'population'
    })[['country_code','country_name','year','population']]
    df['year'] = df['year'].astype(int)
    return df

def main():
    load_dotenv(dotenv_path=os.path.join('configs','.env'), override=True)
    raw_dir = os.path.join('data','raw')
    bronze_dir = os.path.join('data','bronze')
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(bronze_dir, exist_ok=True)

    country_list = os.getenv("COUNTRY_LIST", "BR;US").split(';')
    df = fetch_worldbank_population(country_list)
    csv_path = os.path.join(raw_dir, 'worldbank_population.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8')
    log.info(f"Salvo raw CSV: {csv_path} (linhas={len(df)})")

    try:
        parquet_path = os.path.join(bronze_dir, 'worldbank_population.parquet')
        df.to_parquet(parquet_path, index=False)  # requer pyarrow/fastparquet
        log.info(f"Salvo bronze Parquet: {parquet_path}")
    except Exception as e:
        log.warning(f"Não foi possível salvar Parquet (instale pyarrow): {e}")

if __name__ == "__main__":
    main()
