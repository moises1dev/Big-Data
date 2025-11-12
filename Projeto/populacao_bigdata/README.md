# Projeto: Análise da População Mundial com Big Data

Este repositório contém um pipeline inicial (Python + PySpark) para coletar, processar e analisar dados populacionais.

## Estrutura
```
populacao_bigdata/
  data/
    raw/      # dados brutos
    bronze/   # padronizado
    silver/   # limpo e tipado
    gold/     # agregados
  src/
    ingest/   # scripts de coleta
    pipelines/# jobs PySpark
    utils/    # helpers
  notebooks/  # EDA
  dashboards/ # BI
  configs/    # variáveis e schemas
  logs/       # logs de execução
```


## Executando tudo só com Python (PySpark embutido)
> Se você instalou `pyspark` via `pip`, pode rodar sem `spark-submit`:

```bash
# 0) checar ambiente (Python/Java/PySpark)
python scripts/check_env.py

# 1) ingestão (API → CSV e tenta Parquet)
python scripts/run_ingest.py

# 2) pipeline PySpark (bronze/raw → silver → gold)
python scripts/run_pipeline.py

# 3) EDA rápida no terminal
python scripts/eda_preview.py
```
