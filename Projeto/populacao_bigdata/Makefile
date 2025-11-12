# Comandos úteis
setup:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

ingest:
	python src/ingest/api_ingest.py

pipeline:
	spark-submit src/pipelines/pyspark_pipeline.py

eda:
	jupyter lab notebooks/EDA_populacao.ipynb
