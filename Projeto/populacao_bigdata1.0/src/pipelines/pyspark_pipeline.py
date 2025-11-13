"""Template de pipeline PySpark (bronze -> silver -> gold)
- Lê Parquet bronze/worldbank_population.parquet (ou CSV raw como fallback)
- Limpa e cria silver/population_clean.parquet
- Agrega e salva gold/population_by_country_year.parquet
"""
import os
from pyspark.sql import SparkSession, functions as F

def build_spark(app_name: str = "populacao_pipeline") -> SparkSession:
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def run():
    spark = build_spark()
    bronze_parquet = os.path.join("data","bronze","worldbank_population.parquet")
    raw_csv = os.path.join("data","raw","worldbank_population.csv")
    silver_path = os.path.join("data","silver","population_clean.parquet")
    gold_path = os.path.join("data","gold","population_by_country_year.parquet")

    if os.path.exists(bronze_parquet):
        df = spark.read.parquet(bronze_parquet)
    else:
        df = spark.read.option("header", True).csv(raw_csv)

    df_clean = (df
        .withColumn("population", F.col("population").cast("long"))
        .filter(F.col("population").isNotNull())
        .withColumn("year", F.col("year").cast("int"))
        .dropDuplicates(["country_code","year"]))

    df_clean.write.mode("overwrite").parquet(silver_path)
    gold = (df_clean.groupBy("country_code","country_name","year")
        .agg(F.sum("population").alias("population_total"))
        .orderBy("country_code","year"))
    gold.write.mode("overwrite").parquet(gold_path)

    print("Pipeline concluído.")
    spark.stop()

if __name__ == "__main__":
    run()
