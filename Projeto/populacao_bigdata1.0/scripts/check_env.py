#!/usr/bin/env python

import subprocess, sys, shutil

def cmd_out(cmd):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return (p.stdout or p.stderr).strip()
    except Exception as e:
        return f"ERR: {e}"

print("=== Python ===")
print(sys.version)

print("\n=== Java ===")
print(cmd_out(["java", "-version"]))

print("\n=== PySpark ===")
try:
    import pyspark
    from pyspark.sql import SparkSession
    print("pyspark:", pyspark.__version__)
    spark = (SparkSession.builder.appName("env_check").getOrCreate())
    print("SparkSession OK. Version:", spark.version)
    spark.stop()
except Exception as e:
    print("PySpark import/usage failed:", e)

