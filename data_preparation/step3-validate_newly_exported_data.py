import findspark
findspark.init("/opt/spark")

import os
from pyspark.sql import SparkSession
from contextlib import redirect_stdout

spark = SparkSession.builder.appName("Query Parquet Tables").getOrCreate()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PARQUET_DIR = os.path.join(PROJECT_ROOT, "data_storage", "hub1-raw_data_for_manipulation", "parquet_data")
OUTPUT_LOG = os.path.join(PROJECT_ROOT, "data_validation", "validation_step3.log")

os.makedirs(os.path.dirname(OUTPUT_LOG), exist_ok=True)

parquet_files = [f for f in os.listdir(DATA_PARQUET_DIR) if f.endswith(".parquet")]

# Open the log file and redirect stdout into
with open(OUTPUT_LOG, "w", encoding="utf-8") as f:
    with redirect_stdout(f):
        if not parquet_files:
            print("Can not find any .parquet files in the data_parquet directory. Please check again...")
        else:
            for file in parquet_files:
                table_name = file.replace(".parquet", "")
                file_path = os.path.join(DATA_PARQUET_DIR, file)

                print("\n==============================")
                print(f"Table: {table_name}")
                print(f"File: {file_path}")

                # Đọc parquet
                df = spark.read.parquet(file_path)

                print("Schema:")
                df.printSchema()

                print("First 10 records:")
                df.show(10, truncate=False)

                total = df.count()
                print(f"Number of records: {total}")

print(f"\nAlready wrote result in: {OUTPUT_LOG}")

spark.stop()
