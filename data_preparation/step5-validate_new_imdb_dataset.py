import findspark
findspark.init("/opt/spark")

import os
from pyspark.sql import SparkSession
from contextlib import redirect_stdout

spark = SparkSession.builder.appName("Validate Fact Movie Data").getOrCreate()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FACT_DATA_PATH = os.path.join(PROJECT_ROOT, "data_storage", "hub2-new_dataset_for_HDFS", "fact_movie_full.parquet")
LOG_PATH = os.path.join(PROJECT_ROOT, "data_validation", "validation_step5.log")

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

df = spark.read.parquet(FACT_DATA_PATH)

# Redirect stdout to write the log file
with open(LOG_PATH, "w", encoding="utf-8") as f:
    with redirect_stdout(f):
        print("=== VALIDATION REPORT: fact_movie_full.parquet ===\n")

        print("Schema:")
        df.printSchema()

        print("\nNumber of records:")
        print(df.count())

        print("\nNULL values in each field:")
        for col_name in df.columns:
            count_null = df.filter(df[col_name].isNull()).count()
            print(f"   {col_name}: {count_null} nulls")

        print("\nDistribution by titleType:")
        df.groupBy("titleType").count().orderBy("count", ascending=False).show(truncate=False)

        # Distribution by startYear
        print("\nDistribution by startYear:")
        df.groupBy("startYear").count().orderBy("startYear").show(20, truncate=False)

        # Distribution by averageRating
        print("\nDistribution by averageRating:")
        df.groupBy("averageRating").count().orderBy("averageRating").show(20, truncate=False)

        print(f"\nReport was saved successfully at: {LOG_PATH}")

spark.stop()
