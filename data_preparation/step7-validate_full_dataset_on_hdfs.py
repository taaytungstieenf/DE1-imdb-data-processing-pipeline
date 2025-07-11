import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Validate HDFS Parquet Dataset").getOrCreate()

HDFS_TARGET_DIR = "hdfs://localhost:9000/user/hadoop/imdb_storage/fact_movie_full.parquet"

current_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(current_dir, "../data_validation/validation_step7.log")

df = spark.read.parquet(HDFS_TARGET_DIR)

row_count = df.count()
schema_str = df._jdf.schema().treeString()

with open(log_path, "w") as f:
    f.write(f"Validation of dataset on HDFS path:\n{HDFS_TARGET_DIR}\n\n")
    f.write(f"Total rows: {row_count}\n\n")
    f.write("Schema:\n")
    f.write(schema_str)

print(f"Validation completed. Check log file here: {log_path}")

spark.stop()
