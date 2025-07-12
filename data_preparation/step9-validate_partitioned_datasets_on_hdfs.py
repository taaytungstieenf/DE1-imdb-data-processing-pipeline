import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Check HDFS Partitions").getOrCreate()

hdfs_path = "hdfs://localhost:9000/user/hadoop/partitioned_by_year"

df = spark.read.option("basePath", hdfs_path).parquet(hdfs_path)

log_file = "../data_validation/validation_step9.log"
if os.path.exists(log_file):
    os.remove(log_file)

def write_log(msg):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

schema_string = df._jdf.schema().treeString()
write_log("Schema:")
write_log(schema_string)

total_rows = df.count()
write_log(f"\nNumber of records: {total_rows}")

spark.stop()
