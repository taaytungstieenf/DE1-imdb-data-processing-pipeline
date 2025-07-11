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

write_log(">> Schema:")
write_log(df.printSchema().__str__())

total_rows = df.count()
write_log(f"\nNumber of records: {total_rows}")

write_log("\nFirst 5 records:")
df.limit(5).toPandas().to_string(buf=open(log_file, "a", encoding="utf-8"))

write_log("\nNumber of records by startYear:")
year_counts = df.groupBy("startYear").count().orderBy("startYear")
for row in year_counts.collect():
    write_log(f"{row['startYear']}: {row['count']}")

columns_to_check = ["primaryTitle", "genres", "averageRating"]
write_log("\nCheck NULL values:")
for col in columns_to_check:
    null_count = df.filter(df[col].isNull() | (df[col] == "")).count()
    write_log(f"{col}: {null_count} NULL/empty")

spark.stop()
