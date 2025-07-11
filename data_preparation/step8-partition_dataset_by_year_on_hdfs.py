import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("Partition Movies by Year").getOrCreate()

INPUT_PARQUET_PATH = "hdfs://localhost:9000/user/hadoop/imdb_storage/fact_movie_full.parquet"
OUTPUT_PARTITIONED_PATH = "hdfs://localhost:9000/user/hadoop/partitioned_by_year"

df = spark.read.parquet(INPUT_PARQUET_PATH)

# Cast startYear to Integer (remove error line if any)
df_clean = df.withColumn("startYear", col("startYear").cast(IntegerType())).filter(col("startYear").isNotNull())

# Write data into partitions by startYear
df_clean.write.mode("overwrite").partitionBy("startYear").parquet(OUTPUT_PARTITIONED_PATH)

print("Partitioned movie data by startYear and wrote to HDFS.")

spark.stop()
