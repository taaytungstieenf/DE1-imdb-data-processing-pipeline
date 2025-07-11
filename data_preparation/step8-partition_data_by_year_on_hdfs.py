import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Partition Movies by Year") \
    .getOrCreate()

# Đường dẫn HDFS đầu vào và đầu ra
INPUT_PARQUET_PATH = "hdfs://localhost:9000/user/hadoop/imdb_storage/fact_movie_full.parquet"
OUTPUT_PARTITIONED_PATH = "hdfs://localhost:9000/user/hadoop/partitioned_by_year"

# Đọc dữ liệu gốc
df = spark.read.parquet(INPUT_PARQUET_PATH)

# Ép kiểu startYear sang Integer (loại bỏ dòng lỗi nếu có)
df_clean = df.withColumn("startYear", col("startYear").cast(IntegerType())) \
             .filter(col("startYear").isNotNull())

# Ghi dữ liệu phân vùng theo startYear
df_clean.write \
    .mode("overwrite") \
    .partitionBy("startYear") \
    .parquet(OUTPUT_PARTITIONED_PATH)

print("✅ Đã phân vùng dữ liệu phim theo startYear và ghi ra HDFS.")

# Dừng Spark
spark.stop()
