import findspark
findspark.init("/opt/spark")

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Đường dẫn
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PARQUET_DIR = os.path.join(PROJECT_ROOT, "data_storage", "hub1-raw_data_for_manipulation", "parquet_data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data_storage", "hub2-new_dataset_for_HDFS")
SPARK_LOCAL_DIR = os.path.join(PROJECT_ROOT, ".spark_temp")  # Ghi dữ liệu tạm tại đây

# Tạo thư mục nếu chưa có
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SPARK_LOCAL_DIR, exist_ok=True)

# Khởi tạo SparkSession với spark.local.dir
spark = SparkSession.builder \
    .appName("Build Fact Movie Table") \
    .config("spark.local.dir", SPARK_LOCAL_DIR) \
    .getOrCreate()

# Load các bảng cần thiết
df_title = spark.read.parquet(os.path.join(DATA_PARQUET_DIR, "title.basics.parquet"))
df_rating = spark.read.parquet(os.path.join(DATA_PARQUET_DIR, "title.ratings.parquet"))
df_crew = spark.read.parquet(os.path.join(DATA_PARQUET_DIR, "title.crew.parquet"))
df_principals = spark.read.parquet(os.path.join(DATA_PARQUET_DIR, "title.principals.parquet"))
df_name = spark.read.parquet(os.path.join(DATA_PARQUET_DIR, "name.basics.parquet"))

# Join title + ratings
df = df_title.join(df_rating, on="tconst", how="left")

# Join crew
df = df.join(df_crew, on="tconst", how="left")

# Join principal actor info
df = df.join(df_principals.select("tconst", "nconst", "category"), on="tconst", how="left")
df = df.join(df_name.select("nconst", "primaryName", "primaryProfession"), on="nconst", how="left")

# Chọn cột cần thiết
df_final = df.select(
    "tconst", "titleType", "primaryTitle", "originalTitle",
    "startYear", "endYear", "runtimeMinutes", "genres",
    "averageRating", "numVotes",
    "directors", "writers",
    "category", "primaryName", "primaryProfession"
)

# Ghi ra parquet
df_final.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "fact_movie_full.parquet"))

print("✅ Đã tạo bảng dữ liệu gộp: fact_movie_full.parquet")

# Tắt Spark
spark.stop()
