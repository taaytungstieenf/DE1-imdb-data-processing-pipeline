import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession
import os

# --- Khởi tạo SparkSession ---
spark = SparkSession.builder \
    .appName("Check HDFS Partitions") \
    .getOrCreate()

# --- Đường dẫn HDFS đến dữ liệu phân vùng ---
hdfs_path = "hdfs://localhost:9000/user/hadoop/partitioned_by_year"

# --- Đọc dữ liệu từ HDFS ---
df = spark.read.option("basePath", hdfs_path).parquet(hdfs_path)

# --- Tạo log file ---
log_file = "../data_validation/validation_of_step9_partition_check.log"
if os.path.exists(log_file):
    os.remove(log_file)

def write_log(msg):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# --- 1. In schema ---
write_log(">> Schema:")
write_log(df.printSchema().__str__())

# --- 2. Đếm tổng số dòng ---
total_rows = df.count()
write_log(f"\n>> Tổng số dòng: {total_rows}")

# --- 3. Lấy 5 dòng đầu ---
write_log("\n>> 5 dòng đầu tiên:")
df.limit(5).toPandas().to_string(buf=open(log_file, "a", encoding="utf-8"))

# --- 4. Số lượng dòng theo từng startYear ---
write_log("\n>> Số lượng dòng theo từng startYear:")
year_counts = df.groupBy("startYear").count().orderBy("startYear")
for row in year_counts.collect():
    write_log(f"{row['startYear']}: {row['count']}")

# --- 5. Kiểm tra giá trị NULL của một số cột chính ---
columns_to_check = ["primaryTitle", "genres", "averageRating"]
write_log("\n>> Kiểm tra giá trị NULL:")
for col in columns_to_check:
    null_count = df.filter(df[col].isNull() | (df[col] == "")).count()
    write_log(f"{col}: {null_count} NULL/empty")

# --- Dừng Spark ---
spark.stop()
