import findspark
findspark.init("/opt/spark")

import os
from pyspark.sql import SparkSession
from contextlib import redirect_stdout

# Tạo SparkSession
spark = SparkSession.builder.appName("Query Parquet Tables").getOrCreate()

# Đường dẫn dự án
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PARQUET_DIR = os.path.join(PROJECT_ROOT, "data_storage", "data_parquet")
OUTPUT_LOG = os.path.join(PROJECT_ROOT, "data_validation", "validation_step3_parquet_data_information.log")

# Đảm bảo thư mục data_validation tồn tại
os.makedirs(os.path.dirname(OUTPUT_LOG), exist_ok=True)

# Lấy danh sách file parquet
parquet_files = [f for f in os.listdir(DATA_PARQUET_DIR) if f.endswith(".parquet")]

# Mở file log và redirect stdout vào đó
with open(OUTPUT_LOG, "w", encoding="utf-8") as f:
    with redirect_stdout(f):
        if not parquet_files:
            print("❌ Không tìm thấy file .parquet nào.")
        else:
            for file in parquet_files:
                table_name = file.replace(".parquet", "")
                file_path = os.path.join(DATA_PARQUET_DIR, file)

                print("\n==============================")
                print(f"📦 Bảng: {table_name}")
                print(f"📁 File: {file_path}")

                # Đọc parquet
                df = spark.read.parquet(file_path)

                print("🔹 Schema:")
                df.printSchema()

                print("🔹 10 dòng đầu:")
                df.show(10, truncate=False)

                total = df.count()
                print(f"🔹 Số dòng: {total}")

print(f"\n✅ Đã ghi kết quả vào: {OUTPUT_LOG}")

# Dừng Spark
spark.stop()
