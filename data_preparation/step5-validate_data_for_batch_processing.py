import findspark
findspark.init("/opt/spark")

import os
from pyspark.sql import SparkSession
from contextlib import redirect_stdout

# Khởi tạo Spark
spark = SparkSession.builder.appName("Validate Fact Movie Data").getOrCreate()

# Xác định đường dẫn
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FACT_DATA_PATH = os.path.join(PROJECT_ROOT, "data_validation", "data_for_batch", "fact_movie_full.parquet")
LOG_PATH = os.path.join(PROJECT_ROOT, "data_validation", "validation_of_step5_fact_movie_full_dataset.log")

# Đảm bảo thư mục data_validation tồn tại
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# Đọc dữ liệu
df = spark.read.parquet(FACT_DATA_PATH)

# Redirect stdout để ghi log
with open(LOG_PATH, "w", encoding="utf-8") as f:
    with redirect_stdout(f):
        print("=== ✅ VALIDATION REPORT: fact_movie_full.parquet ===\n")

        # Schema
        print("🔹 Schema:")
        df.printSchema()

        # Tổng số dòng
        print("\n🔹 Tổng số dòng:")
        print(df.count())

        # Nulls theo cột
        print("\n🔹 Số lượng giá trị NULL theo cột:")
        for col_name in df.columns:
            count_null = df.filter(df[col_name].isNull()).count()
            print(f"   {col_name}: {count_null} nulls")

        # Phân phối theo titleType
        print("\n🔹 Phân phối theo titleType:")
        df.groupBy("titleType").count().orderBy("count", ascending=False).show(truncate=False)

        # Phân phối theo startYear
        print("\n🔹 Phân phối theo startYear:")
        df.groupBy("startYear").count().orderBy("startYear").show(20, truncate=False)

        # Phân phối theo averageRating
        print("\n🔹 Phân phối theo averageRating:")
        df.groupBy("averageRating").count().orderBy("averageRating").show(20, truncate=False)

        print(f"\n✅ Đã ghi báo cáo vào: {LOG_PATH}")

spark.stop()
