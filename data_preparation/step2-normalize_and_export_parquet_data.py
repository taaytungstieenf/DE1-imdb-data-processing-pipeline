import findspark
findspark.init("/opt/spark")

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Normalizing TSV Data").getOrCreate()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_SRC_DIR = os.path.join(PROJECT_ROOT, "data_storage", "hub1-raw_data_for_manipulation", "tsv_data")
DATA_OUT_DIR = os.path.join(PROJECT_ROOT, "data_storage", "hub1-raw_data_for_manipulation", "parquet_data")

os.makedirs(DATA_OUT_DIR, exist_ok=True)

tsv_files = [f for f in os.listdir(DATA_SRC_DIR) if f.endswith(".tsv")]

for filename in tsv_files:
    print(f"📂 Đang xử lý: {filename}")

    input_path = os.path.join(DATA_SRC_DIR, filename)
    base_name = filename.replace(".tsv", "")
    output_path = os.path.join(DATA_OUT_DIR, base_name + ".parquet")

    # Đọc TSV
    df = spark.read.option("header", True).option("sep", "\t").csv(input_path)

    # Thay thế '\N' bằng NULL
    for col_name in df.columns:
        df = df.withColumn(col_name, when(col(col_name) == "\\N", None).otherwise(col(col_name)))

    # Ghi ra định dạng Parquet
    df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Đã lưu tại: {output_path}")

spark.stop()
