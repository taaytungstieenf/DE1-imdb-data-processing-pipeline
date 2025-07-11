import os
import subprocess

# Thêm đường dẫn HDFS vào PATH (chắc chắn bạn đã cài Hadoop ở /opt/hadoop)
env = os.environ.copy()
env["PATH"] += ":/opt/hadoop/bin"

LOCAL_PARQUET_DIR = "../data_storage/data_for_batch"
HDFS_TARGET_DIR = "/user/hadoop/imdb_storage"

print(f"📁 Đảm bảo thư mục gốc HDFS tồn tại: {HDFS_TARGET_DIR}")
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_TARGET_DIR], env=env)

for folder_name in os.listdir(LOCAL_PARQUET_DIR):
    if folder_name.endswith(".parquet"):
        local_path = os.path.join(LOCAL_PARQUET_DIR, folder_name)
        hdfs_path = os.path.join(HDFS_TARGET_DIR, folder_name)

        print(f"\n🚀 Đang đẩy {folder_name} lên HDFS → {hdfs_path}")
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path], env=env)
        subprocess.run(["hdfs", "dfs", "-put", local_path, hdfs_path], env=env)

        print(f"✅ Đã xong: {folder_name}")

print("\n🎉 Hoàn tất đẩy tất cả dữ liệu lên HDFS.")
