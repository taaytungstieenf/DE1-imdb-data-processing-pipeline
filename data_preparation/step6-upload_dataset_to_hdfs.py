import os
import subprocess

env = os.environ.copy()
env["PATH"] += ":/opt/hadoop/bin"

LOCAL_PARQUET_DIR = "../data_storage/hub2-new_dataset_for_HDFS"
HDFS_TARGET_DIR = "/user/hadoop/imdb_storage"

print(f"Make sure root directory HDFS exists: {HDFS_TARGET_DIR}")
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_TARGET_DIR], env=env)

for folder_name in os.listdir(LOCAL_PARQUET_DIR):
    if folder_name.endswith(".parquet"):
        local_path = os.path.join(LOCAL_PARQUET_DIR, folder_name)
        hdfs_path = os.path.join(HDFS_TARGET_DIR, folder_name)

        print(f"\nPushing {folder_name} on HDFS → {hdfs_path}")
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path], env=env)
        subprocess.run(["hdfs", "dfs", "-put", local_path, hdfs_path], env=env)

        print(f"Done: {folder_name}")

print("\nPushing new dataset to HDFS is complete. You can now check it on HDFS.")
