import os
import subprocess
import json
from pathlib import Path

HDFS_CMD = "/opt/hadoop/bin/hdfs"
HDFS_PARTITION_PATH = "/user/hadoop/partitioned_by_year"
LOCAL_TARGET_DIR = "../data_storage/data_for_warehouse"
STATE_FILE = "../data_storage/data_for_warehouse/downloaded_partitions.json"

def load_downloaded_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_downloaded_state(downloaded_years):
    with open(STATE_FILE, "w") as f:
        json.dump(sorted(downloaded_years), f)

def list_available_hdfs_partitions():
    result = subprocess.run([HDFS_CMD, "dfs", "-ls", HDFS_PARTITION_PATH],
                            capture_output=True, text=True)
    lines = result.stdout.strip().split("\n")
    partitions = []
    for line in lines:
        if "startYear=" in line:
            path = line.split()[-1]
            year = path.split("startYear=")[-1]
            partitions.append((int(year), path))
    return sorted(partitions)

def download_partition(year, hdfs_partition_path):
    local_dir = os.path.join(LOCAL_TARGET_DIR, f"startYear={year}")
    os.makedirs(local_dir, exist_ok=True)

    print(f"Downloading startYear={year} from {hdfs_partition_path} -> {local_dir}")

    # Liệt kê các file .parquet trong thư mục HDFS partition
    result = subprocess.run([HDFS_CMD, "dfs", "-ls", hdfs_partition_path],
                            capture_output=True, text=True)
    lines = result.stdout.strip().split("\n")

    for line in lines:
        if ".parquet" in line:
            hdfs_file_path = line.split()[-1]
            subprocess.run([HDFS_CMD, "dfs", "-copyToLocal", hdfs_file_path, local_dir])

def main():
    downloaded_years = load_downloaded_state()
    partitions = list_available_hdfs_partitions()

    for year, hdfs_path in partitions:
        if str(year) not in downloaded_years:
            download_partition(year, hdfs_path)
            downloaded_years.add(str(year))
            save_downloaded_state(downloaded_years)
            break  # chỉ tải 1 năm mỗi lần chạy

if __name__ == "__main__":
    main()
