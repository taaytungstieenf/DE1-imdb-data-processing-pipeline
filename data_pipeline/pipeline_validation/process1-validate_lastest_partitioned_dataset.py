import os
import json
import findspark
from datetime import datetime
from io import StringIO
import sys

findspark.init("/opt/spark")
from pyspark.sql import SparkSession

STATE_FILE = "../../data_storage/hub3-partitioned_datasets_for_warehouse/downloaded_partitions.json"
LOCAL_DATA_DIR = "../../data_storage/hub3-partitioned_datasets_for_warehouse"
LOG_FILE_PATH = "../../data_validation/validation_process1.log"

def get_latest_downloaded_year():
    if not os.path.exists(STATE_FILE):
        raise FileNotFoundError(f"State file not found: {STATE_FILE}")
    with open(STATE_FILE, "r") as f:
        years = json.load(f)
    if not years:
        raise ValueError("No downloaded partitions found in state file.")
    return max(int(y) for y in years)

def validate_partition(year, log_buffer):
    partition_path = os.path.join(LOCAL_DATA_DIR, f"startYear={year}")
    if not os.path.exists(partition_path):
        raise FileNotFoundError(f"Partition folder not found: {partition_path}")

    spark = SparkSession.builder.appName(f"Validate IMDB Partition: startYear={year}").getOrCreate()

    log_buffer.write(f"\nReading data from: {partition_path}\n")
    df = spark.read.option("basePath", LOCAL_DATA_DIR).parquet(partition_path)

    log_buffer.write("\nSchema of the loaded partition:\n")
    df.printSchema()

    log_buffer.write("\nSample records:\n")
    df.show(5, truncate=False)

    spark.stop()

def main():
    # Prepare a buffer to write log
    log_buffer = StringIO()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_buffer.write(f"\n\n================ VALIDATION RUN: {timestamp} ================\n")

    # Temporarily switch stdout to log buffer
    original_stdout = sys.stdout
    sys.stdout = log_buffer

    try:
        year = get_latest_downloaded_year()
        log_buffer.write(f"\n✅ Validating latest downloaded partition: startYear={year}\n")
        validate_partition(year, log_buffer)
    except Exception as e:
        log_buffer.write(f"\n❌ Error during validation: {e}\n")
    finally:
        # Return the original stdout
        sys.stdout = original_stdout

        # Write buffer to a log file
        os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
        with open(LOG_FILE_PATH, "a") as log_file:
            log_file.write(log_buffer.getvalue())
        log_buffer.close()

if __name__ == "__main__":
    main()
