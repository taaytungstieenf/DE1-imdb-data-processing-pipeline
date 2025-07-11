import os
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from sqlalchemy import create_engine

# Cấu hình kết nối MySQL từ biến môi trường hoặc mặc định
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '246357'),
    'database': os.getenv('DB_NAME', 'imdbDB'),
}


def read_all_parquet_files(base_dir):
    all_dfs = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)

                # Trích xuất partition: startYear=XXXX
                try:
                    partition_folder = Path(file_path).parent.name
                    if partition_folder.startswith("startYear="):
                        start_year = partition_folder.split("=")[1]
                    else:
                        start_year = None
                except Exception:
                    start_year = None

                try:
                    table = pq.read_table(file_path)
                    df = table.to_pandas()

                    # Nếu thiếu startYear thì thêm vào
                    if "startYear" not in df.columns and start_year is not None:
                        df["startYear"] = start_year

                    all_dfs.append(df)
                    print(f"✅ Loaded: {file_path} (startYear={start_year})")
                except Exception as e:
                    print(f"❌ Failed to load {file_path}: {e}")

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame()


def load_to_mysql(df, table_name="fact_movies"):
    if df.empty:
        print("⚠️ No data to load.")
        return

    # Tạo connection string cho SQLAlchemy
    user = DB_CONFIG['user']
    password = DB_CONFIG['password']
    host = DB_CONFIG['host']
    database = DB_CONFIG['database']
    conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"

    engine = create_engine(conn_str)

    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"✅ Successfully loaded {len(df)} rows into '{table_name}' table in MySQL.")
    except Exception as e:
        print(f"❌ Error loading data to MySQL: {e}")


if __name__ == "__main__":
    parquet_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../data_storage/data_for_warehouse")
    )

    print(f"📂 Reading parquet files from: {parquet_dir}")
    df = read_all_parquet_files(parquet_dir)
    print(f"📊 Total rows loaded into memory: {len(df)}")

    load_to_mysql(df)
