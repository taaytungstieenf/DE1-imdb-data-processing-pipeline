import os
import mysql.connector
import logging
from pathlib import Path

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '246357'),
    'database': os.getenv('DB_NAME', 'imdbDB'),
}

# Config logger
log_file_path = Path(__file__).resolve().parent.parent / 'data_validation' / 'validation_stepY.log'
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def get_table_schemas():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # List all tables in the database
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            logging.info(f"\nSchema for table: `{table}`")
            cursor.execute(f"DESCRIBE `{table}`;")
            rows = cursor.fetchall()
            header = f"{'Field':<20} {'Type':<20} {'Null':<10} {'Key':<10} {'Default':<15} {'Extra':<10}"
            logging.info(header)
            logging.info("-" * len(header))
            for row in rows:
                row_str = f"{row[0]:<20} {row[1]:<20} {row[2]:<10} {row[3]:<10} {str(row[4]):<15} {row[5]:<10}"
                logging.info(row_str)

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        logging.error("Connection error or Querying error: %s", err)

if __name__ == "__main__":
    get_table_schemas()
