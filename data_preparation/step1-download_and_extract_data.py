import os
import urllib.request
import gzip
import shutil

IMDB_URLS = [
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    "https://datasets.imdbws.com/title.akas.tsv.gz",
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.crew.tsv.gz",
    "https://datasets.imdbws.com/title.episode.tsv.gz",
    "https://datasets.imdbws.com/title.principals.tsv.gz",
    "https://datasets.imdbws.com/title.ratings.tsv.gz"
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_RAW_DIR = os.path.join(BASE_DIR, '..', 'data_storage', 'hub1-raw_data_for_manipulation')
TSV_DIR = os.path.join(DATA_RAW_DIR, 'tsv_data')

os.makedirs(DATA_RAW_DIR, exist_ok=True)
os.makedirs(TSV_DIR, exist_ok=True)

def download_and_extract(url):
    filename_gz = os.path.join(DATA_RAW_DIR, os.path.basename(url))
    filename_tsv = os.path.splitext(os.path.basename(url))[0]  # Bỏ .gz
    tsv_path = os.path.join(TSV_DIR, filename_tsv)

    if not os.path.exists(filename_gz):
        print(f"Downloading: {url}")
        urllib.request.urlretrieve(url, filename_gz)
    else:
        print(f"Already downloaded: {filename_gz}")

    if not os.path.exists(tsv_path):
        print(f"Extracting to: {tsv_path}")
        with gzip.open(filename_gz, 'rb') as f_in:
            with open(tsv_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        print(f"Already extracted: {tsv_path}")

if __name__ == "__main__":
    for url in IMDB_URLS:
        download_and_extract(url)
    print("All files downloaded and extracted.")
