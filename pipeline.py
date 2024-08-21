import os
import gzip
import pandas as pd
from minio import Minio
from minio.error import S3Error
from datetime import datetime

MINIO_ENDPOINT = '127.0.0.1:9000'
MINIO_ACCESS_KEY = 'ZXnWAKImpIEOsbJAW2Uy'
MINIO_SECRET_KEY = 'dCqI1k3yu4U1lNDfx8kdO99lHsakl0t1f0q6202F'
MINIO_BUCKET_NAME = 'test'

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)
def convert_ndjson_to_csv(ndjson_file, csv_file):
    print(f"Converting {ndjson_file} to {csv_file}...")
    try:
        df = pd.read_json(ndjson_file, lines=True)
        df.to_csv(csv_file, index=False)
        print(f"Conversion to CSV completed and saved to {csv_file}.")
    except Exception as e:
        print(f"Conversion error: {e}")
def compress_csv_with_gzip(csv_file, gzipped_file):
    print(f"Compressing {csv_file} to {gzipped_file}...")
    try:
        with open(csv_file, 'rb') as f_in, gzip.open(gzipped_file, 'wb') as f_out:
            f_out.writelines(f_in)
        print(f"Compression completed and saved to {gzipped_file}.")
    except Exception as e:
        print(f"Compression error: {e}")
def upload_to_minio(gzipped_file, minio_path):
    print(f"Uploading {gzipped_file} to MinIO bucket {MINIO_BUCKET_NAME} as {minio_path}...")
    try:
        minio_client.fput_object(MINIO_BUCKET_NAME, minio_path, gzipped_file)
        print(f"Upload of {gzipped_file} to MinIO bucket {MINIO_BUCKET_NAME} as {minio_path} successful.")
    except S3Error as e:
        print(f"Upload error: {e}")
def run_pipeline():
    ndjson_files = [
        "0.ndjson",
        "1.ndjson",
        "2.ndjson",
        "3.ndjson",
        "4.ndjson"
    ]
    
    for ndjson_file in ndjson_files:
        csv_file = ndjson_file.replace('.ndjson', '.csv')
        gzipped_file = csv_file + '.gz'
        convert_ndjson_to_csv(ndjson_file, csv_file)
        compress_csv_with_gzip(csv_file, gzipped_file)
        upload_to_minio(gzipped_file, f'{datetime.now().strftime("%Y-%m-%d")}/{gzipped_file}')

if __name__ == '__main__':
    run_pipeline()
