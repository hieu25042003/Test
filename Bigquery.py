from google.cloud import bigquery
from minio import Minio
import os

def create_table_if_not_exists(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        table = client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception as e:
        print(f"Table {table_id} does not exist. Creating table...")
        schema = [
            bigquery.SchemaField("field1", "STRING"),
            bigquery.SchemaField("field2", "INTEGER"),
            bigquery.SchemaField("field3", "FLOAT")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table_id}.")

def download_from_minio(bucket_name, object_name, file_path):
    minio_client = Minio(
        "127.0.0.1:9000",
        access_key="ZXnWAKImpIEOsbJAW2Uy",
        secret_key="dCqI1k3yu4U1lNDfx8kdO99lHsakl0t1f0q6202F",
        secure=False
    )
    minio_client.fget_object(bucket_name, object_name, file_path)
    print(f"Downloaded {object_name} from MinIO to {file_path}.")

def load_data_to_bigquery(project_id, dataset_id, table_id, file_path):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("field1", "STRING"),
            bigquery.SchemaField("field2", "INTEGER"),
            bigquery.SchemaField("field3", "FLOAT")
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )
    
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
    job.result()  
    print(f"Loaded data from {file_path} into {table_id}.")

def main():
    project_id = "hieu"
    dataset_id = "can"
    table_id = "1"
    bucket_name = "test"
    object_name = "data_0.ndjson"
    file_path = "\home\canhieu\airflow\dags"

    create_table_if_not_exists(project_id, dataset_id, table_id)
    download_from_minio(bucket_name, object_name, file_path)
    load_data_to_bigquery(project_id, dataset_id, table_id, file_path)

if __name__ == "__main__":
    main()
