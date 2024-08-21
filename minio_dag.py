from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipeline import run_pipeline  

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 24),
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='A DAG to run data pipeline daily at 07:00 AM Vietnam time',
    schedule='0 7 * * *',  
)

run_pipeline_task = PythonOperator(
    task_id='run_pipeline_task',
    python_callable=run_pipeline,  
    dag=dag,
)
