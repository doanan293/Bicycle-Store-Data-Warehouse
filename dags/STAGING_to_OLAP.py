from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import os
import pandas as pd

# Set the Google Cloud credentials environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/imdatsing/airflow/dags/__pycache__/seventh-jet-424513-h5-a426c383f9d7.json"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,               # Each task instance runs independently of the previous instances
    'email_on_failure': False,              # Disable email notifications on task failure
    'email_on_retry': False,                # Disable email notifications on retries
    'retries': 0,                           # No retry after a task fails
}

# Define the DAG
dag = DAG(
    'STAGING_to_OLAP',
    default_args=default_args,
    schedule=timedelta(days=1),             # The DAG should run once per day
    start_date=datetime(2024, 5, 26),
    catchup=False,
)

# Function to read SQL files
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Function to execute a BigQuery file
def execute_query(file_path, **kwargs):
    client = bigquery.Client(location="asia-southeast1")
    sql_query = read_sql_file(file_path)
    query_job = client.query(sql_query)
    query_job.result()
    print(f"Query from {file_path} executed successfully.")

# Define tasks for each SQL file
t1 = PythonOperator(
    task_id='create_dim_currency_table',
    python_callable=execute_query,
    op_kwargs={'file_path': '/home/imdatsing/airflow/dags/__pycache__/BigQuery/Dim_Currency.sql'},
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_dim_customer_table',
    python_callable=execute_query,
    op_kwargs={'file_path': '/home/imdatsing/airflow/dags/__pycache__/BigQuery/Dim_Customer.sql'},
    dag=dag,
)

t3 = PythonOperator(
    task_id='create_dim_date_table',
    python_callable=execute_query,
    op_kwargs={'file_path': '/home/imdatsing/airflow/dags/__pycache__/BigQuery/Dim_Date.sql'},
    dag=dag,
)

t4 = PythonOperator(
    task_id='create_dim_delivery_address_table',
    python_callable=execute_query,
    op_kwargs={'file_path': '/home/imdatsing/airflow/dags/__pycache__/BigQuery/Dim_DeliveryAddress.sql'},
    dag=dag,
)

t5 = PythonOperator(
    task_id='create_dim_detail_order_table',
    python_callable=execute_query,
    op_kwargs={'file_path': '/home/imdatsing/airflow/dags/__pycache__/BigQuery/Dim_DetailOrder.sql'},
    dag=dag,
)

t6 = PythonOperator(
    task_id='create_dim_sales_reason_table',
    python_callable=execute_query,
    op_kwargs={'file_path': '/home/imdatsing/airflow/dags/__pycache__/BigQuery/Dim_SalesReason.sql'},
    dag=dag,
)

t7 = PythonOperator(
    task_id='create_fact_table',
    python_callable=execute_query,
    op_kwargs={'file_path': '/home/imdatsing/airflow/dags/__pycache__/BigQuery/Fact.sql'},
    dag=dag,
)

t8 = PythonOperator(
    task_id='create_fact_rfm_table',
    python_callable=execute_query,
    op_kwargs={'file_path': '/home/imdatsing/airflow/dags/__pycache__/BigQuery/Fact_RFM.sql'},
    dag=dag,
)

# Set task dependencies 
[t1, t2, t3, t4, t5, t6] >> t7 >> t8
