"""
Airflow DAG to ingest NYC Taxi data (yellow and green) for 2019-2020 into BigQuery.
Processes 48 files total: 2 taxi types × 2 years × 12 months
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.task_group import TaskGroup

# Configuration - Update these values
GCP_PROJECT_ID = "zoomcamp-data-engineer-484608"
GCP_BUCKET_NAME = "eduardo-zoomcamp-bucket"
GCP_DATASET = "zoomcamp"
GCP_LOCATION = "US"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Yellow taxi schema
YELLOW_TAXI_SCHEMA = [
    {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "tpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
]

# Green taxi schema
GREEN_TAXI_SCHEMA = [
    {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ehail_fee", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trip_type", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
]


def create_file_processing_tasks(dag, taxi_type, year, month):
    """Create a task group for processing one taxi data file."""
    
    file_id = f"{taxi_type}_{year}_{month}"
    csv_file = f"/tmp/{file_id}.csv"
    gcs_path = f"taxi_data/{taxi_type}/{year}/{month}.csv"
    
    with TaskGroup(group_id=f"process_{file_id}") as group:
        
        # Step 1: Download and extract CSV file
        download_task = BashOperator(
            task_id='download_extract',
            bash_command=f"""
            wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv.gz | \
            gunzip > {csv_file}
            """,
        )
        
        # Step 2: Upload to GCS
        upload_task = LocalFilesystemToGCSOperator(
            task_id='upload_to_gcs',
            src=csv_file,
            dst=gcs_path,
            bucket=GCP_BUCKET_NAME,
            gcp_conn_id='google_cloud_default',
        )
        
        # Step 3: Create external table
        external_table_name = f"{taxi_type}_taxi_external"
        create_external_table = BigQueryInsertJobOperator(
            task_id='create_external_table',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{GCP_DATASET}.{external_table_name}`
                    OPTIONS (
                      format = 'CSV',
                      uris = ['gs://{GCP_BUCKET_NAME}/taxi_data/{taxi_type}/*'],
                      skip_leading_rows = 1
                    );
                    """,
                    "useLegacySql": False,
                }
            },
        )
        
        # Step 4: Create partitioned table (one-time, but safe to run multiple times)
        schema = YELLOW_TAXI_SCHEMA if taxi_type == "yellow" else GREEN_TAXI_SCHEMA
        pickup_field = "tpep_pickup_datetime" if taxi_type == "yellow" else "lpep_pickup_datetime"
        partitioned_table_name = f"{taxi_type}_taxi_partitioned"
        
        create_partitioned_table = BigQueryCreateEmptyTableOperator(
            task_id='create_partitioned_table',
            gcp_conn_id='google_cloud_default',
            project_id=GCP_PROJECT_ID,
            dataset_id=GCP_DATASET,
            table_id=partitioned_table_name,
            schema_fields=schema,
            time_partitioning={
                "type": "DAY",
                "field": pickup_field,
            },
            exists_ok=True,
        )
        
        # Step 5: Load data from external table to partitioned table
        load_data = BigQueryInsertJobOperator(
            task_id='load_to_partitioned',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": f"""
                    INSERT INTO `{GCP_PROJECT_ID}.{GCP_DATASET}.{partitioned_table_name}`
                    SELECT *
                    FROM `{GCP_PROJECT_ID}.{GCP_DATASET}.{external_table_name}`
                    WHERE {pickup_field} BETWEEN '{year}-{month}-01' AND '{year}-{month}-31'
                    """,
                    "useLegacySql": False,
                }
            },
        )
        
        # Step 6: Cleanup local file
        cleanup_task = BashOperator(
            task_id='cleanup',
            bash_command=f"rm -f {csv_file}",
        )
        
        # Set dependencies
        download_task >> upload_task >> create_external_table >> create_partitioned_table >> load_data >> cleanup_task
    
    return group


# Create the DAG
with DAG(
    'taxi_data_ingest',
    default_args=default_args,
    description='Ingest NYC Taxi data (yellow and green) for 2019-2020',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['taxi', 'bigquery', 'data-engineering'],
) as dag:
    
    # Generate all combinations of taxi type, year, and month
    taxi_types = ['yellow', 'green']
    years = ['2019', '2020']
    months = [f"{i:02d}" for i in range(1, 13)]
    
    # Create tasks for each combination
    all_tasks = []
    for taxi_type in taxi_types:
        for year in years:
            for month in months:
                task_group = create_file_processing_tasks(dag, taxi_type, year, month)
                all_tasks.append(task_group)
    
    # All tasks run in parallel (no dependencies between them)
