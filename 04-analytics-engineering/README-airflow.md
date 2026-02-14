# NYC Taxi Data Ingestion with Airflow

This DAG ingests 48 CSV files (yellow and green taxi data for 2019-2020) into BigQuery.

## Setup

### 1. Update Configuration

Edit `taxi_data_ingest_dag.py` and update these variables (lines 18-21):

```python
GCP_PROJECT_ID = "your-project-id"
GCP_BUCKET_NAME = "your-bucket-name"
GCP_DATASET = "taxi_data"
GCP_LOCATION = "US"
```

### 2. Configure GCP Connection

In Airflow UI, go to Admin → Connections and create/update the `google_cloud_default` connection:
- **Connection Type**: Google Cloud
- **Project ID**: Your GCP project ID
- **Keyfile Path**: Path to your service account JSON key file
  OR
- **Keyfile JSON**: Paste your service account JSON key content

Alternatively, set the environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
```

### 3. Install Dependencies

```bash
pip install -r requirements-airflow.txt
```

### 4. Copy DAG to Airflow

Copy the DAG file to your Airflow DAGs folder:
```bash
cp taxi_data_ingest_dag.py ~/airflow/dags/
```

Or update your `airflow.cfg` to point to this directory:
```ini
dags_folder = /workspaces/data-engineering-zoomcamp/04-analytics-engineering
```

### 5. Start Airflow (if not running)

```bash
# Initialize the database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start webserver (in one terminal)
airflow webserver --port 8080

# Start scheduler (in another terminal)
airflow scheduler
```

### 6. Run the DAG

1. Open Airflow UI: http://localhost:8080
2. Find the `taxi_data_ingest` DAG
3. Toggle it to ON
4. Click "Trigger DAG" to start the ingestion

## What This DAG Does

For each of the 48 files:
1. **Download**: Downloads CSV file from GitHub and extracts it
2. **Upload**: Uploads to GCS bucket
3. **External Table**: Creates/updates BigQuery external table pointing to GCS
4. **Partitioned Table**: Creates partitioned table (one per taxi type)
5. **Load Data**: Merges data into partitioned table (deduplication with MD5)
6. **Cleanup**: Removes local CSV file

## Tables Created

- `yellow_taxi_external` - External table pointing to all yellow taxi CSVs in GCS
- `green_taxi_external` - External table pointing to all green taxi CSVs in GCS
- `yellow_taxi_partitioned` - Partitioned table (by `tpep_pickup_datetime`)
- `green_taxi_partitioned` - Partitioned table (by `lpep_pickup_datetime`)

## Execution Time

- With parallel execution: ~15-20 minutes for all 48 files
- Tasks run in parallel based on Airflow's parallelism settings

## Troubleshooting

### "Permission denied" errors
- Check your GCP service account has these roles:
  - BigQuery Data Editor
  - BigQuery Job User
  - Storage Object Creator

### "Table already exists" errors
- Safe to ignore - the DAG handles existing tables gracefully

### Tasks stuck in "queued"
- Increase parallelism in `airflow.cfg`:
  ```ini
  parallelism = 32
  dag_concurrency = 16
  max_active_runs_per_dag = 1
  ```
