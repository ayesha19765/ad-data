# adaptive-ads/airflow/dags/load_imdb_movie_datasets_local.py

import os
from io import StringIO
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
# add near the imports
import re

def _sanitize_columns(cols):
    new, used = [], set()
    for c in cols:
        s = c.strip().lower()
        s = re.sub(r'[^0-9a-z_]+', '_', s)   # keep only [a-z0-9_]
        s = re.sub(r'_+', '_', s).strip('_') # collapse/trim underscores
        if not s or s[0].isdigit():          # cannot be empty / start with digit
            s = 'c_' + s
        base, i = s, 1
        while s in used:
            i += 1
            s = f'{base}_{i}'
        used.add(s)
        new.append(s)
    return new


# ---- Environment / constants -------------------------------------------------
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
FAKE_GCS_ROOT = "/opt/airflow/fake_gcs"

BASE_URL = "https://github.com/AarthiHonguthi/ad-analytics/raw/main/dbt/seeds/imdb_movie_dataset"

# Use ONLY the genres that exist in your local dataset
GENRES = [
    "action", "adventure", "animation", "biography",
    "crime", "family", "fantasy", "film-noir",
    "history", "horror", "mystery", "romance",
    "scifi", "sports", "thriller", "war",
]

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "imdb_dataset")

# Feature flag: set ENABLE_BQ_LOAD=true in .env to enable BigQuery loading
ENABLE_BQ_LOAD = os.environ.get("ENABLE_BQ_LOAD", "false").lower() == "true"


# ---- Helpers -----------------------------------------------------------------
def check_file_exists(url: str) -> bool:
    """Return True if the CSV URL exists (HTTP 200)."""
    try:
        r = requests.head(url, allow_redirects=True, timeout=20)
        return r.status_code == 200
    except Exception:
        return False


def csv_url_to_parquet(url: str, out_dir: str, parquet_name: str) -> str:
    """
    Read CSV directly from URL and write Parquet to fake_gcs.
    Removes the need for any intermediate local CSV files.
    """
    os.makedirs(out_dir, exist_ok=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    # Load CSV -> DataFrame -> Parquet
    df = pd.read_csv(StringIO(r.text))
    out_path = os.path.join(out_dir, parquet_name)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path)
    return out_path


def load_parquet_to_bigquery(parquet_path: str, table_id: str) -> None:
    """
    Load local Parquet file into BigQuery (no GCS needed).
    WRITE_TRUNCATE makes re-runs idempotent. Also sanitize column names to be BQ-safe.
    """
    df = pd.read_parquet(parquet_path)

    # sanitize columns (e.g., "gross(in $)" -> "gross_in")
    original = list(df.columns)
    df.columns = _sanitize_columns(df.columns)
    if original != list(df.columns):
        print("Column renames:", "; ".join(f"{o}->{n}" for o, n in zip(original, df.columns) if o != n))

    client = bigquery.Client(project=GCP_PROJECT_ID)
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )
    job.result()  # wait for the load to finish



# ---- DAG ---------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="load_imdb_movie_datasets_local",
    description="Download IMDb CSVs -> Parquet into fake_gcs -> (optional) load to BigQuery (no GCS)",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2024, 5, 21),
    catchup=False,
    tags=["imdb", "movies", "parquet", "no-gcs"],
) as dag:

    for genre in GENRES:
        csv_url      = f"{BASE_URL}/{genre}.csv"
        out_dir      = f"{FAKE_GCS_ROOT}/{genre}"
        parquet_name = f"{genre}.parquet"
        parquet_path = f"{out_dir}/{parquet_name}"
        bq_table_id  = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{genre}"

        # Skip genres whose CSV isn't available
        check_task = ShortCircuitOperator(
            task_id=f"check_{genre}_exists",
            python_callable=check_file_exists,
            op_kwargs={"url": csv_url},
        )

        # Build Parquet directly (URL -> DF -> Parquet in fake_gcs)
        build_parquet_task = PythonOperator(
            task_id=f"build_{genre}_parquet",
            python_callable=csv_url_to_parquet,
            op_kwargs={
                "url": csv_url,
                "out_dir": out_dir,
                "parquet_name": parquet_name,
            },
        )

        if ENABLE_BQ_LOAD:
            load_bq_task = PythonOperator(
                task_id=f"load_{genre}_parquet_to_bigquery",
                python_callable=load_parquet_to_bigquery,
                op_kwargs={
                    "parquet_path": parquet_path,
                    "table_id": bq_table_id,
                },
            )
            check_task >> build_parquet_task >> load_bq_task
        else:
            check_task >> build_parquet_task
