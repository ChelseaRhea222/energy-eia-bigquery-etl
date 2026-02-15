import os
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables from .env.
load_dotenv()

# Project and resource configuration (with dataset/table defaults).
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "energy")
TABLE = os.getenv("BQ_TABLE", "net_metering_annual")
EIA_API_KEY = os.getenv("EIA_API_KEY")

# Validate required credentials/config before running ETL.
if not PROJECT_ID:
    raise ValueError("Missing GCP_PROJECT_ID in .env")
if not EIA_API_KEY:
    raise ValueError("Missing EIA_API_KEY in .env")

# EIA endpoint used for source extraction.
EIA_URL = "https://api.eia.gov/v2/electricity/state-electricity-profiles/net-metering/data/"

def fetch_net_metering(limit=5000, offset=0):
    # Build request params and fetch one page of EIA records.
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "annual",
        "data[0]": "capacity",
        "data[1]": "customers",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": offset,
        "length": limit
    }
    r = requests.get(EIA_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()["response"]["data"]

def ensure_dataset(client):
    # Create dataset if it does not already exist.
    dataset_id = f"{PROJECT_ID}.{DATASET}"
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")

def ensure_table(client):
    # Create target table with explicit schema if missing.
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    schema = [
        bigquery.SchemaField("period", "INTEGER"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("stateName", "STRING"),
        bigquery.SchemaField("technology", "STRING"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("sectorName", "STRING"),
        bigquery.SchemaField("capacity", "FLOAT"),
        bigquery.SchemaField("customers", "INTEGER"),
        bigquery.SchemaField("capacity_units", "STRING"),
        bigquery.SchemaField("customers_units", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Created table {table_id}")

def transform(records):
    # ===== TRANSFORM =====
    # Convert API records to a DataFrame and standardize schema/types.
    df = pd.DataFrame(records).copy()

    df = df.rename(columns={
        "capacity-units": "capacity_units",
        "customers-units": "customers_units",
    })

    df["period"] = pd.to_numeric(df["period"], errors="coerce").astype("Int64")
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
    df["customers"] = pd.to_numeric(df["customers"], errors="coerce").astype("Int64")

    df["ingested_at"] = pd.Timestamp.now("UTC")

    # Drop rows missing required business keys.
    df = df.dropna(subset=["period", "state", "technology", "sector"])

    return df

def load_to_bigquery(client, df):
    # ===== LOAD =====
    # Append transformed rows into the destination BigQuery table.
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows into {table_id}")

def main():
    # Orchestrate ETL: init client, ensure targets, extract, transform, load.
    client = bigquery.Client(project=PROJECT_ID)

    ensure_dataset(client)
    ensure_table(client)

    records = fetch_net_metering()
    df = transform(records)

    print("Ready to load rows:", len(df))
    load_to_bigquery(client, df)

if __name__ == "__main__":
    main()
