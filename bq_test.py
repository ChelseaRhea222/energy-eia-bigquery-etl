import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

project_id = os.getenv("GCP_PROJECT_ID")
client = bigquery.Client(project=project_id)

print("✅ Using project:", project_id)
print("✅ BigQuery client created")

datasets = list(client.list_datasets(project=project_id))
print("✅ Datasets found:", len(datasets))
for ds in datasets[:5]:
    print(" -", ds.dataset_id)
