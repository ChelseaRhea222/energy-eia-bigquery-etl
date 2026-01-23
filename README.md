# Energy EIA → BigQuery ETL

Python ETL pipeline that pulls U.S. EIA net metering data (capacity + customers) via API and loads it into Google BigQuery for analytics and dashboards.

## Data Source
EIA Open Data (State Electricity Profiles → Net Metering)

## What it does
- Fetches latest annual net metering data from EIA API
- Normalizes fields and types
- Creates BigQuery dataset/table if missing
- Appends rows to BigQuery table

## Setup

### 1) Create a `.env` file (do not commit)
```env
EIA_API_KEY=YOUR_EIA_KEY
GCP_PROJECT_ID=energy-eia-16657
BQ_DATASET=energy
BQ_TABLE=net_metering_annual
