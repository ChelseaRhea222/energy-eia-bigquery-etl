# Energy Net Metering Data Pipeline

An end-to-end data engineering pipeline that ingests public U.S. energy net-metering data from the EIA API, transforms and validates it with Python, and loads it into Google BigQuery for analytics and dashboards.

This project demonstrates practical **ETL, API ingestion, cloud warehousing, and automated data loading** using modern analytics tools.

---

## Overview

This pipeline:

1. Pulls annual net-metering data from the U.S. Energy Information Administration (EIA) API
2. Cleans and standardizes fields
3. Enforces schema + data types
4. Loads data into BigQuery
5. Makes it ready for dashboards (Looker Studio / SQL analysis)

Result → production-style, analysis-ready warehouse table

---

## Tech Stack

* Python
* Pandas
* Requests
* Google BigQuery
* dotenv (.env secrets)
* EIA Public API

---

## Project Structure

```
energy-net-metering/
│
├── etl_fetch.py           # API → local sample CSV
├── etl_to_bigquery.py     # API → transform → BigQuery
├── bq_test.py             # BigQuery connection test
├── net_metering_sample.csv
├── .env                   # API keys (not committed)
└── README.md
```

---

## Pipeline Architecture

```
EIA API
   ↓
Python ETL (clean + transform)
   ↓
BigQuery Warehouse
   ↓
SQL / Dashboards / Analytics
```

---

## Setup

### 1. Install dependencies

```
pip install pandas requests python-dotenv google-cloud-bigquery
```

### 2. Create `.env`

```
EIA_API_KEY=your_key_here
GCP_PROJECT_ID=your_project_id
```

---

## Usage

### Pull sample data locally

Fetches API data and saves a CSV preview
Runs: 

```
python etl_fetch.py
```

---

### Load into BigQuery

Creates dataset + table automatically and appends rows
Runs: 

```
python etl_to_bigquery.py
```

---

### Test BigQuery connection

Confirms credentials + datasets
Runs: 

```
python bq_test.py
```

---

## Example BigQuery Queries

### Total capacity by year

```sql
SELECT
  period,
  SUM(capacity) AS total_capacity
FROM energy.net_metering_annual
GROUP BY period
ORDER BY period;
```

### Capacity by state

```sql
SELECT
  stateName,
  SUM(capacity) AS capacity_mw
FROM energy.net_metering_annual
GROUP BY stateName
ORDER BY capacity_mw DESC;
```

---

## Skills Demonstrated

* API ingestion
* ETL pipeline design
* Data cleaning + schema enforcement
* Cloud data warehousing
* BigQuery automation
* Analytics-ready modeling
* Production-style project structure

---

## Author

Chelsea Rhea
Data Analytics & Engineering Portfolio
GitHub • SQL • BigQuery • Python • ETL
