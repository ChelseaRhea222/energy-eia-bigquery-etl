import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env so secrets are not hard-coded.
load_dotenv()

# ===== VALIDATE =====
# Read and validate required API credentials.
API_KEY = os.getenv("EIA_API_KEY")
if not API_KEY:
    raise ValueError("Missing EIA_API_KEY. Put it in a .env file.")

# EIA endpoint for state electricity net metering data.
URL = "https://api.eia.gov/v2/electricity/state-electricity-profiles/net-metering/data/"

def fetch_net_metering(limit=5000, offset=0):
    # ===== EXTRACT =====
    # Build query parameters (frequency, selected metrics, sorting, pagination).
    params = {
        "api_key": API_KEY,
        "frequency": "annual",
        "data[0]": "capacity",
        "data[1]": "customers",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": offset,
        "length": limit
    }

    # Call the API, fail fast on HTTP errors, and return the records array.
    r = requests.get(URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload["response"]["data"]

def main():
    # ===== EXTRACT =====
    # Extract records from the API and load them into a DataFrame.
    records = fetch_net_metering()
    df = pd.DataFrame(records)

    # ===== VALIDATE =====
    # Basic validation/inspection output for quick sanity checks.
    print("Pulled rows:", len(df))
    print("Columns:", df.columns.tolist())
    print(df.head(10))

    # Write a local sample file for inspection/debugging.
    df.to_csv("net_metering_sample.csv", index=False)
    print("Wrote net_metering_sample.csv")

# Run the ETL fetch flow when this script is executed directly.
if __name__ == "__main__":
    main()
