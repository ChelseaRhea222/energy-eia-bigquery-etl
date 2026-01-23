import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("EIA_API_KEY")
if not API_KEY:
    raise ValueError("Missing EIA_API_KEY. Put it in a .env file.")

URL = "https://api.eia.gov/v2/electricity/state-electricity-profiles/net-metering/data/"

def fetch_net_metering(limit=5000, offset=0):
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

    r = requests.get(URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload["response"]["data"]

def main():
    records = fetch_net_metering()
    df = pd.DataFrame(records)

    print("✅ Pulled rows:", len(df))
    print("Columns:", df.columns.tolist())
    print(df.head(10))

    # Save a sample CSV locally (optional, not required for BigQuery)
    df.to_csv("net_metering_sample.csv", index=False)
    print("✅ Wrote net_metering_sample.csv")

if __name__ == "__main__":
    main()
