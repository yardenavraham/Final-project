import requests
import pandas as pd
import boto3
import os
from datetime import date

# API and storage config
url = "https://data.gov.il/api/3/action/datastore_search?resource_id=6c6f191a-0839-411d-ac4f-59abe36a3593"
local_filename = "data/output/telofun_data.csv"
s3_bucket = "yarden-liron-pipeline" 
today = date.today().isoformat()
s3_key = f"raw/telofun/{today}/telofun_data.csv"

try:
    print("Send request to Tel-O-Fun API...")
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    if not data.get("success", False):
        raise ValueError("API returned success=False")

    records = data["result"]["records"]
    df = pd.DataFrame(records)
    print(f"✅ Loaded {len(df)} records")

    # Save locally
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
    df.to_csv(local_filename, index=False)
    print(f"✅ Saved to local file: {local_filename}")

    # Upload to S3
    s3 = boto3.client("s3")
    s3.upload_file(local_filename, s3_bucket, s3_key)
    print(f"✅ Uploaded to s3://{s3_bucket}/{s3_key}")

except requests.exceptions.RequestException as e:
    print(f"❌ Network error: {e}")

except ValueError as ve:
    print(f"❌ Data content error: {ve}")

except Exception as ex:
    print(f"❌ General error: {ex}")
