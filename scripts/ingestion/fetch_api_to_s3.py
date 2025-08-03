import requests
import pandas as pd
import boto3
import os

# ==== הגדרות ====
url = "https://data.gov.il/api/3/action/datastore_search?resource_id=6c6f191a-0839-411d-ac4f-59abe36a3593"
local_filename = "data/output/api_data.csv"
s3_bucket = "your-bucket-name"  # החליפי בשם הדלי שלך
s3_key = "raw/api_data.csv"     # הנתיב בתוך הדלי

try:
    print("שולחת בקשה ל-API...")
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    if not data.get("success", False):
        raise ValueError("התגובה מה-API נכשלה (success=False)")

    records = data["result"]["records"]
    df = pd.DataFrame(records)
    print(f"נטענו {len(df)} שורות")

    # שמירה מקומית
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
    df.to_csv(local_filename, index=False)
    print(f"הקובץ נשמר מקומית: {local_filename}")

    # העלאה ל-S3
    s3 = boto3.client("s3")
    s3.upload_file(local_filename, s3_bucket, s3_key)
    print(f"הקובץ הועלה ל־S3: s3://{s3_bucket}/{s3_key}")

except requests.exceptions.RequestException as e:
    print(f"שגיאת רשת: {e}")

except ValueError as ve:
    print(f"שגיאה בתוכן הנתונים: {ve}")

except Exception as ex:
    print(f"שגיאה כללית: {ex}")
