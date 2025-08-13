import requests
import pandas as pd
import boto3
import os
from datetime import date
from shapely.geometry import LineString


# API endpoint
url = "https://gisn.tel-aviv.gov.il/GisOpenData/service.asmx/GetLayer"
local_filename = "data/raw/roads_data.csv"
s3_bucket = "yarden-liron-pipeline"
today = date.today().isoformat()
s3_key = f"raw/roads/{today}/roads_data.csv"

# Parameters for the API request
params = {
    "layerCode": 577,       # Tel-O-Fun layer
    "layerWhere": "",
    "xmin": "",
    "ymin": "",
    "xmax": "",
    "ymax": "",
    "projection": "itm"     # Can also use "wgs84" for lat/lon
}

# Send GET request to the API
response = requests.get(url, params=params, timeout=60)
response.raise_for_status()  # Stop if HTTP error

# Parse JSON
data = response.json()

features = data["features"]


# יוצרים רשימה של dict שמשלבת attributes ו-geometry
rows = []
for feature in features:
    row = feature['attributes'].copy()  # מאפייני השביל
    row['geometry'] = feature['geometry']['paths'][0] # מוסיפים את הגיאומטריה כdict
    rows.append(row)

df = pd.DataFrame(rows)

print(df.head())

'''
def paths_to_wkt(paths):
    # paths זה רשימה של רשימות נקודות, למשל [[[x1,y1],[x2,y2],...]]
    # נניח שמדובר בקו אחד (polyline)
    line = LineString(paths[0])
    return line.wkt
df['geometry_wkt'] = df['geometry'].apply(lambda g: paths_to_wkt(g['paths']) if g else None)
print(df[['oid_shvil', 'geometry_wkt']].head())
'''

os.makedirs(os.path.dirname(local_filename), exist_ok=True)
df.to_csv(local_filename, index=False)
print(f"Local save: {local_filename}")

# Upload to S3
s3 = boto3.client("s3")
s3.upload_file(local_filename, s3_bucket, s3_key)
print(f"Upload to s3://{s3_bucket}/{s3_key}")
