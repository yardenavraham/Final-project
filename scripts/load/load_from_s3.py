import boto3
import pandas as pd
import io
from datetime import date

bucket_name = 'yarden-liron-pipeline'
pipeline_name = 'roads'
today = date.today().isoformat()
local_path = "data/load/roads_data_p.parquet"


key = f'raw/{pipeline_name}/{today}/roads_data.csv'

s3 = boto3.client('s3')

obj = s3.get_object(Bucket=bucket_name, Key=key)
df = pd.read_csv(io.BytesIO(obj['Body'].read()))

df.to_parquet(local_path, index=False)

