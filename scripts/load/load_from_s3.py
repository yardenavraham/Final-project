import boto3
import pandas as pd
import io

bucket_name = 'yarden-liron-pipeline'
pipeline_name = 'roads'

from datetime import date
today = date.today().isoformat()

key = f'raw/{pipeline_name}/{today}/roads_data.csv'

s3 = boto3.client('s3')

obj = s3.get_object(Bucket=bucket_name, Key=key)
df = pd.read_csv(io.BytesIO(obj['Body'].read()))

print(df.head())
