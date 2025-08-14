from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from datetime import date
import os

from dotenv import load_dotenv
import os

load_dotenv()

access_key = os.getenv("ACCESS_KEY")
secret_key = os.getenv("SECRET_KEY")   



print(os.getcwd())

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('roads_pipeline') \
    .getOrCreate()

bucket_name = 'yarden-liron-pipeline'
pipeline_name = 'roads'
today = date.today().isoformat()

local_parquet_path = "/home/developer/projects/lab/roads_data_p.parquet"

df = spark.read.parquet(local_parquet_path)


df = df.withColumnRenamed("oid_shvil", "road_id") \
       .withColumnRenamed("shem_mikta", "road_name") \
       .withColumnRenamed("bitzua", "year_exec") \
       .withColumnRenamed("ms_orech", "length") \
       .withColumnRenamed("miflas", "surface_type") \
              


df.show(5)

bucket_name = "yarden-liron-processed-data"
output_path = f"s3a://{bucket_name}/processed/roads/{today}"

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

df.write.mode("overwrite").parquet(output_path)

print(f"DataFrame written to {output_path}")

df.select("geometry").show(5, truncate=False)

spark.stop()