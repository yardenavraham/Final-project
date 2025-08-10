from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from datetime import date

import os
print(os.getcwd())

spark = SparkSession.builder \
    .master("local") \
    .appName('roads_pipeline') \
    .getOrCreate()

bucket_name = 'yarden-liron-pipeline'
pipeline_name = 'roads'
today = date.today().isoformat()

local_parquet_path = "/home/developer/projects/lab/roads_data_p.parquet"

df = spark.read.parquet(local_parquet_path)

df.show(5)


df = df.withColumnRenamed("oid_shvil", "id") \
       .withColumnRenamed("shem_mikta", "road_section") \
       .withColumnRenamed("bitzua", "year_exec") \
       .withColumnRenamed("ms_orech", "length") \
       .withColumnRenamed("miflas", "surface_type") \
              


df.show(5)

bucket_name = "yarden-liron-processed-data"
output_path = f"s3a://{bucket_name}/processed/roads/{today}"

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA4EIYVDJXZG3W6X24")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

df.write.mode("overwrite").parquet(output_path)

print(f"DataFrame written to {output_path}")

spark.stop()