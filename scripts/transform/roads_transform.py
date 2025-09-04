from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from datetime import date
import os
import boto3

 
local_csv_path = "data/load/roads_data.csv"
local_processed_roads = "data/processed/roads_data_p"
s3_bucket_processed = "yarden-liron-processed-data"
s3_key = f'processed/roads'
S3_REGION = "us-east-1" 

s3_bucket_raw = 'yarden-liron-pipeline'
key = f'raw/roads/roads_data.csv'


# ---- start Spark session ----    # S3A setup (matches your successful test)

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("roads_pipeline")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .getOrCreate()
)


# ---- define schema ----
roads_schema = T.StructType([
    T.StructField("oid_shvil", T.LongType(), True),
    T.StructField("width", T.DoubleType(), True),
    T.StructField("shem_mikta", T.StringType(), True),
    T.StructField("direction", T.StringType(), True),
    T.StructField("bitzua",  T.IntegerType(), True),
    T.StructField("ms_orech", T.DoubleType(), True),
    T.StructField("miflas", T.StringType(), True),
    T.StructField("date_created", T.StringType(), True),
    T.StructField("date_import", T.TimestampType(), True),
    T.StructField("Shape_Length", T.DoubleType(), True),
    T.StructField("geometry", T.StringType(), True)
])

# ----- load csv from s3
df = spark.read.csv(
    f's3a://{s3_bucket_raw}/{key}',
    header=True,
    schema=roads_schema
)

# ---- parse geometry string into ArrayType ----
geometry_schema = T.ArrayType(T.ArrayType(T.DoubleType()))
df = df.withColumn(
    "geometry",
    F.from_json(F.col("geometry"), geometry_schema)
)

# ---- rename columns ----
df = df.withColumnRenamed("oid_shvil", "road_id") \
       .withColumnRenamed("shem_mikta", "road_name") \
       .withColumnRenamed("bitzua", "year_exec") \
       .withColumnRenamed("ms_orech", "length") \
       .withColumnRenamed("miflas", "surface_type") \
       .withColumnRenamed("Shape_Length", "shape_length") 

df.show(5)



# ----- null values - roads without name ----
df = df.withColumn(
    "road_name",
    F.when(F.col("road_name").isNull(), F.col("road_id")).otherwise(F.col("road_name"))
)


# ---- save locally as parquet ----
df.write.mode("overwrite").parquet(local_processed_roads)
print(f"DataFrame saved locally at {local_processed_roads}")

# ----- Upload to S3 ------------

df.write \
  .mode("overwrite") \
  .parquet(f's3a://{s3_bucket_processed}/{s3_key}')


print(f"Upload to s3://{s3_bucket_processed}/{s3_key}")

spark.stop()
