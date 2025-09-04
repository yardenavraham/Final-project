import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import broadcast

from utils import save_spark_df_to_s3

# -------- CONFIG --------
S3_BUCKET = "yarden-liron-processed-data"
S3_REGION = "us-east-1"
TEL_PREFIX  = "processed/telofun/"
ROADS_PREFIX = "processed/roads/"

bucket_name = "yarden-liron-processed-data"
output_path = f"s3a://{bucket_name}/processed/road_station"
key = "processed/road_station"

s3 = boto3.client("s3")

def get_latest_folder(bucket, prefix):
    """ מחזיר את התיקייה האחרונה תחת prefix """
    result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    folders = [c["Prefix"] for c in result.get("CommonPrefixes", [])]
    if prefix == ROADS_PREFIX:
        return ROADS_PREFIX
    if not folders:
        raise ValueError(f"No folders found under {prefix}")
    return sorted(folders)[-1]   # לוקח את האחרונה (בהנחה YYYY-MM-DD)

# ---------- Spark Session ----------
spark = (
    SparkSession.builder
    .appName("station_nearest_road")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_REGION}.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ---- load latest Tel-O-Fun ----
latest_tel = get_latest_folder(S3_BUCKET, TEL_PREFIX)
print("***Latest Tel-Ofun folder:", latest_tel)

tel_df = spark.read.parquet(f"s3a://{S3_BUCKET}/{latest_tel}")
tel_df = tel_df.select("station_id", "station_name", "lon", "lat") \
               .dropna(subset=["lon","lat"]).distinct()

# ---- load latest Roads ----
latest_roads = get_latest_folder(S3_BUCKET, ROADS_PREFIX)
print("***Latest Roads folder:", latest_roads)

roads_df = spark.read.parquet(f"s3a://{S3_BUCKET}/{latest_roads}")

roads_df = roads_df.select("road_id", "geometry")

# --- seperate points in path---
point_df  = roads_df.select(
    "road_id",
    F.posexplode("geometry").alias("idx_road", "point")
).select(
    "road_id",
    "idx_road",
    F.col("point")[0].alias("lon_road"),
    F.col("point")[1].alias("lat_road")
)

point_df.show(5)

# --- choose only first and last points from each path
window = Window.partitionBy("road_id")
point_df = point_df.withColumn("max_idx_road", F.max("idx_road").over(window))
point_df = point_df.filter((F.col("idx_road") == 0) | (F.col("idx_road") == F.col("max_idx_road"))).drop("max_idx_road")

# --- join

df_joined = point_df.crossJoin(broadcast(tel_df))

df_joined = df_joined.withColumn(
    "dist", 
    F.sqrt((F.col("lon_road") - F.col("lon"))**2 + (F.col("lat_road") - F.col("lat"))**2)
)

# ------ find closest -------------

# for each point in path calculate the path
window2 = Window.partitionBy("road_id", "idx_road").orderBy("dist")
df_closest = df_joined.withColumn("rn", F.row_number().over(window2)).filter(F.col("rn") == 1).drop("rn")

df_closest.show(5, truncate=False)


local_processed_road_station = "data/processed/road_station.parquet"

# ---- save locally as parquet ----
df_closest.write.mode("overwrite").parquet(local_processed_road_station)
print(f"DataFrame saved locally at {local_processed_road_station}")



# ----- Upload to S3 ------------

#df_closest.write.mode("overwrite").parquet(output_path)


save_spark_df_to_s3(
    df=df_closest,
    s3_bucket=bucket_name,
    s3_key= key,
    mode="overwrite"
)


#print(f"Upload to {output_path}")

spark.stop()

