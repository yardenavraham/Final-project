from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from datetime import datetime

# ---------- Config ----------
KAFKA_BROKER = "course-kafka:9092"
KAFKA_TOPIC  = "telofun_raw"

S3_BUCKET = "yarden-liron-processed-data"
S3_REGION = "us-east-1" 

OUT_PATH = f"s3a://{S3_BUCKET}/processed/telofun/"
RUN_ID   = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
CHK_PATH = f"s3a://{S3_BUCKET}/_chk/station_status_stream/{RUN_ID}"  # fresh checkpoint each run

# ---------- Spark Session ----------
spark = (
    SparkSession.builder
    .appName("telofun_processed_diag")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )
    # S3A setup (matches your successful test)
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_REGION}.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ---------- Schemas ----------
attributes_schema = StructType([
    StructField("OBJECTID",       LongType(),   True),
    StructField("tachana_id",     StringType(), True),
    StructField("Shem_tachana",   StringType(), True),
    StructField("Teur_tachana",   StringType(), True),
    StructField("free_bikes",     IntegerType(), True),
    StructField("free_bikesE",    IntegerType(), True),
    StructField("free_amudim",    IntegerType(), True),
    StructField("free_amudimE",   IntegerType(), True),
    StructField("shabat",         StringType(), True),
    StructField("lon",            DoubleType(), True),
    StructField("lat",            DoubleType(), True),
    StructField("x",              DoubleType(), True),
    StructField("y",              DoubleType(), True),
    StructField("UniqueId",       StringType(), True),
    StructField("guid",           StringType(), True),
    StructField("date_import",    StringType(), True)
])

geometry_schema = StructType([
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True)
])

record_schema = StructType([
    StructField("attributes", attributes_schema, True),
    StructField("geometry",   geometry_schema,   True)
])

# ---------- Read from Kafka ----------
raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BROKER)
         .option("subscribe", KAFKA_TOPIC)
         # For first run / debugging: consume backlog to prove data flows
         .option("startingOffsets", "earliest")
         # Avoid failing if old offsets are pruned by Kafka
         .option("failOnDataLoss", "false")
         # Avoid huge first batch
         .option("maxOffsetsPerTrigger", "2000")
         .load()
)

# ---------- Parse JSON ----------
parsed = (
    raw.select(
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast("string").alias("json")
    )
    .select(
        "kafka_ts",
        F.from_json("json", record_schema).alias("r")
    )
)

# ---------- Flatten + Normalize ----------
records = (
    parsed
    .withColumn("station_id",     F.col("r.attributes.tachana_id"))
    .withColumn("station_name",   F.coalesce(F.col("r.attributes.Shem_tachana"),
                                             F.col("r.attributes.Teur_tachana")))
    .withColumn("bikes_available",F.col("r.attributes.free_bikes"))
    .withColumn("docks_available",F.col("r.attributes.free_amudim"))
    .withColumn("lon",            F.col("r.attributes.lon"))
    .withColumn("lat",            F.col("r.attributes.lat"))
    .withColumn("x_itm",          F.coalesce(F.col("r.attributes.x"), F.col("r.geometry.x")))
    .withColumn("y_itm",          F.coalesce(F.col("r.attributes.y"), F.col("r.geometry.y")))
    .withColumn("unique_id",      F.col("r.attributes.UniqueId"))
    .withColumn("object_id",      F.col("r.attributes.OBJECTID"))
    .withColumn("guid",           F.col("r.attributes.guid"))
    .withColumn(
        "event_time",
        F.coalesce(
            F.to_timestamp(F.col("r.attributes.date_import"), "dd/MM/yyyy HH:mm:ss"),
            F.col("kafka_ts")
        )
    )
    .drop("r")
    .withColumn("dt",   F.to_date("event_time"))
    .withColumn("hour", F.date_format("event_time", "HH"))
)

# ---------- Output Columns ----------
final_cols = [
    "station_id", "station_name",
    "x_itm", "y_itm", "lon", "lat",
    "bikes_available", "docks_available",
    "event_time", "dt", "hour",
    "unique_id", "object_id", "guid"
]

final_df = records.select(*final_cols)

# ---------- Start two sinks: console (debug) + S3 ----------
# 1) Console: shows that micro-batches are happening + row content
console_query = (
    final_df.writeStream
            .format("console")
            .option("truncate", "false")
            .option("numRows", "5")
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
)

# 2) S3 Parquet sink
s3_query = (
    final_df.writeStream
            .format("parquet")
            .option("path", OUT_PATH)
            .option("checkpointLocation", CHK_PATH)  # fresh checkpoint per run
            .partitionBy("dt", "hour")
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
)

console_query.awaitTermination()
s3_query.awaitTermination()
