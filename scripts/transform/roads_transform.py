from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from datetime import date
import os
from dotenv import load_dotenv

load_dotenv()
#print(os.getcwd())
access_key = os.getenv("ACCESS_KEY")
secret_key = os.getenv("SECRET_KEY")   
today = date.today().isoformat()
local_parquet_path = "data/load/roads_data_p.parquet"



# ---- get parquet file with spark ----

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('roads_pipeline') \
    .getOrCreate()

df = spark.read.parquet(local_parquet_path)

print(list(df.columns))
# ---- change columns names ----
df = df.withColumnRenamed("oid_shvil", "road_id") \
       .withColumnRenamed("shem_mikta", "road_name") \
       .withColumnRenamed("bitzua", "year_exec") \
       .withColumnRenamed("ms_orech", "length") \
       .withColumnRenamed("miflas", "surface_type") \
       .withColumnRenamed("Shape_Length", "shape_length") 
print(list(df.columns))     

#df.select("surface_type").distinct().show()   
#df.select("geometry").show(5, truncate=False)  
    
df.show(5)

# ---- spark to s3 ----

bucket_name = "yarden-liron-processed-data"
output_path = f"s3a://{bucket_name}/processed/roads/{today}"

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

df.write.mode("overwrite").parquet(output_path)

print(f"DataFrame written to {output_path}")



spark.stop()