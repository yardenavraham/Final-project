from pyspark.sql import DataFrame

def save_spark_df_to_s3(
    df: DataFrame,
    s3_bucket: str,
    s3_key: str,
    mode: str,
    show_log: bool = True
):

    s3_path = f"s3a://{s3_bucket}/{s3_key}"
    
    df.write.mode(mode).parquet(s3_path)
    
    if show_log:
        print(f"Spark DataFrame uploaded to {s3_path} in {mode} mode.")
