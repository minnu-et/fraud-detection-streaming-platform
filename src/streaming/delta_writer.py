from pyspark.sql import DataFrame

def write_to_delta(df: DataFrame, 
                   checkpoint_path: str, 
                   output_path: str) -> None:
    query = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .outputMode("append") \
        .start(output_path)
    
    return query
