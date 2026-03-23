from pyspark.sql import DataFrame

def write_to_delta(df, checkpoint_path: str, 
                   output_path: str, mode: str = "append"):
    query = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .outputMode(mode) \
        .start(output_path)
    
    return query
