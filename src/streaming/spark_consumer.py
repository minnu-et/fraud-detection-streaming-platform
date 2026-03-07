from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from src.detection.rules import apply_basic_rules
from src.streaming.delta_writer import write_to_delta
from src.detection.rules import apply_velocity_rule


TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("is_fraud", BooleanType()),
    StructField("fraud_type", StringType()),
])

def create_spark_session():
    return SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", 
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
def read_from_kafka(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load() \
        .select(
            from_json(col("value").cast("string"), 
                     TRANSACTION_SCHEMA).alias("data")
        ) \
        .select("data.*")

def main():
    print("Starting Spark Streaming consumer...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    df = read_from_kafka(spark)
    df = apply_basic_rules(df)
    
    BASE_PATH = "/home/minnu/Projects/fraud-detection-streaming-platform/data"
    
    # Query 1: write all transactions to Delta Lake
    query1 = write_to_delta(
        df,
        checkpoint_path=f"{BASE_PATH}/checkpoints/transactions",
        output_path=f"{BASE_PATH}/delta/transactions"
    )
    
    # Query 2: velocity fraud alerts

    velocity_df = apply_velocity_rule(df)
    
    query2 = velocity_df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("update") \
        .start()
    
    query1.awaitTermination()


if __name__ == "__main__":
    main()
