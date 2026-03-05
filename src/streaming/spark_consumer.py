from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("timestamp", StringType()),
    StructField("is_fraud", BooleanType()),
    StructField("fraud_type", StringType()),
])

def create_spark_session():
    return SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
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
    
    query = df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
