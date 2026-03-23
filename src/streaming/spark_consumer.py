from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from src.detection.rules import apply_basic_rules, apply_velocity_rule, apply_geo_anomaly_rule, apply_amount_spike_rule
from src.streaming.delta_writer import write_to_delta
from pyspark.sql.streaming.state import GroupStateTimeout
from src.utils.config import load_config, get_full_path

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
    StructField("user_avg_amount", DoubleType()),
])

def create_spark_session(config: dict = None):
    if config is None:
        config = load_config()
    return SparkSession.builder \
        .appName(config["spark"]["app_name"]) \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", 
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", 
                config["spark"]["shuffle_partitions"]) \
        .getOrCreate()

def read_from_kafka(spark, config: dict = None):
    if config is None:
        config = load_config()
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 
                config["kafka"]["bootstrap_servers"]) \
        .option("subscribe", config["kafka"]["topic"]) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(
            from_json(col("value").cast("string"), 
                     TRANSACTION_SCHEMA).alias("data")
        ) \
        .select("data.*") \
        .filter(col("transaction_id").isNotNull())
def main():
    config = load_config()
    
    print("Starting Spark Streaming consumer...")
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel(config["spark"]["log_level"])
    
    df = read_from_kafka(spark, config)
    df = apply_basic_rules(df)
    df = apply_geo_anomaly_rule(df, config["fraud_rules"]["geo_risk_countries"])
    df = apply_amount_spike_rule(df, config["fraud_rules"]["amount_spike_multiplier"])
    
    BASE_PATH = config["paths"]["base"]
    
    query1 = write_to_delta(
        df,
        checkpoint_path=f"{BASE_PATH}/{config['paths']['checkpoints']['transactions']}",
        output_path=f"{BASE_PATH}/{config['paths']['delta']['transactions']}"
    )
    
    velocity_df = apply_velocity_rule(
        df,
        threshold=config["fraud_rules"]["velocity_threshold"],
        window_duration=config["fraud_rules"]["velocity_window"]
    )
    
    query2 = write_to_delta(
        velocity_df,
        checkpoint_path=f"{BASE_PATH}/{config['paths']['checkpoints']['velocity_alerts']}",
        output_path=f"{BASE_PATH}/{config['paths']['delta']['velocity_alerts']}",
        mode="append"
    )
    
    query1.awaitTermination()

if __name__ == "__main__":
    main()
