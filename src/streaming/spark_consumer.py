from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, lit,current_timestamp,expr
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
    import os
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    
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
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
            col("value").cast("string").alias("raw_value")
        )\
            .select("data.*", "raw_value")  
def main():
    config = load_config()
    BASE_PATH = config["paths"]["base"]
    print("Starting Spark Streaming consumer...")
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel(config["spark"]["log_level"])
    
    df = read_from_kafka(spark, config)
    
    good_df = df.filter(col("transaction_id").isNotNull())
    bad_df = df.filter(col("transaction_id").isNull())
    dlq_df = bad_df.select(to_json(struct(col("raw_value").alias("raw_message"),
                                          current_timestamp().alias("failed_at"),
                                          lit("parse_failed").alias("reason"))).alias("value"))
    good_df = apply_basic_rules(good_df)
    good_df = apply_geo_anomaly_rule(good_df, config["fraud_rules"]["geo_risk_countries"])
    good_df = apply_amount_spike_rule(good_df, config["fraud_rules"]["amount_spike_multiplier"])
    velocity_df = apply_velocity_rule(
        good_df,
        threshold=config["fraud_rules"]["velocity_threshold"],
        window_duration=config["fraud_rules"]["velocity_window"]
    )

    high_amount_alerts = good_df \
    .filter(col("fraud_flag") == True) \
    .select(
        expr("uuid()").alias("alert_id"),
        col("transaction_id"),
        col("user_id"),
        lit("high_amount").alias("rule_name"),
        lit("HIGH").alias("severity"),
        current_timestamp().alias("triggered_at"),
        lit(None).cast("timestamp").alias("window_start"),
        lit(None).cast("timestamp").alias("window_end")
    )

    geo_anomaly_alerts = good_df \
    .filter(col("geo_flag") == True) \
    .select(
        expr("uuid()").alias("alert_id"),
        col("transaction_id"),
        col("user_id"),
        lit("geo_anomaly").alias("rule_name"),
        lit("MEDIUM").alias("severity"),
        current_timestamp().alias("triggered_at"),
        lit(None).cast("timestamp").alias("window_start"),
        lit(None).cast("timestamp").alias("window_end")
    )

    amount_spike_alerts = good_df \
    .filter(col("amount_spike_flag") == True) \
    .select(
        expr("uuid()").alias("alert_id"),
        col("transaction_id"),
        col("user_id"),
        lit("amount_spike").alias("rule_name"),
        lit("HIGH").alias("severity"),
        current_timestamp().alias("triggered_at"),
        lit(None).cast("timestamp").alias("window_start"),
        lit(None).cast("timestamp").alias("window_end")
    )

    velocity_alerts = velocity_df \
    .select(
        expr("uuid()").alias("alert_id"),
        lit(None).cast("string").alias("transaction_id"),
        col("user_id"),
        lit("velocity_fraud").alias("rule_name"),
        lit("HIGH").alias("severity"),
        current_timestamp().alias("triggered_at"),
        col("window_start"),
        col("window_end")
    )

    alerts_df = high_amount_alerts \
    .union(geo_anomaly_alerts) \
    .union(amount_spike_alerts) \
    .union(velocity_alerts)
    
    query1 = write_to_delta(
        good_df,
        checkpoint_path=f"{BASE_PATH}/{config['paths']['checkpoints']['transactions']}",
        output_path=f"{BASE_PATH}/{config['paths']['delta']['transactions']}"
    )
    
    
    
    query2 = write_to_delta(
        velocity_df,
        checkpoint_path=f"{BASE_PATH}/{config['paths']['checkpoints']['velocity_alerts']}",
        output_path=f"{BASE_PATH}/{config['paths']['delta']['velocity_alerts']}",
        mode="append"
    )

    query3 = dlq_df.writeStream\
             .format("kafka")\
             .option("kafka.bootstrap.servers",config["kafka"]["bootstrap_servers"])\
             .option("topic","transactions_dlq")\
             .option("checkpointLocation",f"{BASE_PATH}/{config['paths']['checkpoints']['dlq']}")\
             .start()

    query4 = write_to_delta(
    alerts_df,
    checkpoint_path=f"{BASE_PATH}/{config['paths']['checkpoints']['unified_alerts']}",
    output_path=f"{BASE_PATH}/{config['paths']['delta']['unified_alerts']}",
    mode="append"
    )
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
