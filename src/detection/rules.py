from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, window, count
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from datetime import datetime, timedelta

def apply_basic_rules(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "fraud_flag",
        when(col("amount") > 1000, True).otherwise(False)
    ).withColumn(
        "fraud_reason",
        when(col("amount") > 1000, "high_amount").otherwise(None)
    )
def apply_velocity_rule(df, threshold: int = 5, window_duration: str = "5 minutes"):
    """
    Flag users who make more than `threshold` transactions
    within a `window_duration` time window.
    """
    velocity_df = df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), window_duration),
            col("user_id")
        ) \
        .agg(count("*").alias("transaction_count")) \
        .filter(col("transaction_count") > threshold) \
        .select(
            col("user_id"),
            col("transaction_count"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end")
        )
    
    return velocity_df

def apply_geo_anomaly_rule(df: DataFrame,
                            suspicious_countries: list = None) -> DataFrame:
    if suspicious_countries is None:
        suspicious_countries = ["NG", "RU", "KP", "IR"]

    return df.withColumn(
        "geo_flag",
        when(col("country").isin(suspicious_countries), True).otherwise(False)
    ).withColumn(
        "geo_reason",
        when(col("country").isin(suspicious_countries),
             "high_risk_country").otherwise(None)
    )
def apply_amount_spike_rule(df: DataFrame, multiplier: float = 3.0) -> DataFrame:
    """
    Flag transactions where amount is more than 3x the user's average.
    """
    return df.withColumn(
        "amount_spike_flag",
        when(col("amount") > col("user_avg_amount") * multiplier, True).otherwise(False)
    ).withColumn(
        "amount_spike_reason",
        when(col("amount") > col("user_avg_amount") * multiplier,
             "amount_spike_3x_average").otherwise(None)
    )
