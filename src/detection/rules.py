from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, window, count

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
