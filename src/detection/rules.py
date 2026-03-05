from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def apply_basic_rules(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "fraud_flag",
        when(col("amount") > 1000, True).otherwise(False)
    ).withColumn(
        "fraud_reason",
        when(col("amount") > 1000, "high_amount").otherwise(None)
    )
