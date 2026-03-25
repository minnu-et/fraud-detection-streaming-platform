import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from datetime import datetime
from src.detection.rules import (
    apply_basic_rules,
    apply_geo_anomaly_rule,
    apply_amount_spike_rule,
    apply_velocity_rule,
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestFraudRules") \
        .master("local[1]") \
        .getOrCreate()


def create_test_df(spark, rows):
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("country", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


# --- Basic rules ---

def test_high_amount_flagged(spark):
    df = create_test_df(spark, [("T001", "USR_0001", 1500.0, "IN")])
    result = apply_basic_rules(df)
    row = result.collect()[0]
    assert row["fraud_flag"] == True
    assert row["fraud_reason"] == "high_amount"


def test_normal_amount_not_flagged(spark):
    df = create_test_df(spark, [("T002", "USR_0002", 200.0, "IN")])
    result = apply_basic_rules(df)
    row = result.collect()[0]
    assert row["fraud_flag"] == False
    assert row["fraud_reason"] is None


# --- Geo anomaly ---

def test_geo_anomaly_high_risk_country(spark):
    df = create_test_df(spark, [("T003", "USR_0003", 100.0, "NG")])
    result = apply_geo_anomaly_rule(df)
    row = result.collect()[0]
    assert row["geo_flag"] == True
    assert row["geo_reason"] == "high_risk_country"


def test_geo_anomaly_safe_country(spark):
    df = create_test_df(spark, [("T004", "USR_0004", 100.0, "IN")])
    result = apply_geo_anomaly_rule(df)
    row = result.collect()[0]
    assert row["geo_flag"] == False
    assert row["geo_reason"] is None


# --- Amount spike ---

def test_amount_spike_flagged(spark):
    df = create_test_df(spark, [("T005", "USR_0005", 900.0, "IN")])
    df = df.withColumn("user_avg_amount", col("amount").cast("double") / 10)
    result = apply_amount_spike_rule(df)
    row = result.collect()[0]
    assert row["amount_spike_flag"] == True


def test_amount_spike_not_flagged(spark):
    df = create_test_df(spark, [("T006", "USR_0006", 300.0, "IN")])
    df = df.withColumn("user_avg_amount", col("amount").cast("double") / 2)
    result = apply_amount_spike_rule(df)
    row = result.collect()[0]
    assert row["amount_spike_flag"] == False


# --- Velocity rule ---

def create_velocity_df(spark, rows):
    """
    Velocity rule needs a timestamp column.
    withWatermark is silently ignored on static DataFrames —
    this test covers the groupBy/count logic only.
    Watermark behavior requires integration testing against a real stream.
    """
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("country", StringType()),
        StructField("timestamp", TimestampType()),
    ])
    return spark.createDataFrame(rows, schema)


def test_velocity_rule_flags_user_over_threshold(spark):
    # 6 transactions for the same user within the same 5-minute window
    # threshold is 5, so this should produce 1 alert row
    t = datetime(2024, 1, 1, 10, 0, 0)
    rows = [
        ("T101", "USR_0010", 100.0, "IN", datetime(2024, 1, 1, 10, 0, 0)),
        ("T102", "USR_0010", 100.0, "IN", datetime(2024, 1, 1, 10, 1, 0)),
        ("T103", "USR_0010", 100.0, "IN", datetime(2024, 1, 1, 10, 2, 0)),
        ("T104", "USR_0010", 100.0, "IN", datetime(2024, 1, 1, 10, 3, 0)),
        ("T105", "USR_0010", 100.0, "IN", datetime(2024, 1, 1, 10, 4, 0)),
        ("T106", "USR_0010", 100.0, "IN", datetime(2024, 1, 1, 10, 4, 30)),
    ]
    df = create_velocity_df(spark, rows)
    result = apply_velocity_rule(df, threshold=5, window_duration="5 minutes")
    rows_out = result.collect()

    assert len(rows_out) == 1
    row = rows_out[0]
    assert row["user_id"] == "USR_0010"
    assert row["transaction_count"] == 6
    assert row["window_start"] is not None
    assert row["window_end"] is not None


def test_velocity_rule_does_not_flag_user_under_threshold(spark):
    # only 3 transactions — should not appear in results
    rows = [
        ("T201", "USR_0020", 100.0, "IN", datetime(2024, 1, 1, 10, 0, 0)),
        ("T202", "USR_0020", 100.0, "IN", datetime(2024, 1, 1, 10, 1, 0)),
        ("T203", "USR_0020", 100.0, "IN", datetime(2024, 1, 1, 10, 2, 0)),
    ]
    df = create_velocity_df(spark, rows)
    result = apply_velocity_rule(df, threshold=5, window_duration="5 minutes")
    rows_out = result.collect()

    assert len(rows_out) == 0


def test_velocity_rule_only_flags_correct_user(spark):
    # USR_0030 crosses threshold, USR_0031 does not
    rows = [
        ("T301", "USR_0030", 100.0, "IN", datetime(2024, 1, 1, 10, 0, 0)),
        ("T302", "USR_0030", 100.0, "IN", datetime(2024, 1, 1, 10, 1, 0)),
        ("T303", "USR_0030", 100.0, "IN", datetime(2024, 1, 1, 10, 2, 0)),
        ("T304", "USR_0030", 100.0, "IN", datetime(2024, 1, 1, 10, 3, 0)),
        ("T305", "USR_0030", 100.0, "IN", datetime(2024, 1, 1, 10, 4, 0)),
        ("T306", "USR_0030", 100.0, "IN", datetime(2024, 1, 1, 10, 4, 30)),
        ("T307", "USR_0031", 100.0, "IN", datetime(2024, 1, 1, 10, 0, 0)),
        ("T308", "USR_0031", 100.0, "IN", datetime(2024, 1, 1, 10, 1, 0)),
    ]
    df = create_velocity_df(spark, rows)
    result = apply_velocity_rule(df, threshold=5, window_duration="5 minutes")
    rows_out = result.collect()

    assert len(rows_out) == 1
    assert rows_out[0]["user_id"] == "USR_0030"
