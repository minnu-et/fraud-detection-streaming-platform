import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.detection.rules import apply_basic_rules, apply_geo_anomaly_rule


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


def test_high_amount_flagged(spark):
    df = create_test_df(spark, [
        ("T001", "USR_0001", 1500.0, "IN"),
    ])
    result = apply_basic_rules(df)
    row = result.collect()[0]
    assert row["fraud_flag"] == True
    assert row["fraud_reason"] == "high_amount"


def test_normal_amount_not_flagged(spark):
    df = create_test_df(spark, [
        ("T002", "USR_0002", 200.0, "IN"),
    ])
    result = apply_basic_rules(df)
    row = result.collect()[0]
    assert row["fraud_flag"] == False
    assert row["fraud_reason"] is None


def test_geo_anomaly_high_risk_country(spark):
    df = create_test_df(spark, [
        ("T003", "USR_0003", 100.0, "NG"),
    ])
    result = apply_geo_anomaly_rule(df)
    row = result.collect()[0]
    assert row["geo_flag"] == True
    assert row["geo_reason"] == "high_risk_country"


def test_geo_anomaly_safe_country(spark):
    df = create_test_df(spark, [
        ("T004", "USR_0004", 100.0, "IN"),
    ])
    result = apply_geo_anomaly_rule(df)
    row = result.collect()[0]
    assert row["geo_flag"] == False
    assert row["geo_reason"] is None
