import pytest
from pytest import approx
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr, sum as spark_sum, mean as spark_mean

from good_eats.directories import test_data

import logging

logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)


# --------------------------------------------------------------------
# SparkSession Fixture
# --------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """
    Provide a session-scoped SparkSession for all PySpark-based tests.
    """
    spark = (
        SparkSession.builder.appName("good_eats_tests").master("local[*]").getOrCreate()
    )

    # Reduce Spark logging verbosity
    spark.sparkContext.setLogLevel("ERROR")

    yield spark
    spark.stop()


# --------------------------------------------------------------------
# PySpark DataFrame Fixture
# --------------------------------------------------------------------
@pytest.fixture
def spark_df(spark):
    """
    Load the synthetic CSV using Spark.
    Schema inference matches pandas automatically because the CSV uses
    numeric columns without quotes and properly quoted text fields.
    """
    path = test_data("fake_good_eats.csv")

    df = spark.read.csv(
        path, header=True, inferSchema=True, multiLine=False, escape='"', quote='"'
    )
    return df


# --------------------------------------------------------------------
# Schema Tests
# --------------------------------------------------------------------


def test_columns_present(spark_df):
    expected = [
        "Agency",
        "Time Period",
        "Food Product Group",
        "Food Product Category",
        "Product Name",
        "Product Type",
        "Origin Detail",
        "Distributor",
        "Vendor",
        "# of Units",
        "Total Weight in lbs",
        "Total Cost",
    ]
    assert spark_df.columns == expected


def test_no_missing_except_health_hospitals(spark_df):
    non_hh = spark_df.filter(col("Agency") != "Health + Hospitals")
    assert non_hh.filter(col("Total Cost").isNull()).count() == 0


# --------------------------------------------------------------------
# Categorical Domain Tests
# --------------------------------------------------------------------


def test_agency_domain(spark_df):
    allowed = {
        "Administration for Childrens Services",
        "Health + Hospitals",
        "Department of Education",
    }

    agencies = {row[0] for row in spark_df.select("Agency").distinct().collect()}
    assert agencies.issubset(allowed)
    assert len(agencies) >= 2


def test_time_period_domain(spark_df):
    allowed = {"2018-2019", "2019-2020", "2020-2021"}

    periods = {row[0] for row in spark_df.select("Time Period").distinct().collect()}
    assert periods.issubset(allowed)
    assert len(periods) >= 2


# --------------------------------------------------------------------
# Quantitative Tests
# --------------------------------------------------------------------


def test_units_weight_cost_positive(spark_df):
    assert spark_df.filter(col("# of Units") <= 0).count() == 0
    assert spark_df.filter(col("Total Weight in lbs") <= 0).count() == 0

    hh = spark_df.filter(col("Agency") == "Health + Hospitals")
    non_hh = spark_df.filter(col("Agency") != "Health + Hospitals")

    # Health + Hospitals must have NULL cost
    assert hh.filter(col("Total Cost").isNotNull()).count() == 0

    # Others must have positive cost
    assert non_hh.filter(col("Total Cost") <= 0).count() == 0
    assert non_hh.filter(col("Total Cost").isNull()).count() == 0


def test_cost_correlated_with_weight(spark_df):
    """
    Correlation ignores null values automatically.
    """
    corr_value = spark_df.select(corr("Total Weight in lbs", "Total Cost")).collect()[
        0
    ][0]
    assert corr_value > 0.3


# --------------------------------------------------------------------
# Diversity Tests
# --------------------------------------------------------------------


def test_multiple_product_groups(spark_df):
    n = spark_df.select("Food Product Group").distinct().count()
    assert n >= 3


def test_multiple_distributors(spark_df):
    n = spark_df.select("Distributor").distinct().count()
    assert n >= 2


# --------------------------------------------------------------------
# Aggregate Tests (Sums and Means)
# --------------------------------------------------------------------


def test_total_units_sum(spark_df):
    total_units = spark_df.agg(spark_sum("# of Units")).collect()[0][0]
    expected = 7205
    assert total_units == expected


def test_total_weight_sum(spark_df):
    total_weight = spark_df.agg(spark_sum("Total Weight in lbs")).collect()[0][0]
    expected = 21475
    assert total_weight == expected


def test_total_cost_sum_non_hh(spark_df):
    non_hh = spark_df.filter(col("Agency") != "Health + Hospitals")
    total_cost = non_hh.agg(spark_sum("Total Cost")).collect()[0][0]
    expected = 36950
    assert total_cost == expected


def test_mean_units(spark_df):
    mean_units = spark_df.agg(spark_mean("# of Units")).collect()[0][0]
    expected = 720.5
    assert mean_units == expected


def test_mean_weight(spark_df):
    mean_weight = spark_df.agg(spark_mean("Total Weight in lbs")).collect()[0][0]
    expected = 2147.5
    assert round(mean_weight, 10) == expected


def test_mean_cost_non_hh(spark_df):
    non_hh = spark_df.filter(col("Agency") != "Health + Hospitals")
    mean_cost = non_hh.agg(spark_mean("Total Cost")).collect()[0][0]
    expected = 6158.333333333333
    assert mean_cost == approx(expected, rel=1e-12)
