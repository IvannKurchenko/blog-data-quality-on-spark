import logging
from datetime import timedelta
from typing import List

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.contexts.workspace_context import WorkspaceContext
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQRule
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from common import AirlineDataset, FaaDataset
from great_expectations.src.constants import STATE_CODES


def evaluate():
    print("Connect to compute")
    spark = DatabricksSession.builder.getOrCreate()

    prepare_data(spark)
    input_df = read_flights(spark)
    all_checks = create_all_checks()

    observer = DQMetricsObserver(name="dq_metrics")
    dq_engine = DQEngine(WorkspaceClient(), observer=observer)

    # See https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply/#programmatic-approach
    valid_and_invalid_df, observation = dq_engine.apply_checks(input_df, all_checks)

    save_results(dq_engine, valid_and_invalid_df)
    print_report(observation, spark, valid_and_invalid_df)
    print_dashboard()


def create_check_pass_percentage_thresholds(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            # The proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.
            ("Flight duplicates", 10),
            # At least 80% of `TailNum` column values can be found in `Federal Aviation Agency Database`
            ("TailNum values can be found in FAA", 20),
            # 90th percentile of `DepDelay` is under 60 minutes;
            ("DepDelay is under 60 minutes", 10)
        ],
        schema=StructType([
            StructField("name", StringType(), True),
            StructField("pass_threshold_percentage", IntegerType(), True)
        ])
    )


def create_all_checks() -> List[DQRule]:
    """
    Create all DQ checks.
    See: https://databrickslabs.github.io/dqx/docs/guide/quality_checks_definition/
    """
    accuracy_checks = create_accuracy_checks()
    completeness_checks = create_completeness_checks()
    consistency_checks = create_consistency_checks()
    credibility_checks = create_credibility_checks()
    currentness_checks = create_currentness_checks()
    reasonableness_checks = create_reasonableness_checks()
    uniqueness_checks = create_uniqueness_checks()

    all_checks = [
        *accuracy_checks,
        *completeness_checks,
        *consistency_checks,
        *credibility_checks,
        *currentness_checks,
        *reasonableness_checks,
        *uniqueness_checks
    ]
    return all_checks


def create_uniqueness_checks() -> List[DQDatasetRule]:
    """
    Uniqueness checks.
    The proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.
    """
    return [
        DQDatasetRule(
            name="Flight duplicates",
            criticality="error",
            check_func=check_funcs.is_unique,
            check_func_kwargs={
                "columns": ["FlightDate", "AirlineId", "TailNum", "OriginAirportID", "DestAirportID"]
            }
        )
    ]


def create_reasonableness_checks() -> List[DQDatasetRule]:
    """
    Reasonableness checks.
    Average speed calculated based on `AirTime` (in minutes) and `Distance` is close to the average cruise
    speed of modern aircraft - 885 KpH.

    90th percentile of `DepDelay` is under 60 minutes;
    """
    speed_column = F.col("Distance") / (F.col("Airtime") / F.lit(60))
    return [
        DQDatasetRule(
            name="Average speed should be greater then 800 KmPh",
            criticality="error",
            check_func=check_funcs.is_aggr_not_less_than,
            check_func_kwargs={
                "aggr_type": "avg",
                "column": speed_column,
                "limit": F.lit(800)
            }
        ),
        DQDatasetRule(
            name="Average speed should be less then 900 KmPh",
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            check_func_kwargs={
                "aggr_type": "avg",
                "column": speed_column,
                "limit": F.lit(900)
            }
        ),
        DQRowRule(
            name="DepDelay is under 60 minutes",
            column="DepDelay",
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            check_func_kwargs={
                "limit": 60
            }
        )
    ]


def create_currentness_checks() -> List[DQRowRule]:
    """
    Currentness / Currency.
    All values in the column `FlightDate` are not older than 2016.
    """
    return [
        DQRowRule(
            name="Flights should not be older then 11 years.",
            column="FlightDate",
            criticality="error",
            check_func=check_funcs.is_data_fresh,
            check_func_kwargs={
                "max_age_minutes": int((11 * timedelta(days=365)).total_seconds() / 60)
            }
        )
    ]


def create_credibility_checks() -> List[DQDatasetRule]:
    """
    Credibility / Accuracy checks.
    At least 80% of `TailNum` column values can be found in `Federal Aviation Agency Database`
    """
    return [
        DQDatasetRule(
            name="TailNum values can be found in FAA",
            check_func=check_funcs.foreign_key,
            check_func_kwargs={
                "columns": ["TailNum"],
                "ref_columns": ["FaaTailNum"],
                "ref_table": "faa_tail_numbers"
            }
        )
    ]


def create_consistency_checks() -> List[DQDatasetRule]:
    """
    Consistency & Integrity checks.
    All values in column `AirlineID` match `Code` in `L_AIRLINE_ID` table
    """
    return [
        DQDatasetRule(
            check_func=check_funcs.foreign_key,
            check_func_kwargs={
                "columns": ["AirlineID"],
                "ref_columns": ["Code"],
                "ref_table": "airline_id"
            }
        )
    ]


def create_completeness_checks() -> List[DQRowRule]:
    """
    Completeness checks.
    All values in columns `FlightDate`, `AirlineID`, `TailNum` are not null.
    """
    return [
        DQRowRule(
            name="FlightDate is null",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="FlightDate",
        ),
        DQRowRule(
            name="AirlineID is null",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="AirlineID",
        ),
        DQRowRule(
            name="TailNum is null",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="TailNum",
        )
    ]


def create_accuracy_checks() -> List[DQRowRule]:
    """
    Accuracy & Validity checks.
    - All values of the ` TailNum ` column are valid "tail number" combinations
    - All values in the column `OriginState` contain valid state abbreviations
    - All rows have `ActualElapsedTime` that is more than `AirTime`.
    """
    return [
        DQRowRule(
            name="Invalid TailNum format",
            criticality="error",
            check_func=check_funcs.regex_match,
            column="TailNum",
            check_func_kwargs={
                "regex": "^N(?:[1-9]\\d{0,4}|[1-9]\\d{0,3}[A-Z]|[1-9]\\d{0,2}[A-Z]{2})$"
            },
        ),
        DQRowRule(
            name="Invalid OriginState value",
            criticality="error",
            check_func=check_funcs.is_in_list,
            column="OriginState",
            check_func_kwargs={
                "allowed": STATE_CODES
            },
        ),
        DQRowRule(
            name="ActualElapsedTime that is more than AirTime",
            check_func=check_funcs.is_not_greater_than,
            column="AirTime",
            criticality="error",
            check_func_kwargs={
                "limit": F.col("ActualElapsedTime")
            }
        )
    ]


def prepare_data(spark: SparkSession):
    """
    Prepare data for DQ checks by storing it to Unity catalog, so to make further work for DQX easier.
    For instance, saving local FAA csv, so remote Spark session (working via Databricks connect) could access it.

    Using `ignore` mode to store it just once for case study purposes.
    """
    airline_dataset = AirlineDataset(spark)
    faa_dataset = FaaDataset(spark)

    flights_df = airline_dataset.on_time_on_time_performance_2016_1_df()
    flights_df.write.mode("ignore").saveAsTable("flights")

    airline_id_df = airline_dataset.l_airline_id_df()
    airline_id_df.write.mode("ignore").saveAsTable("airline_id")

    faa_tail_numbers_df = faa_dataset.all_tail_numbers_df()
    faa_tail_numbers_df.write.mode("ignore").saveAsTable("faa_tail_numbers")


def read_flights(spark: SparkSession) -> DataFrame:
    """
    Read main table for DQ checks.
    """
    input_df = spark.read.table("flights").alias("flights")
    return input_df


def save_results(dq_engine: DQEngine, valid_and_invalid_df):
    quarantine_df = (
        valid_and_invalid_df
        .where((F.size(F.col("_errors")) > 0) | (F.size(F.col("_warnings")) > 0))
    )
    output_df = (
        valid_and_invalid_df
        .where((F.size(F.col("_errors")) == 0) | (F.size(F.col("_warnings")) == 0))
    )
    dq_engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=OutputConfig("dqx_output", mode="overwrite"),
        quarantine_config=OutputConfig("dqx_quarantine", mode="overwrite")
    )


def print_report(observation, spark: SparkSession, valid_and_invalid_df: DataFrame):
    # See: https://databrickslabs.github.io/dqx/docs/guide/summary_metrics/
    metrics = observation.get
    input_row_count = metrics['input_row_count']
    valid_row_count = metrics['valid_row_count']
    error_row_count = metrics['error_row_count']
    warning_row_count = metrics['warning_row_count']

    print(f"Input row count: {input_row_count}")
    print(f"Valid: row count: {valid_row_count}, percentage: {valid_row_count / input_row_count * 100:.2f}%")
    print(f"Error row count: {error_row_count}, percentage: {error_row_count / input_row_count * 100:.2f}%")
    print(f"Warning row count: {warning_row_count}, percentage: {warning_row_count / input_row_count * 100:.2f}%")

    errors_df = (
        valid_and_invalid_df
        .select(F.explode(F.col("_errors")).alias("error"))
        .select(F.expr("error.*"))
        .withColumn("criticality", F.lit("error"))
    )
    errors_df.write.mode("overwrite").saveAsTable("errors")

    warnings_df = (
        valid_and_invalid_df
        .select(F.explode(F.col("_warnings")).alias("warning"))
        .select(F.expr("warning.*"))
        .withColumn("criticality", F.lit("warning"))
    )
    warnings_df.write.mode("overwrite").saveAsTable("warnings")

    check_pass_percentage_thresholds_df = create_check_pass_percentage_thresholds(spark)

    data_quality_checks = (
        spark.table("errors").select("name", "criticality")
        .unionAll(spark.table("warnings").select("name", "criticality"))
    )

    data_quality_checks.show()
    (
        data_quality_checks
        .groupBy("name", "criticality")
        .agg(F.count("*").alias("count"))
        .withColumn("percentage", F.round(F.col("count") / input_row_count * 100, scale=2))
        .join(check_pass_percentage_thresholds_df, on="name", how="left")
        .withColumn("pass_threshold_percentage", F.coalesce(F.col("pass_threshold_percentage"), F.lit(100)))
        .withColumn("passed", F.col("percentage") >= F.col("pass_threshold_percentage"))
        .show()
    )


def print_dashboard():
    ctx = WorkspaceContext(WorkspaceClient())
    dashboards_folder_link = f"{ctx.installation.workspace_link('')}dashboards/"
    print(f"Open a dashboard from the following folder and refresh it: {dashboards_folder_link}")


def run():
    logging.info("Start Airline dataset evaluation")
    evaluate()


if __name__ == "__main__":
    run()
