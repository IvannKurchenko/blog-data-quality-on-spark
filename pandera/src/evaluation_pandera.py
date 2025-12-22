import json
import logging

import pandera.pyspark as pa
from pandera import Check, DataFrameSchema
from pandera.api.extensions import register_check_method
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import SparkSessionManager, AirlineDataset, FaaDataset
from constants import STATE_CODES

logging.basicConfig(level=logging.INFO)


def evaluate(spark: SparkSession):
    input_df = prepare_data(spark)
    schema = create_validate_schema()
    df_out = schema.validate(input_df, lazy=True)
    df_out_errors = df_out.pandera.errors
    print(json.dumps(dict(df_out_errors), indent=4))


def prepare_data(spark: SparkSession) -> DataFrame:
    airline_dataset = AirlineDataset(spark)
    faa_dataset = FaaDataset(spark)

    # Read dimensional table with airline codes
    airline_id_df = airline_dataset.l_airline_id_df().withColumnRenamed("Code", "AirlineCode")
    # Read FAA tail numbers data set
    faa_tail_numbers_df = faa_dataset.all_tail_numbers_df()
    # Read main flights data set
    on_time_on_time_performance_df = airline_dataset.on_time_on_time_performance_2016_1_df()

    # Prepare data set for validation
    input_df = (
        on_time_on_time_performance_df
        .withColumn("FlightSpeed", F.col("Distance") / (F.col("AirTime") / F.lit(60)))
        .withColumn(
            "FlightCompoundId",
            F.concat_ws(
                "-",
                F.col("FlightDate"),
                F.col("AirlineId"),
                F.col("TailNum"),
                F.col("OriginAirportID"),
                F.col("DestAirportID"),
            )
        )
        .join(
            airline_id_df,
            on=on_time_on_time_performance_df.AirlineID == airline_id_df.AirlineCode,
            how="left",
        )
        .join(
            faa_tail_numbers_df,
            on=on_time_on_time_performance_df.TailNum == faa_tail_numbers_df.FaaTailNum,
            how="left",
        )
    )
    return input_df


def create_validate_schema() -> DataFrameSchema:
    schema = pa.DataFrameSchema(
        {
            "TailNum": create_tail_num_column(),
            "OriginState": create_origin_state_column(),
            "AirTime": create_airtime_column(),
            "FlightDate": create_flight_date_column(),
            "AirlineID": create_airline_id_column(),
            "AirlineCode": create_airline_code_column(),
            "FaaTailNum": create_faa_tail_num_column(),
            "FlightSpeed": create_flight_speed_column(),
            "DepDelay": create_dep_delay_column(),
            "FlightCompoundId": create_flight_compound_id_column(),
        }
    )
    return schema


@register_check_method
def duplicates_percentage(pyspark_obj, *, percentage: int) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    duplicates_percentage = (F.count("*") - F.count_distinct(column_name)) / F.count("*") * 100
    condition = (
            data_frame.select(duplicates_percentage.alias("duplicates"))
            .where(F.col("duplicates") > percentage)
            .count() == 0
    )
    return condition


def create_flight_compound_id_column() -> pa.Column:
    return pa.Column(
        dtype=T.StringType(),
        nullable=False,
        coerce=False,
        required=True,
        name="FlightCompoundId",
        checks=[
            Check(
                check_fn=duplicates_percentage,
                percentage=10,
                element_wise=False,
                error="FlightCompoundId is duplicate percentage",
                n_failure_cases=10,
            )
        ]
    )


@register_check_method
def percentile_within_boundaries(
        pyspark_obj, *, percentile: float, bottom_limit: int, upper_limit: int
) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    column = F.percentile_approx(F.col(pyspark_obj.column_name), F.lit(percentile))
    condition = (
            data_frame.select(column.alias(column_name))
            .where(F.col(column_name) >= F.lit(bottom_limit))
            .where(F.col(column_name) <= F.lit(upper_limit))
            .count() == 0
    )
    return condition


def create_dep_delay_column() -> pa.Column:
    return pa.Column(
        dtype=T.DecimalType(38, 2),
        nullable=False,
        name="DepDelay",
        checks=[
            Check(
                check_fn=percentile_within_boundaries,
                percentile=0.9,
                bottom_limit=0,
                upper_limit=60,
                element_wise=False,
                error="DepDelay is above 60 minutes",
            )
        ],
    )


@register_check_method
def average_within_boundaries(pyspark_obj, *, bottom_limit: int, upper_limit: int) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    column = F.avg(F.col(pyspark_obj.column_name))
    condition = (
            data_frame.select(column.alias(column_name))
            .where(F.col(column_name) >= F.lit(bottom_limit))
            .where(F.col(column_name) <= F.lit(upper_limit))
            .count() == 0
    )
    return condition

def create_flight_speed_column() -> pa.Column:
    return pa.Column(
        dtype=T.DoubleType(),
        nullable=False,
        name="FlightSpeed",
        checks=[
            Check(
                check_fn=average_within_boundaries,
                bottom_limit=800,
                upper_limit=900,
                element_wise=False,
                error="Average FlightSpeed is not within boundaries of 800 Km and 900 Km",
                n_failure_cases=10,
            )
        ],
    )


@register_check_method
def null_values_max_percentage(pyspark_obj, *, percent: int) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    nulls_count = data_frame.where(F.isnull(F.col(column_name))).count()
    total_count = data_frame.count()
    condition = int((nulls_count / total_count) * 100) < percent
    return condition


def create_faa_tail_num_column() -> pa.Column:
    return pa.Column(
        dtype=T.StringType(),
        nullable=True,
        name="FaaTailNum",
        checks=[
            Check(
                check_fn=null_values_max_percentage,
                percent=20,
                element_wise=False,
                name="FaaTailNum has more than 20 percent of null values",
                error="FaaTailNum has more than 20 percent of null values",
                n_failure_cases=10,
            )
        ]
    )


def create_airline_code_column() -> pa.Column:
    return pa.Column(
        dtype=T.IntegerType(),
        nullable=False,
        name="AirlineCode",
    )


def create_airline_id_column() -> pa.Column:
    return pa.Column(
        dtype=T.IntegerType(),
        nullable=False,
        name="AirlineID",
    )


@register_check_method
def max_age_days(pyspark_obj, *, age_days: int) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    condition = (
            data_frame
            .where(F.date_diff(F.now(), F.col(column_name)) > F.lit(age_days))
            .count() == 0
    )
    return condition


def create_flight_date_column() -> pa.Column:
    return pa.Column(
        dtype=T.DateType(),
        nullable=False,
        name="FlightDate",
        checks=[
            Check(
                check_fn=max_age_days,
                age_days=367 * 10,
                element_wise=True,
                name="FlightDate is older then 10 years ago",
                description="ActualElapsedTime that is more than AirTime",
                error="FlightDate is older then 10 years ago",
                n_failure_cases=10,
            )
        ]
    )


@register_check_method
def greater_then_column(pyspark_obj, *, limit) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    condition_col = F.col(pyspark_obj.column_name) > F.col(limit)
    condition = data_frame.filter(~condition_col).count() == 0
    return condition


def create_airtime_column() -> pa.Column:
    return pa.Column(
        dtype=T.DoubleType(),
        nullable=False,
        required=True,
        name="ActualElapsedTime",
        checks=[
            Check(
                check_fn=greater_then_column,
                limit="AirTime",
                element_wise=True,
                name="ActualElapsedTime is more than AirTime",
                description="ActualElapsedTime is more than AirTime",
                error="ActualElapsedTime that is less than AirTime",
                n_failure_cases=10,
            )
        ]
    )


def create_origin_state_column() -> pa.Column:
    return pa.Column(
        dtype=T.StringType(),
        required=True,
        name="OriginState",
        metadata={},
        checks=[pa.Check.isin(STATE_CODES)],
    )


@register_check_method
def matches_regexp(pyspark_obj, *, regexp) -> bool:
    cond = F.regexp(F.col(pyspark_obj.column_name), F.lit(regexp))
    return pyspark_obj.dataframe.filter(~cond).count() == 0


def create_tail_num_column() -> pa.Column:
    return pa.Column(
        dtype=T.StringType(),
        required=True,
        nullable=False,  # With this flag Pandera does null checks for us
        name="TailNum",
        description="Airplane tail number",
        metadata={},
        checks=[
            Check(
                check_fn=matches_regexp,
                element_wise=True,
                name="Invalid TailNum format",
                description="Invalid TailNum format",
                error="Invalid TailNum format",
                n_failure_cases=10,
                regexp="^N(?:[1-9]\\d{0,4}|[1-9]\\d{0,3}[A-Z]|[1-9]\\d{0,2}[A-Z]{2})$",
            )
        ],
    )


def main():
    logging.info("Connecting to Spark session")
    with SparkSessionManager() as spark:
        logging.info("Start Airline dataset evaluation")
        evaluate(spark)


if __name__ == "__main__":
    main()
