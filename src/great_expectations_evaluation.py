import logging

import great_expectations as gx
from great_expectations import ExpectationSuite
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.expectations import ExpectColumnDistinctValuesToBeInSet, ExpectColumnValuesToMatchRegex, \
    ExpectColumnPairValuesAToBeGreaterThanB, ExpectColumnValuesToNotBeNull, \
    ExpectColumnPairValuesToBeEqual, ExpectColumnMeanToBeBetween, ExpectColumnQuantileValuesToBeBetween, \
    ExpectColumnProportionOfUniqueValuesToBeBetween
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common import SparkSessionManager, AirlineDataset, FaaDataset
from src.constants import STATE_CODES

logging.basicConfig(level=logging.INFO)


def evaluate(spark: SparkSession):
    airline_dataset = AirlineDataset(spark)
    faa_dataset = FaaDataset(spark)

    airline_id_df = (
        airline_dataset
        .l_airline_id_df()
        .select('Code')
        .withColumnRenamed('Code', 'AirlineCode')
    )

    on_time_on_time_performance_df = (
        airline_dataset
        .on_time_on_time_performance_2016_1_df()
    )

    master_df = faa_dataset.master_df()
    dereg_df = faa_dataset.dereg_df()

    tail_num_column = F.concat(F.lit('N'), F.col('N-NUMBER')).alias('FaaTailNum')
    faa_tail_numbers = (
        master_df
        .select(tail_num_column)
        .union(dereg_df.select(tail_num_column))
        .distinct()
    )

    test_on_time_on_time_performance_df = (
        on_time_on_time_performance_df
        .withColumn('FlightSpeed', F.col('Distance') / (F.col('AirTime') / F.lit(60)))
        .withColumn(
            'FlightCompoundId',
            F.concat(
                F.col('FlightDate'),
                F.lit('-'),
                F.col('AirlineId'),
                F.lit('-'),
                F.col('TailNum'),
                F.lit('-'),
                F.col('OriginAirportID'),
                F.lit('-'),
                F.col('DestAirportID')
            )
        )
        .join(airline_id_df, on=on_time_on_time_performance_df.AirlineID == airline_id_df.AirlineCode, how='left')
        .join(faa_tail_numbers, on=on_time_on_time_performance_df.TailNum == faa_tail_numbers.FaaTailNum, how='left')
    )

    context = gx.get_context()

    data_source_name = "airline"
    data_source = context.data_sources.add_or_update_spark(name=data_source_name)

    data_asset_name = "flights"
    assets = data_source.get_assets_as_dict()
    if data_asset_name not in assets:
        logging.info(f"Creating data asset: `{data_asset_name}`")
        data_source.add_dataframe_asset(name=data_asset_name)

    data_asset = (
        context.data_sources
        .get(data_source_name)
        .get_asset(data_asset_name)
    )

    batch_definition_name = "airline_batch_definition"
    data_asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )

    batch_definition = (
        context
        .data_sources.get(data_source_name)
        .get_asset(data_asset_name)
        .get_batch_definition(batch_definition_name)
    )

    batch_parameters = {"dataframe": test_on_time_on_time_performance_df}

    #
    # Accuracy & Validity
    #

    tail_num_expectation = ExpectColumnValuesToMatchRegex(
        column="TailNum",
        regex="^N(?:[1-9]\d{0,4}|[1-9]\d{0,3}[A-Z]|[1-9]\d{0,2}[A-Z]{2})$"
    )

    origin_state_expectation = ExpectColumnDistinctValuesToBeInSet(
        column="OriginState",
        value_set=STATE_CODES
    )

    elapsed_time_greater_then_air_time = ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="ActualElapsedTime",
        column_B="AirTime",
        or_equal=False
    )

    #
    # Completeness
    #

    # `FlightDate`, `AirlineID`, `TailNum` are not null.
    flight_date_not_null_expectation = ExpectColumnValuesToNotBeNull(
        column="FlightDate",
    )

    airline_id_not_null_expectation = ExpectColumnValuesToNotBeNull(
        column="AirlineID"
    )

    tail_num_not_null_expectation = ExpectColumnValuesToNotBeNull(column="TailNum")

    #
    # Consistency & Integrity
    #

    # Alternatives:
    # ExpectColumnValuesToBeInSet - but that requires airline codes to load in memory
    # ExpectQueryResultsToMatchComparison - also possible in
    airline_id_expectation = ExpectColumnPairValuesToBeEqual(
        column_A="AirlineID",
        column_B="AirlineCode"
    )

    #
    # Credibility / Accuracy
    #
    airline_id_expectation = ExpectColumnPairValuesToBeEqual(
        column_A="TailNum",
        column_B="FaaTailNum"
    )

    #
    # Currentnes / Currency
    #

    # TODO!

    #
    # Reasonableness
    #

    flight_speed_expectation = ExpectColumnMeanToBeBetween(
        column='FlightSpeed',
        min_value=870,
        max_value=900
    )

    dep_delay_expectation = ExpectColumnQuantileValuesToBeBetween(
        column="DepDelay",
        quantile_ranges={
            "quantiles": [0.9],
            "value_ranges": [[0, 60]]
        }
    )

    #
    # Uniqueness
    #
    flight_compound_id_expectation = ExpectColumnProportionOfUniqueValuesToBeBetween(
        column='FlightCompoundId',
        min_value=0.8,
        max_value=1
    )

    suite = ExpectationSuite(name="airline_expectation_suite")
    suite.add_expectation(tail_num_expectation)
    suite.add_expectation(origin_state_expectation)
    suite.add_expectation(elapsed_time_greater_then_air_time)
    suite.add_expectation(flight_date_not_null_expectation)
    suite.add_expectation(airline_id_not_null_expectation)
    suite.add_expectation(tail_num_not_null_expectation)
    suite.add_expectation(airline_id_expectation)
    suite.add_expectation(flight_speed_expectation)
    suite.add_expectation(dep_delay_expectation)
    suite.add_expectation(flight_compound_id_expectation)

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results: ExpectationSuiteValidationResult = batch.validate(suite)
    print(validation_results)
    # logging.info(
    #     f"""
    #     Validation results:
    #     Success: {validation_results.success}
    #     Statistics: {validation_results.statistics}
    #     """
    # )


def run():
    logging.info('Connecting to Spark session')
    with SparkSessionManager() as spark:
        logging.info('Start Airline dataset evaluation')
        evaluate(spark)


if __name__ == "__main__":
    run()
