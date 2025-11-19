import logging

import great_expectations as gx
from great_expectations import ExpectationSuite
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.core.batch import Batch
from great_expectations.expectations import ExpectColumnDistinctValuesToBeInSet, ExpectColumnValuesToMatchRegex, \
    ExpectColumnPairValuesAToBeGreaterThanB, ExpectColumnValuesToNotBeNull, \
    ExpectColumnPairValuesToBeEqual, ExpectColumnMeanToBeBetween, ExpectColumnQuantileValuesToBeBetween, \
    ExpectColumnProportionOfUniqueValuesToBeBetween, ExpectColumnMinToBeBetween, ExpectCompoundColumnsToBeUnique
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.common import SparkSessionManager, AirlineDataset, FaaDataset
from src.constants import STATE_CODES

logging.basicConfig(level=logging.INFO)


def evaluate(spark: SparkSession):
    test_on_time_on_time_performance_df = prepare_data(spark)
    batch = prepare_greate_expectations_batch(test_on_time_on_time_performance_df)
    suite = prepare_test_suite()
    validation_results: ExpectationSuiteValidationResult = batch.validate(suite)
    print_validation_result(validation_results)


def prepare_test_suite() -> ExpectationSuite:
    """
    Prepare a test suite for evaluating expectations.
    """
    tail_num_syntactic_validity_expectation = ExpectColumnValuesToMatchRegex(
        column="TailNum",
        regex="^N(?:[1-9]\d{0,4}|[1-9]\d{0,3}[A-Z]|[1-9]\d{0,2}[A-Z]{2})$"
    )

    origin_state_semantic_validity_expectation = ExpectColumnDistinctValuesToBeInSet(
        column="OriginState",
        value_set=STATE_CODES
    )

    elapsed_time_greater_then_air_timey_expectation = ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="ActualElapsedTime",
        column_B="AirTime",
        or_equal=False
    )

    flight_date_not_null_expectation = ExpectColumnValuesToNotBeNull(column="FlightDate")
    airline_id_not_null_expectation = ExpectColumnValuesToNotBeNull(column="AirlineID")
    tail_num_not_null_expectation = ExpectColumnValuesToNotBeNull(column="TailNum")

    airline_id_expectation = ExpectColumnPairValuesToBeEqual(
        column_A="AirlineID",
        column_B="AirlineCode"
    )

    tail_num_faa_validity_expectation = ExpectColumnPairValuesToBeEqual(
        column_A="TailNum",
        column_B="FaaTailNum"
    )

    flight_date_expectation = ExpectColumnMinToBeBetween(
        column="FlightDate",
        min_value="2016-01-01",
    )

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

    flight_compound_id_expectation = ExpectCompoundColumnsToBeUnique(
        column_list=['FlightDate', 'AirlineId', 'TailNum', 'OriginAirportID', 'DestAirportID'],
        mostly=0.9,
    )

    suite = ExpectationSuite(name="airline_expectation_suite")
    suite.add_expectation(tail_num_syntactic_validity_expectation)
    suite.add_expectation(origin_state_semantic_validity_expectation)
    suite.add_expectation(elapsed_time_greater_then_air_timey_expectation)

    suite.add_expectation(flight_date_not_null_expectation)
    suite.add_expectation(airline_id_not_null_expectation)
    suite.add_expectation(tail_num_not_null_expectation)

    suite.add_expectation(flight_date_expectation)

    suite.add_expectation(airline_id_expectation)

    suite.add_expectation(tail_num_faa_validity_expectation)

    suite.add_expectation(flight_speed_expectation)
    suite.add_expectation(dep_delay_expectation)
    suite.add_expectation(flight_compound_id_expectation)
    return suite


def prepare_data(spark: SparkSession) -> DataFrame:
    """
    Prepare data for evaluation,
    """
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

    faa_tail_numbers_df = faa_dataset.all_tail_numbers_df()

    test_on_time_on_time_performance_df = (
        on_time_on_time_performance_df
        .withColumn('FlightSpeed', F.col('Distance') / (F.col('AirTime') / F.lit(60)))
        .join(airline_id_df, on=on_time_on_time_performance_df.AirlineID == airline_id_df.AirlineCode, how='left')
        .join(faa_tail_numbers_df, on=on_time_on_time_performance_df.TailNum == faa_tail_numbers_df.FaaTailNum, how='left')
    )
    return test_on_time_on_time_performance_df

def prepare_greate_expectations_batch(data_frame: DataFrame) -> Batch:
    """
    Prepare greate expectations common infrastructure to start expectations evaluation.
    """
    context = gx.get_context()

    data_source_name = "airline"
    logging.info(f"Creating data source: `{data_source_name}`")
    data_source = context.data_sources.add_spark(name=data_source_name)

    data_asset_name = "flights"
    logging.info(f"Creating data asset: `{data_asset_name}`")
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

    batch_definition_name = "airline_batch_definition"
    logging.info(f"Creating batch definition: `{batch_definition_name}`")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

    batch_parameters = {"dataframe": data_frame}
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    return batch


def print_validation_result(result: ExpectationSuiteValidationResult):
    status = "✓ PASSED" if result.success else "✗ FAILED"
    print(f"\n{status} - {result.suite_name}")

    if result.statistics:
        successful = result.statistics.get('successful_expectations', 0)
        evaluated = result.statistics.get('evaluated_expectations', 0)
        success_pct = result.statistics.get('success_percent', 0)
        print(f"Results: {successful}/{evaluated} passed ({success_pct:.1f}%)")

    failed_results = [r for r in result.results if not r.success]
    if failed_results:
        print("\nFailed Expectations:")
        for exp_result in failed_results:
            exp_type = exp_result.expectation_config.type
            unexpected_pct = exp_result.result.get('unexpected_percent', 0)
            print(f"  • {exp_type} ({unexpected_pct:.2f}% unexpected)")


def run():
    logging.info('Connecting to Spark session')
    with SparkSessionManager() as spark:
        logging.info('Start Airline dataset evaluation')
        evaluate(spark)


if __name__ == "__main__":
    run()
