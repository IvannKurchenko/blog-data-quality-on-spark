import json
import logging
import os
import shutil
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from soda.sampler.sample_context import SampleContext
from soda.sampler.sample_ref import SampleRef
from soda.sampler.sampler import Sampler
from soda.scan import Scan

from src.common import SparkSessionManager, AirlineDataset, FaaDataset

logging.basicConfig(level=logging.INFO)

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


# Inspired by the example: https://docs.soda.io/use-case-guides/route-failed-rows#example-script
# See more in the doc: https://docs.soda.io/run-a-scan/failed-row-samples#configure-a-python-custom-sampler
class CustomSampler(Sampler):
    def store_sample(self, sample_context: SampleContext):
        rows = sample_context.sample.get_rows()
        json_data = json.dumps(rows, default=json_serializer) # Convert failed rows to JSON
        exceptions_df = pd.read_json(json_data) #create dataframe with failed rows
        # Define exceptions dataframe
        exceptions_schema = sample_context.sample.get_schema().get_dict()
        exception_df_schema = []
        for n in exceptions_schema:
            exception_df_schema.append(n["name"])
        exceptions_df.columns = exception_df_schema
        check_name = sample_context.check_name
        exceptions_df.insert(0, 'Failed Check', check_name)
        check_file_name = sample_context.check_name.lower().strip().replace(" ", "_") + ".csv"
        check_file_path = "soda_samples/" + check_file_name
        os.makedirs("soda_samples", exist_ok=True)
        exceptions_df.to_csv(check_file_path, sep=",", index=False, encoding="utf-8")




def evaluate(spark: SparkSession):
    shutil.rmtree('soda_samples', ignore_errors=True)
    prepare_data(spark)
    scan = Scan()
    scan.add_spark_session(spark, data_source_name="all_flights")
    scan.set_data_source_name("all_flights")
    scan.add_sodacl_yaml_file(file_path="evaluation_soda_checks.yaml")
    scan.sampler = CustomSampler()
    # See - https://docs.soda.io/run-a-scan/failed-row-samples#customize-failed-row-samples-for-datasets-and-columns
    scan._configuration.samples_limit = 20

    scan.execute()
    #print(scan.get_scan_results())

    # Inspect the scan logs
    #######################
    scan.set_verbose(False) # Set after execution to skip details
    print(scan.get_logs_text())
    scan.get_scan_results()

    # Get all CSV files in the folder
    csv_files = Path('soda_samples').glob('*.csv')

    # Read and concatenate all CSVs
    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

    # Print to output
    print("Samples:")
    with pd.option_context('display.max_columns', None,
                           'display.width', None,
                           'display.max_colwidth', None,
                           'display.max_rows', None):
        print(df)


def prepare_data(spark: SparkSession) -> None:
    """
    To make Spark Data Frames visible for Soda, first we do need to create temporary views.
    The same names should be used in checks configurations.
    """
    airline_dataset = AirlineDataset(spark)
    faa_dataset = FaaDataset(spark)

    flights_df = airline_dataset.on_time_on_time_performance_2016_1_df()
    airline_id_df = airline_dataset.l_airline_id_df()
    faa_tail_numbers_df = faa_dataset.all_tail_numbers_df()

    flights_df.createOrReplaceTempView("flights")
    airline_id_df.createOrReplaceTempView("airline_id")
    faa_tail_numbers_df.createOrReplaceTempView("faa_tail_numbers")

def run():
    logging.info('Connecting to Spark session')
    with SparkSessionManager() as spark:
        logging.info('Start Airline dataset evaluation')
        evaluate(spark)


if __name__ == "__main__":
    run()
