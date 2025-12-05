import json
import logging
import os
import shutil
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from soda.sampler.sample_context import SampleContext
from soda.sampler.sampler import Sampler
from soda.scan import Scan

from common import SparkSessionManager, AirlineDataset, FaaDataset

SODA_SAMPLES_FOLDER = "soda_samples"


class CustomSampler(Sampler):
    """
    Custom sampler that dumps collected samples of failed rows into the local filesystem
    for later display as part of the data quality evaluation report.

    Inspired by the example: https://docs.soda.io/use-case-guides/route-failed-rows#example-script
    See more in the doc: https://docs.soda.io/run-a-scan/failed-row-samples#configure-a-python-custom-sampler
    """

    # Custom serialized for Spark types
    @staticmethod
    def json_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    # Main method to override
    def store_sample(self, sample_context: SampleContext):  # type: ignore
        check_name = sample_context.check_name or sample_context.sample_name
        exceptions_df = self._create_exceptions_df(sample_context, check_name)
        check_file_path = self._get_sample_file_path(check_name)
        os.makedirs(SODA_SAMPLES_FOLDER, exist_ok=True)
        exceptions_df.to_csv(check_file_path, index=False, encoding="utf-8")

    def _create_exceptions_df(
        self, sample_context: SampleContext, check_name: str
    ) -> pd.DataFrame:
        rows = sample_context.sample.get_rows()
        json_data = json.dumps(rows, default=CustomSampler.json_serializer)
        exceptions_df = pd.read_json(json_data)
        exceptions_df.columns = [
            col["name"] for col in sample_context.sample.get_schema().get_dict()
        ]
        # Add column with a check for which row has failed.
        exceptions_df.insert(0, "Failed Check", check_name)
        return exceptions_df

    def _get_sample_file_path(self, check_name: str) -> str:
        check_file_name = f"{check_name.lower().strip().replace(' ', '_')}.csv"
        return f"{SODA_SAMPLES_FOLDER}/{check_file_name}"


def evaluate(spark: SparkSession):
    shutil.rmtree(SODA_SAMPLES_FOLDER, ignore_errors=True)
    prepare_data(spark)
    scan = run_soda_scan(spark)
    log_evaluation_report(scan)


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


def run_soda_scan(spark: SparkSession) -> Scan:
    """Set up and run Soda programmatically for data quality evaluation."""
    scan = Scan()
    scan.add_spark_session(spark, data_source_name="all_flights")

    # Necessary to make `all_flights` data source visible checks yaml file
    scan.set_data_source_name("all_flights")

    scan.add_sodacl_yaml_file(file_path="evaluation_soda_checks.yaml")
    scan.sampler = CustomSampler()

    # Setting data source wide samples limit.
    # See for more details:
    # https://docs.soda.io/run-a-scan/failed-row-samples#customize-failed-row-samples-for-datasets-and-columns
    scan._configuration.samples_limit = 3

    scan.execute()
    return scan


def log_evaluation_report(scan: Scan):
    print("Soda evaluation report:")
    log_scan_report(scan)
    log_samples_report()


def log_samples_report():
    csv_files = list(Path(SODA_SAMPLES_FOLDER).glob("*.csv"))
    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
    print("Samples:")
    print(df.to_string())


def log_scan_report(scan: Scan):
    scan.set_verbose(False)
    logs_text = scan.get_logs_text()
    if logs_text:
        cleaned_logs = "\n".join(
            line.split(" | ", 1)[1] if " | " in line else line
            for line in logs_text.split("\n")
        )
        print(cleaned_logs)
    else:
        print("No logs available.")


def run():
    logging.info("Connecting to Spark session")
    with SparkSessionManager() as spark:
        logging.info("Start Airline dataset evaluation")
        evaluate(spark)


if __name__ == "__main__":
    run()
