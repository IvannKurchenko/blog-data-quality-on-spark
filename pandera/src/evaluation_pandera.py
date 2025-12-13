import logging

from pyspark.sql import SparkSession

from common import SparkSessionManager, AirlineDataset

logging.basicConfig(level=logging.INFO)

def evaluate(spark: SparkSession):
    airline_dataset = AirlineDataset(spark)
    df = airline_dataset.on_time_on_time_performance_2016_1_df()
    print(f"Loaded dataframe with {df.count()} rows")
    df.show(5)

def main():
    logging.info('Connecting to Spark session')
    with SparkSessionManager() as spark:
        logging.info('Start Airline dataset evaluation')
        evaluate(spark)


if __name__ == "__main__":
    main()
