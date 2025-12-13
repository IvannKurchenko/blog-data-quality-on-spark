from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

class SparkSessionManager:
    def __init__(self):
        self.spark = None

    def get_session(self) -> SparkSession:
        """Create and return a SparkSession if not already created."""
        if self.spark is None:
            self.spark = (
                SparkSession.builder.appName("MariaDB Connection Example")
                .master("local[*]")
                .config("spark.jars", "mariadb-java-client-3.5.6.jar")
                .getOrCreate()
            )
        return self.spark

    def __enter__(self) -> SparkSession:
        """Create and return a SparkSession configured for MariaDB connection."""
        return self.get_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop the SparkSession."""
        if self.spark:
            self.spark.stop()
            self.spark = None


def read_table(spark: SparkSession, name: str) -> DataFrame:
    """Read a table from MariaDB and return as DataFrame."""
    url = "jdbc:mysql://relational.fel.cvut.cz:3306/Airline?permitMysqlScheme"
    properties = {
        "user": "guest",
        "password": "ctu-relational",
        "driver": "org.mariadb.jdbc.Driver",
    }
    return spark.read.jdbc(url=url, table=name, properties=properties)


class AirlineDataset:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def l_airline_id_df(self) -> DataFrame:
        return read_table(self.spark, "L_AIRLINE_ID")

    def l_airport_df(self) -> DataFrame:
        return read_table(self.spark, "L_AIRPORT")

    def l_airport_id_df(self) -> DataFrame:
        return read_table(self.spark, "L_AIRPORT_ID")

    def l_airport_seq_id_df(self) -> DataFrame:
        return read_table(self.spark, "L_AIRPORT_SEQ_ID")

    def l_cancellation_df(self) -> DataFrame:
        return read_table(self.spark, "L_CANCELLATION")

    def l_city_market_id_df(self) -> DataFrame:
        return read_table(self.spark, "L_CITY_MARKET_ID")

    def l_deparrblk_df(self) -> DataFrame:
        return read_table(self.spark, "L_DEPARRBLK")

    def l_distance_group_250_df(self) -> DataFrame:
        return read_table(self.spark, "L_DISTANCE_GROUP_250")

    def l_diversions_df(self) -> DataFrame:
        return read_table(self.spark, "L_DIVERSIONS")

    def l_months_df(self) -> DataFrame:
        return read_table(self.spark, "L_MONTHS")

    def l_ontime_delay_groups_df(self) -> DataFrame:
        return read_table(self.spark, "L_ONTIME_DELAY_GROUPS")

    def l_quarters_df(self) -> DataFrame:
        return read_table(self.spark, "L_QUARTERS")

    def l_state_abr_aviation_df(self) -> DataFrame:
        return read_table(self.spark, "L_STATE_ABR_AVIATION")

    def l_state_fips_df(self) -> DataFrame:
        return read_table(self.spark, "L_STATE_FIPS")

    def l_unique_carriers_df(self) -> DataFrame:
        return read_table(self.spark, "L_UNIQUE_CARRIERS")

    def l_weekdays_df(self) -> DataFrame:
        return read_table(self.spark, "L_WEEKDAYS")

    def l_world_area_codes_df(self) -> DataFrame:
        return read_table(self.spark, "L_WORLD_AREA_CODES")

    def l_yesno_resp_df(self) -> DataFrame:
        return read_table(self.spark, "L_YESNO_RESP")

    def on_time_on_time_performance_2016_1_df(self) -> DataFrame:
        return read_table(self.spark, "On_Time_On_Time_Performance_2016_1")


class FaaDataset:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def master_df(self) -> DataFrame:
        return self.spark.read.csv("data_faa/MASTER.txt", header=True, inferSchema=True)

    def dereg_df(self) -> DataFrame:
        return self.spark.read.csv("data_faa/DEREG.txt", header=True, inferSchema=True)

    def all_tail_numbers_df(self) -> DataFrame:
        master_df = self.master_df()
        dereg_df = self.dereg_df()

        tail_num_column = F.concat(F.lit('N'), F.col('N-NUMBER')).alias('FaaTailNum')
        faa_tail_numbers = (
            master_df
            .select(tail_num_column)
            .union(dereg_df.select(tail_num_column))
            .distinct()
        )

        return faa_tail_numbers
