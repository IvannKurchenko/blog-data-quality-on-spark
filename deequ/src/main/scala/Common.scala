import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

class SparkSessionFactory {

  def createSession: SparkSession = {
    val spark = SparkSession.builder
      .appName("MariaDB Connection Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    spark
  }
}

object Common {
  def readTable(spark: SparkSession, name: String): DataFrame = {
    val url = "jdbc:mysql://relational.fel.cvut.cz:3306/Airline?permitMysqlScheme"
    val properties = new Properties()
    properties.put("user", "guest")
    properties.put("password", "ctu-relational")
    properties.put("driver", "org.mariadb.jdbc.Driver")
    spark.read.jdbc(url, name, properties)
  }
}

class AirlineDataset(spark: SparkSession) {
  def lAirlineIdDf: DataFrame = Common.readTable(spark, "L_AIRLINE_ID")

  def lAirportDf: DataFrame = Common.readTable(spark, "L_AIRPORT")

  def lAirportIdDf: DataFrame = Common.readTable(spark, "L_AIRPORT_ID")

  def lAirportSeqIdDf: DataFrame = Common.readTable(spark, "L_AIRPORT_SEQ_ID")

  def lCancellationDf: DataFrame = Common.readTable(spark, "L_CANCELLATION")

  def lCityMarketIdDf: DataFrame = Common.readTable(spark, "L_CITY_MARKET_ID")

  def lDeparrblkDf: DataFrame = Common.readTable(spark, "L_DEPARRBLK")

  def lDistanceGroup250Df: DataFrame = Common.readTable(spark, "L_DISTANCE_GROUP_250")

  def lDiversionsDf: DataFrame = Common.readTable(spark, "L_DIVERSIONS")

  def lMonthsDf: DataFrame = Common.readTable(spark, "L_MONTHS")

  def lOntimeDelayGroupsDf: DataFrame = Common.readTable(spark, "L_ONTIME_DELAY_GROUPS")

  def lQuartersDf: DataFrame = Common.readTable(spark, "L_QUARTERS")

  def lStateAbrAviationDf: DataFrame = Common.readTable(spark, "L_STATE_ABR_AVIATION")

  def lStateFipsDf: DataFrame = Common.readTable(spark, "L_STATE_FIPS")

  def lUniqueCarriersDf: DataFrame = Common.readTable(spark, "L_UNIQUE_CARRIERS")

  def lWeekdaysDf: DataFrame = Common.readTable(spark, "L_WEEKDAYS")

  def lWorldAreaCodesDf: DataFrame = Common.readTable(spark, "L_WORLD_AREA_CODES")

  def lYesnoRespDf: DataFrame = Common.readTable(spark, "L_YESNO_RESP")

  def onTimeOnTimePerformance20161Df: DataFrame = Common.readTable(spark, "On_Time_On_Time_Performance_2016_1")
}

class FaaDataset(spark: SparkSession) {
  def masterDf: DataFrame = {
    spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv("data_faa/MASTER.txt")
  }

  def deregDf: DataFrame = {
    spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv("data_faa/DEREG.txt")
  }

  def allTailNumbersDf: DataFrame = {
    val masterDf = this.masterDf
    val deregDf = this.deregDf

    val tailNumColumn = concat(lit("N"), col("N-NUMBER")).as("FaaTailNum")
    val faaTailNumbers = masterDf.select(tailNumColumn)
      .union(deregDf.select(tailNumColumn))
      .distinct()

    faaTailNumbers
  }
}