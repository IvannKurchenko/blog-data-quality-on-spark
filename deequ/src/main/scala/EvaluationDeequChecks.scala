import Constants.StateCodes
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Using}

object EvaluationDeequChecks {

  def main(args: Array[String]): Unit = {
    println("Deequ evaluation starting...")
    val sparkSessionFactory = new SparkSessionFactory
    Using(sparkSessionFactory.createSession)(evaluate) match {
      case Failure(exception) => exception.printStackTrace()
      case Success(_) => println("Evaluation finished successfully")
    }
  }

  def evaluate(spark: SparkSession): Unit = {
    println("Reading main dataframes...")

    val airlineDataset = new AirlineDataset(spark)
    val faaDataset = new FaaDataset(spark)

    val flightsDataFrame = airlineDataset.onTimeOnTimePerformance20161Df.as("flights")
    val airlineDataFrame = airlineDataset.lAirlineIdDf.as("airlines")
    val faaDataFrame = faaDataset.allTailNumbersDf.as("faa")

    val testDataFrame = {
      flightsDataFrame
        .withColumn("Speed", col("Distance") / (col("AirTime") / lit(60)))
        .join(airlineDataFrame, col("flights.AirlineID") === col("airlines.Code"), joinType = "left")
        .join(faaDataFrame, col("flights.TailNum") === col("faa.FaaTailNum"), joinType = "left")
    }

    val allChecks = createAccuracyChecks() ++
      createCompletnessChecks() ++
      createConsistencyChecks() ++
      createCreateCredibilityChecks() ++
      createCurrentnessChecks() ++
      createReasonablenessChecks() ++
      createUniquenessChecks()

    val verificationResult: VerificationResult = {
      VerificationSuite()
        .onData(testDataFrame)
        .addChecks(allChecks)
        .run()
    }

    val resultDataFrame = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
    resultDataFrame.show()
    println(s"Verification result status: ${verificationResult.status}")
  }

  /**
   * Accuracy & Validity checks
   */
  def createAccuracyChecks(): Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "All values of the `TailNum` column are valid 'tail number' combinations")
        .hasPattern("TailNum", """^N(?:[1-9]\\d{0,4}|[1-9]\\d{0,3}[A-Z]|[1-9]\\d{0,2}[A-Z]{2})$""".r),

      Check(CheckLevel.Error, "All values in the column `OriginState` contain valid state abbreviations")
        .isContainedIn("OriginState", StateCodes),

      Check(CheckLevel.Error, "All rows have `ActualElapsedTime` that is more than `AirTime`")
        .isGreaterThan("ActualElapsedTime", "AirTime")
    )
  }

  /**
   * Completeness checks.
   */
  def createCompletnessChecks() = {
    Seq(
      Check(CheckLevel.Error, "All values in columns `FlightDate`, `AirlineID`, `TailNum` are not null.")
        .areComplete(Seq("FlightDate", "AirlineID", "TailNum"))
    )
  }

  /**
   * Consistency checks
   */
  def createConsistencyChecks(): Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "All values in column `AirlineID` match `Code` in `L_AIRLINE_ID` table")
        .areComplete(Seq("Code"))
    )
  }

  /**
   * Credibility / Accuracy checks.
   */
  def createCreateCredibilityChecks(): Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "At least 80% of `TailNum` column values can be found in `Federal Aviation Agency Database`")
        .areComplete(Seq("Code"))
    )
  }

  /**
   * Currentness / Currency.
   */
  def createCurrentnessChecks(): Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "All values in the column `FlightDate` are not older than 2016.")
        .satisfies("`FlightDate` > to_date(2016-01-01)", "Flight Date is outdated")
    )
  }

  /**
   * Reasonableness checks.
   * Average speed calculated based on `AirTime` (in minutes) and `Distance` is close to the average cruise
   * speed of modern aircraft - 885 KpH.
   *
   * 90th percentile of `DepDelay` is under 60 minutes;
   */

  def createReasonablenessChecks(): Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "Average speed is close 885 KpH.")
        .hasMean("Speed", meanSpeed => 870.0d <= meanSpeed && meanSpeed <= 900.0d),

      Check(CheckLevel.Error, "90th percentile of `DepDelay` is under 60 minutes;")
        .hasApproxQuantile("DepDelay", 0.9, _ <= 60)
    )
  }

  /**
   * Uniqueness checks.
   * The proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.
   */
  def createUniquenessChecks() : Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "The proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.")
        .hasUniqueness(Seq("FlightDate", "AirlineId", "TailNum", "OriginAirportID", "DestAirportID"), _ > 0.9)
    )
  }
}
