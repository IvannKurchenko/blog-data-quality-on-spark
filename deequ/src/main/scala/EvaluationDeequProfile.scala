import com.amazon.deequ.profiles.{ColumnProfilerRunner, ColumnProfiles}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Using}

object EvaluationDeequProfile {

  implicit class ColumnProfilesOps(result: ColumnProfiles) {
    def print(columnName: String): Unit = {
      val profile = result.profiles(columnName)
      println(
        s"""
           |`$columnName` profile:
           |  Profile class: ${profile.getClass.getSimpleName}
           |  Completeness: ${profile.completeness}
           |  Approximate Num DistinctValues: ${profile.approximateNumDistinctValues}
           |  Data type: ${profile.dataType}
           |  Histogram (short): ${profile.histogram.map(_.values.toList.take(3).mkString(";")).getOrElse("<empty>")}
           |""".stripMargin
      )
    }
  }

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
    val flightsDataFrame = airlineDataset.onTimeOnTimePerformance20161Df
    val result = ColumnProfilerRunner()
      .onData(flightsDataFrame)
      .run()

    result.print("AirlineID") // Profiled as regular numeric column
    result.print("DepDelay") // Correctly identified as numeric and profiled
    result.print("OriginState") // string with histogram.
    result.print("FlightDate") // date time is not supported
  }
}
