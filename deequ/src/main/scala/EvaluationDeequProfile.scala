import com.amazon.deequ.profiles.{ColumnProfile, ColumnProfilerRunner, ColumnProfiles}
import org.apache.spark.sql.SparkSession

object EvaluationDeequProfile extends EvaluationApp {

  /**
   * Tiny extension over profile result to pretty print results concisely.
   */
  implicit class ColumnProfilesOps(result: ColumnProfiles) {
    def print(columnName: String): Unit = {
      val profile: ColumnProfile = result.profiles(columnName)
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

  override def evaluate(spark: SparkSession): Unit = {
    val airlineDataset = new AirlineDataset(spark)
    val flightsDataFrame = airlineDataset.onTimeOnTimePerformance20161Df

    // All profiling is executed in the snippet
    val result = ColumnProfilerRunner()
      .onData(flightsDataFrame)
      .run()

    result.print("AirlineID")
    result.print("DepDelay")
    result.print("OriginState")
    result.print("FlightDate")
  }
}
