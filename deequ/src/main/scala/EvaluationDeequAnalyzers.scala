import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object EvaluationDeequAnalyzers extends EvaluationApp {
  def evaluate(spark: SparkSession): Unit = {
    val airlineDataset = new AirlineDataset(spark)
    val faaDataset = new FaaDataset(spark)

    val flightsDataFrame = airlineDataset.onTimeOnTimePerformance20161Df.as("flights")
    val airlineDataFrame = airlineDataset.lAirlineIdDf.as("airlines")
    val faaDataFrame = faaDataset.allTailNumbersDf.as("faa")

    // Prepare data frame to analyze with some pre-calculations
    val testDataFrame = {
      flightsDataFrame
        .withColumn("Speed", col("Distance") / (col("AirTime") / lit(60)))
        .join(airlineDataFrame, col("flights.AirlineID") === col("airlines.Code"), joinType = "left")
        .join(faaDataFrame, col("flights.TailNum") === col("faa.FaaTailNum"), joinType = "left")
    }

    val analysisResult: AnalyzerContext = {
      AnalysisRunner
        .onData(testDataFrame)
        .addAnalyzer(Size())

        // Analyze columns for "Accuracy & Validity checks"
        .addAnalyzer(PatternMatch("TailNumValid", "^N(?:[1-9]\\\\d{0,4}|[1-9]\\\\d{0,3}[A-Z]|[1-9]\\\\d{0,2}[A-Z]{2})$".r))
        .addAnalyzer(CountDistinct("OriginState"))

        // Analyze columns for "Completeness checks"
        .addAnalyzer(Completeness("FlightDate"))
        .addAnalyzer(Completeness("AirlineID"))
        .addAnalyzer(Completeness("TailNum"))

        // Analyze columns for "Consistency checks"
        .addAnalyzer(Completeness("Code"))

        // Analyze columns for "Currentness / Currency"
        .addAnalyzer(Compliance("FlightDate", "FlightDate > to_date('2016-01-01')"))

        // Analyze columns for "Reasonableness checks"
        .addAnalyzer(Mean("Speed"))
        .addAnalyzer(ApproxQuantile("DepDelay", 0.9))

        // Analyze columns for "Uniqueness checks"
        .addAnalyzer(Distinctness(Seq("FlightDate", "AirlineId", "TailNum", "OriginAirportID", "DestAirportID")))
        .run()
    }

    val metricsDataFrame = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    metricsDataFrame.orderBy(col("entity"), col("instance")).show(truncate = false)
  }
}
