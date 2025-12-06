import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Using}

object EvaluationDeequ {

  def main(args: Array[String]): Unit = {
    println("Deequ evaluation starting...")
    val sparkSessionFactory = new SparkSessionFactory
    Using(sparkSessionFactory.createSession)(evaluate) match {
      case Failure(exception) => exception.printStackTrace()
      case Success(_) => println("Evaluation finished successfully")
    }
  }

  def evaluate(spark: SparkSession): Unit = {
    import spark.implicits._

    println("Reading main dataframes...")

    val airlineDataset = new AirlineDataset(spark)
    val faaDataset = new FaaDataset(spark)

    val flightsDataFrame = airlineDataset
      .onTimeOnTimePerformance20161Df
      .withColumn("CompositeId", concat_ws("-", col("FlightDate"), col("AirlineID"), col("TailNum"), col("OriginAirportID"), col("DestAirportID")))
    val airlineIdDataFrame = airlineDataset.lAirlineIdDf.select("Code").withColumnRenamed("Code", "AirlineCode")
    val faaTailNumbersDataFrame = faaDataset.allTailNumbersDf

    println(s"On Time Performance DF size: ${flightsDataFrame.count()}")
    println(s"Airline ID DF size: ${airlineIdDataFrame.count()}")
    println(s"FAA Tail Numbers DF size: ${faaTailNumbersDataFrame.count()}")
    println("Dataframes loaded successfully.")

    val analysisResult: AnalyzerContext = {
      AnalysisRunner
        // data to run the analysis on
        .onData(flightsDataFrame)
        // define analyzers that compute metrics
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("FlightDate"))
        .addAnalyzer(Completeness("AirlineID"))
        .addAnalyzer(Completeness("TailNum"))
        .addAnalyzer(Distinctness("CompositeId"))
        .addAnalyzer(Compliance("TailNumValid", "regexp(TailNum, '^N(?:[1-9]\\\\d{0,4}|[1-9]\\\\d{0,3}[A-Z]|[1-9]\\\\d{0,2}[A-Z]{2})$')"))
        .addAnalyzer(ApproxQuantile("DepDelay", 0.9))
        //      .addAnalyzer(Correlation("total_votes", "star_rating"))
        //      .addAnalyzer(Correlation("total_votes", "helpful_votes"))
        // compute metrics
        .run()
    }
    val metricsDataFrame = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    metricsDataFrame.orderBy(col("entity"), col("instance")).show()

    val verificationResult: VerificationResult = {
      VerificationSuite()
        // data to run the verification on
        .onData(flightsDataFrame)
        // define a data quality check
        .addCheck(
          Check(CheckLevel.Error, "Review Check")
            .isUnique("CompositeId")
        )
        .run()

      //          .hasSize(_ >= 3000000) // at least 3 million rows
      //          .hasMin("star_rating", _ == 1.0) // min is 1.0
      //          .hasMax("star_rating", _ == 5.0) // max is 5.0
      //          .isComplete("review_id") // should never be NULL
      //          .isUnique("review_id") // should not contain duplicates
      //.isComplete("marketplace") // should never be NULL
      // contains only the listed values
      //          .isContainedIn("marketplace", Array("US", "UK", "DE", "JP", "FR"))
      //          .isNonNegative("year")) // should not contain negative values
      // compute metrics and verify check conditions

    }

    // convert check results to a Spark data frame
    val resultDataFrame = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
    resultDataFrame.show(truncate=false)

    val suggestionResult = { ConstraintSuggestionRunner()
      // data to suggest constraints for
      .onData(flightsDataFrame)
      // default set of rules for constraint suggestion
      .addConstraintRules(Rules.DEFAULT)
      // run data profiling and constraint suggestion
      .run()
    }

    // We can now investigate the constraints that Deequ suggested.
    val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
      case (column, suggestions) =>
        suggestions.map { constraint =>
          (column, constraint.description, constraint.codeForConstraint)
        }
    }.toSeq.toDS()

    suggestionDataFrame.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("suggestions.csv")
  }
}