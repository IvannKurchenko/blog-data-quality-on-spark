import com.amazon.deequ.suggestions.{ConstraintSuggestionResult, ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.SparkSession

object EvaluationDequeSuggestions extends EvaluationApp {
  /**
   * Tiny extension over profile result to pretty print results concisely.
   */
  implicit class ConstraintSuggestionResultOps(result: ConstraintSuggestionResult) {
    def printSuggestions(column: String): Unit = {
      result.constraintSuggestions.get(column).foreach { suggestions =>
        println(f"$column suggestions: ")
        suggestions.foreach { suggestion =>
          val description = suggestion.description
          val code = suggestion.codeForConstraint
          val shortDescription = if(description.length > 20) description.take(20) + "..." else description
          println(s"  Description: $shortDescription, Code: $code")
        }
      }
    }
  }

  def evaluate(spark: SparkSession): Unit = {
    val airlineDataset = new AirlineDataset(spark)
    val flightsDataFrame = airlineDataset.onTimeOnTimePerformance20161Df
    val suggestionResult = {
      ConstraintSuggestionRunner()
        .onData(flightsDataFrame)
        .addConstraintRules(Rules.DEFAULT)
        .run()
    }

    suggestionResult.printSuggestions("AirlineID")
    suggestionResult.printSuggestions("DepDelay")
    suggestionResult.printSuggestions("OriginState")
    suggestionResult.printSuggestions("FlightDate")
  }
}
