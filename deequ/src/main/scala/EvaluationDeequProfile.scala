import com.amazon.deequ.profiles.{ColumnProfilerRunner, ColumnProfiles}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Using}

object EvaluationDeequProfile {

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

    val flightsDataFrame = airlineDataset
      .onTimeOnTimePerformance20161Df
    val result = ColumnProfilerRunner()
      .onData(flightsDataFrame)
      .run()

    print(ColumnProfiles.toJson(result.profiles.values.toSeq))
  }
}
