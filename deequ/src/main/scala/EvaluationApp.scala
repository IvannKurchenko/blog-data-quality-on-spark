import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Using}

trait EvaluationApp {
  def main(args: Array[String]): Unit = {
    println("Deequ evaluation starting...")
    val sparkSessionFactory = new SparkSessionFactory
    Using(sparkSessionFactory.createSession)(evaluate) match {
      case Failure(exception) => exception.printStackTrace()
      case Success(_) => println("Evaluation finished successfully")
    }
  }

  def evaluate(spark: SparkSession): Unit
}
