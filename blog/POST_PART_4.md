## Data Quality on Spark, Part 3: Deequ

### Introduction
In this series of blog posts, we explore Data Quality from both a theoretical perspective and a practical implementation standpoint using the Spark framework. We also compare several tools designed to support Data Quality assessments.
Although the commercial market for Data Quality solutions is broad and full of capable products, the focus of this series is on open-source tools.
In this part, we continue exploring [Airline's](https://relational.fel.cvut.cz/dataset/Airline) dataset quality by the same Data Quality checks via [DQX](https://github.com/awslabs/deequ) framework.

Previous parts:
- [Data Quality on Spark, Part 1: GreatExpectations](https://medium.com/gitconnected/data-quality-on-spark-part-1-greatexpectations-fd4ffa126ca0)
- [Data Quality on Spark, Part 2: Soda](https://medium.com/gitconnected/data-quality-on-spark-part-2-soda-97d5d32e2d8b)
- [Data Quality on Spark, Part 3: DQX](https://medium.com/gitconnected/data-quality-on-spark-part-3-dqx-f0335b8ff07d)

### Deequ
Deeque, as documentation positions it, is:
> Deequ is a library built on top of Apache Spark for defining "unit tests for data", which measure data quality in large datasets. 

This a library that has been built by Amazon for Spark in short. Apart from just regular checks and verifications it ships interesting features like profiling, analyzers and suggestions that will be demonstrated later.
Main library is written in Scala, although [Python wrapper](Python wrapper available at: https://github.com/awslabs/python-deequ) is also available.
To keep focus on single implementation, further examples will be shown in Scala.

### Setup
To proceed with further working with the library, you'd need to have installed JDK 17, sbt of any version and Scala 2.12.
Some notes regarding other software version compatibility:
- Scala 2.13 is not yet supported ([see GitHub issue](https://github.com/awslabs/deequ/issues/642));
- Spark 3.5 is the latest supported version so far ([see installation instructions](https://github.com/awslabs/deequ?tab=readme-ov-file#requirements-and-installation)); 

After this is in place we can define our `build.sbt` file:
```scala
scalaVersion := "2.12.20"
libraryDependencies ++= Seq(
  "org.mariadb.jdbc" % "mariadb-java-client" % "3.5.6",
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "com.amazon.deequ" % "deequ" % "2.0.9-spark-3.5"
)

// Forking main process for java options to take effect
fork := true

// Relaxing some security constraints for Spark to run on JDK
javaOptions ++= Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false"
)
```
After having this in place we can review the library capabilities per module using the same [Airline's](https://relational.fel.cvut.cz/dataset/Airline) database.

### Profiling
Profiling provides possibility to get high level view of a dataset without prior heavy lifting.
All is needed is to pass dataframe to profile to `com.amazon.deequ.profiles.ColumnProfilerRunner` and run it.
For the sake of brevity, resulting profile are limited just for certain columns.

```scala
object EvaluationDeequProfile extends EvaluationApp {

  /**
   * Tiny extension over profile result to pretty print results concisely.
   */
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

  override def evaluate(spark: SparkSession): Unit = {
    println("Reading main dataframes...")

    val airlineDataset = new AirlineDataset(spark)
    val flightsDataFrame = airlineDataset.onTimeOnTimePerformance20161Df
    
    // All profiling is executed in the this snippet
    val result = ColumnProfilerRunner()
      .onData(flightsDataFrame)
      .run()

    result.print("AirlineID") // Profiled as regular numeric column;
    result.print("DepDelay") // Correctly identified 
    result.print("OriginState") // string with histogram.
    result.print("FlightDate") // Date time is not supported
  }
}
```

This application outputs the following profiles output:
```text
TODO
```

Couple notes on resulting profile:
- `AirlineID` - Profiled as regular numeric column, although this is semantically this is identifier not measurement;
- `DepDelay` - also profiled as numeric column, which is correct in this case. 
- `OriginState` - results histogram profile, which is pretty close to states enumeration in the column.
- `FlightDate` - deeque does not support profiling for date and timestamp types.

### Analyzers
Analyzers are another way to get high level view of data set content, but controlled and granular to the profiles.
This time, we need also to specify which metrics we want to measure. 
Complete list of available analyzers can be found [here](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ/analyzers) 
To stay aligned with general approach of testing the Airlines dataset using 7 category metrics, lets use same columns and measurements for analyzing:

```scala
TODO
```

That should the analyzing results as data frame that outputs as following:
```text
TODO
```

### Checks
After prior profiling analyzing, we can proceed to actual data quality checks implementation.
Although the library provides a lot of out of [built-in checks](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/checks/Check.scala) there couple some limitation to keep in mind for the Airlines case study:
- Single dataset support. Which means for foreign keys checks prior join is necessary.
- Limited data types support. For instance, `date` and `timestamp` types are not natively supported for age checks.

In the same type, it supports SQL expression for predicates, that can be used for wide variety of cases.
To test quality of our dataset all that needs to be done is:
- Create dataframe under the test. This includes some pre-computations, like joining with dimensional tables for foreign keys check or average flight speed check. 
- Create [VerificationSuite](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/VerificationSuite.scala) with checks and run it;
- Handle VerificationResult (todo - link): output detailed results and compute whole suite result (failed or passed).

```scala
TODO
```
Tha would output the following result:
```text
TODO
```

### Suggestions
Although the main testing is done, we have more to explore. Deeque can help to extend list of checks by automatically suggesting checks based on data set profiling.
Let's consider example of generating suggestions for some columns that we have implemented cheks before to see what else can be verified:
```scala
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
          println(s"  Description: ${suggestion.description}, Code: ${suggestion.codeForConstraint}")
        }
      }
    }
  }

  def evaluate(spark: SparkSession): Unit = {
    println("Reading main dataframes...")

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
```
Which will output the following suggestions:
```text
TODO
```
TODO: which one are new?

### Conclusion
Deequ is probably not the most convenient, feature rich and up-to-date library for data quality testing available for Spark. 
But it proposes a lot of very inspiring ideas, such analyzers, profiles, suggestions and test metrics on delta of change.
At least these reasons worth having a look at library and related paper.

All the code you find in this [GitHub repository](https://github.com/IvannKurchenko/blog-data-quality-on-spark). In the next part, we will discover [pandera](https://pandera.readthedocs.io/en/stable/).

### References
- [Test data quality at scale with Deequ](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/)
- [Deequ GitHub](https://github.com/awslabs/deequ)
- [Streaming Data Quality using AWS Deequ](https://www.databricks.com/notebooks/streaming-data-quality.html)
- [Automating Large-Scale Data Quality Verification](http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf)