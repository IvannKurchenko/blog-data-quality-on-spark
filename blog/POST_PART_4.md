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
```

This application outputs the following profiles output:
```text
`AirlineID` profile:
  Profile class: NumericColumnProfile
  Completeness: 1.0
  Approximate Num DistinctValues: 12
  Data type: Integral
  Histogram (short): (20304,DistributionValue(47619,0.10681048927050178));(20416,DistributionValue(11047,0.02477866975306565));(20409,DistributionValue(23018,0.05162989231248893))
`DepDelay` profile:
  Profile class: NumericColumnProfile
  Completeness: 0.9742658026543929
  Approximate Num DistinctValues: 745
  Data type: Fractional
  Histogram (short): <empty>
`OriginState` profile:
  Profile class: StringColumnProfile
  Completeness: 1.0
  Approximate Num DistinctValues: 54
  Data type: String
  Histogram (short): (MA,DistributionValue(9148,0.02051916999194755));(IN,DistributionValue(3133,0.007027389547963672));(ID,DistributionValue(1728,0.0038759429105908795))
`FlightDate` profile:
  Profile class: StandardColumnProfile
  Completeness: 1.0
  Approximate Num DistinctValues: 32
  Data type: Unknown
  Histogram (short): <empty>
Evaluation finished successfully
```
The library supports more detailed profiling for strings and numerical types.

### Analyzers
Analyzers are another way to get high level view of dataset content, but controlled comparing to the profiles.
This time, we need also to specify which metrics we want to measure. Complete list of available analyzers can be found [here](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ/analyzers).
To stay aligned with general approach of testing the Airlines dataset using 7 category metrics, lets use same columns and measurements for analyzing:

```scala
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
```

That should the analyzing results as data frame that outputs as following:
```text
+-----------+----------------------------------------------------------+------------------+------------------+
|entity     |instance                                                  |name              |value             |
+-----------+----------------------------------------------------------+------------------+------------------+
|Column     |AirlineID                                                 |Completeness      |1.0               |
|Column     |Code                                                      |Completeness      |1.0               |
|Column     |DepDelay                                                  |ApproxQuantile-0.9|28.0              |
|Column     |FlightDate                                                |Compliance        |0.9707980898420239|
|Column     |FlightDate                                                |Completeness      |1.0               |
|Column     |OriginState                                               |CountDistinct     |52.0              |
|Column     |Speed                                                     |Mean              |409.33326582686044|
|Column     |TailNum                                                   |Completeness      |0.9904806124348682|
|Dataset    |*                                                         |Size              |445827.0          |
|Multicolumn|FlightDate,AirlineId,TailNum,OriginAirportID,DestAirportID|Distinctness      |0.9611844953311486|
+-----------+----------------------------------------------------------+------------------+------------------+
```
Analyzers can help suspicious or wrong things which we probably want to check later. For example, mean speed of "409" looks a bit low. 

### Checks
After prior profiling and analyzing, we can proceed to actual data quality checks implementation.
Although the library provides a lot of out of [built-in checks](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/checks/Check.scala) there are couple limitations to keep in mind for the `Airlines` case study:
- Checks support single dataframe. Which means for foreign keys checks prior join is necessary.
- Limited data types support. For instance, `date` and `timestamp` types are not natively supported for age checks.

In the same type, it supports SQL expression for predicates, that can be used for wide variety of cases.
To test quality of our dataset all that needs to be done is:
- Create dataframe under the test. This includes some pre-computations, like joining with dimensional tables for foreign keys check or average flight speed check. 
- Create [VerificationSuite](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/VerificationSuite.scala) with checks and run it;
- Handle [VerificationResult](https://github.com/awslabs/deequ/blob/7f0c554169d628ef10d6c8b298937ec4f4a72ff3/src/main/scala/com/amazon/deequ/VerificationResult.scala#L45).

```scala
import Constants.StateCodes
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object EvaluationDeequChecks extends EvaluationApp {
  def evaluate(spark: SparkSession): Unit = {
    val testDataFrame = createTestDataFrame(spark)
    checkTestDataFrame(spark, testDataFrame)
  }

  /**
   * Main method with testing - create `VerificationSuite` for dataframe and checks, run it and print rest.
   */
  private def checkTestDataFrame(spark: SparkSession, testDataFrame: DataFrame): Unit = {
    val allChecks = createAllChecks()
    val verificationResult: VerificationResult = {
      VerificationSuite()
        .onData(testDataFrame)
        .addChecks(allChecks)
        .run()
    }

    VerificationResult.checkResultsAsDataFrame(spark, verificationResult).show()
    println(s"Verification result status: ${verificationResult.status}")
  }

  /**
   * Create data-frame to test with some pre-calculations
   */
  private def createTestDataFrame(spark: SparkSession): DataFrame = {
    val airlineDataset = new AirlineDataset(spark)
    val faaDataset = new FaaDataset(spark)

    val flightsDataFrame = airlineDataset.onTimeOnTimePerformance20161Df.as("flights")
    val airlineDataFrame = airlineDataset.lAirlineIdDf.as("airlines")
    val faaDataFrame = faaDataset.allTailNumbersDf.as("faa")

    flightsDataFrame
      .withColumn("Speed", col("Distance") / (col("AirTime") / lit(60)))
      .join(airlineDataFrame, col("flights.AirlineID") === col("airlines.Code"), joinType = "left")
      .join(faaDataFrame, col("flights.TailNum") === col("faa.FaaTailNum"), joinType = "left")
  }

  private def createAllChecks() = {
    createAccuracyChecks() ++
      createCompletnessChecks() ++
      createConsistencyChecks() ++
      createCreateCredibilityChecks() ++
      createCurrentnessChecks() ++
      createReasonablenessChecks() ++
      createUniquenessChecks()
  }

  /** Accuracy & Validity checks */
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

  /** Completeness checks. */
  def createCompletnessChecks(): Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "All values in columns `FlightDate`, `AirlineID`, `TailNum` are not null.")
        .areComplete(Seq("FlightDate", "AirlineID", "TailNum"))
    )
  }

  /** Consistency checks */
  def createConsistencyChecks(): Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "All values in column `AirlineID` match `Code` in `L_AIRLINE_ID` table")
        .areComplete(Seq("Code"))
    )
  }

  /** Credibility / Accuracy checks. */
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

  /** Reasonableness checks. */
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
   */
  def createUniquenessChecks() : Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "The proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.")
        .hasUniqueness(Seq("FlightDate", "AirlineId", "TailNum", "OriginAirportID", "DestAirportID"), _ > 0.9)
    )
  }
}
```

Tha would output the following result (truncated for the sake of brevity):
```text
+--------------------+-----------+------------+--------------------+-----------------+--------------------+
|               check|check_level|check_status|          constraint|constraint_status|  constraint_message|
+--------------------+-----------+------------+--------------------+-----------------+--------------------+
|All values in the...|      Error|     Success|ComplianceConstra...|          Success|                    |
|90th percentile o...|      Error|     Success|ApproxQuantileCon...|          Success|                    |
|At least 80% of `...|      Error|     Success|ComplianceConstra...|          Success|                    |
|The proportion of...|      Error|     Success|UniquenessConstra...|          Success|                    |
|All values of the...|      Error|       Error|PatternMatchConst...|          Failure|Value: 0.0 does n...|
|All values in col...|      Error|     Success|ComplianceConstra...|          Success|                    |
|All values in col...|      Error|       Error|ComplianceConstra...|          Failure|Value: 0.99048061...|
|Average speed is ...|      Error|       Error|MeanConstraint(Me...|          Failure|Value: 409.333265...|
|All rows have `Ac...|      Error|       Error|ComplianceConstra...|          Failure|Value: 0.97189717...|
|All values in the...|      Error|     Success|ComplianceConstra...|          Success|                    |
+--------------------+-----------+------------+--------------------+-----------------+--------------------+
Verification result status: Error
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
```
Which will output the following suggestions:
```text
Unable to map type DateType
AirlineID suggestions: 
  Description: 'AirlineID' is not null, Code: .isComplete("AirlineID")
  Description: 'AirlineID' has value range '19393', '19805', '19790', '20304', '20366', '19977', '20409', '19930', '20416', '20436', '19690', '21171', Code: .isContainedIn("AirlineID", Array("19393", "19805", "19790", "20304", "20366", "19977", "20409", "19930", "20416", "20436", "19690", "21171"))
  Description: 'AirlineID' has value range '19393', '19805', '19790', '20304', '20366', '19977', '20409' for at least 90.0% of values, Code: .isContainedIn("AirlineID", Array("19393", "19805", "19790", "20304", "20366", "19977", "20409"), _ >= 0.9, Some("It should be above 0.9!"))
  Description: 'AirlineID' has no negative values, Code: .isNonNegative("AirlineID")
DepDelay suggestions: 
  Description: 'DepDelay' has less than 3% missing values, Code: .hasCompleteness("DepDelay", _ >= 0.97, Some("It should be above 0.97!"))
OriginState suggestions: 
  Description: 'OriginState' is not null, Code: .isComplete("OriginState")
  Description: 'OriginState' has value range 'CA', 'TX', 'FL', 'GA', 'IL', 'NY', 'CO', 'AZ', 'NV', 'NC', 'MI', 'VA', 'WA', 'MN', 'MA', 'NJ', 'UT', 'PA', 'HI', 'MO', 'MD', 'TN', 'OH', 'LA', 'OR', 'WI', 'IN', 'AK', 'PR', 'OK', 'KY', 'SC', 'AL', 'ID', 'NE', 'NM', 'CT', 'AR', 'MT', 'ND', 'RI', 'MS', 'IA', 'WY', 'KS', 'SD', 'VI', 'NH', 'ME', 'VT', 'WV', 'TT', Code: .isContainedIn("OriginState", Array("CA", "TX", "FL", "GA", "IL", "NY", "CO", "AZ", "NV", "NC", "MI", "VA", "WA", "MN", "MA", "NJ", "UT", "PA", "HI", "MO", "MD", "TN", "OH", "LA", "OR", "WI", "IN", "AK", "PR", "OK", "KY", "SC", "AL", "ID", "NE", "NM", "CT", "AR", "MT", "ND", "RI", "MS", "IA", "WY", "KS", "SD", "VI", "NH", "ME", "VT", "WV", "TT"))
  Description: 'OriginState' has value range 'CA', 'TX', 'FL', 'GA', 'IL', 'NY', 'CO', 'AZ', 'NV', 'NC', 'MI', 'VA', 'WA', 'MN', 'MA', 'NJ', 'UT', 'PA', 'HI', 'MO', 'MD', 'TN', 'OH', 'LA' for at least 90.0% of values, Code: .isContainedIn("OriginState", Array("CA", "TX", "FL", "GA", "IL", "NY", "CO", "AZ", "NV", "NC", "MI", "VA", "WA", "MN", "MA", "NJ", "UT", "PA", "HI", "MO", "MD", "TN", "OH", "LA"), _ >= 0.9, Some("It should be above 0.9!"))
FlightDate suggestions: 
  Description: 'FlightDate' is not null, Code: .isComplete("FlightDate")
```
Interestingly to find that 90% of flights originates from less than a half of state, so Deeque suggested this as well.

### Conclusion
Deequ is probably not the most convenient, feature rich and up-to-date library for data quality testing available for Spark. 
But it proposes a lot of very inspiring ideas, such analyzers, profiles, suggestions and [incremental metrics computation](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/algebraic_states_example.md).
At least these reasons worth having a look at library and related paper.

All the code you find in this [GitHub repository](https://github.com/IvannKurchenko/blog-data-quality-on-spark). In the next part, we will discover [pandera](https://pandera.readthedocs.io/en/stable/).

### References
- [Test data quality at scale with Deequ](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/)
- [Deequ GitHub](https://github.com/awslabs/deequ)
- [Streaming Data Quality using AWS Deequ](https://www.databricks.com/notebooks/streaming-data-quality.html)
- [Automating Large-Scale Data Quality Verification](http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf)