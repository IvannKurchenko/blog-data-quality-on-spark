## Data quality on Spark, Part 2: Soda

### Introduction
In the following series of blog posts, we will reveal a topic of Data Quality from both theoretical and practical implementation using the Spark framework and compare corresponding tools for this job.
Although the commercial market for Data Quality evaluation is pretty wide and worth looking at, the focus of this series is OSS solutions.
In the [previous](https://medium.com/gitconnected/data-quality-on-spark-part-1-greatexpectations-fd4ffa126ca0) article we outlined definition of Data Quality 
and demonstrated implementation of these principles on example of [Airline](https://relational.fel.cvut.cz/dataset/Airline) data-set using [GreatExpectations](https://greatexpectations.io) framework.

In this part we will continue exploring same data-set and applying same Data Quality measurements using [Soda Core](https://github.com/sodadata/soda-core) framework.

### Soda
[Soda](https://github.com/sodadata/soda-core) is another powerful tool focusing on making Data Quality assurance easy for a wide variety of storages and platforms.
Among supported technologies Spark present as well, which we describe more in details later. 
Important note before we begin - [Soda](https://github.com/sodadata/soda-core) is a whole Data Quality platform, including
cloud solution, number of cloud integrations and [Soda Library](https://docs.soda.io/quick-start-sip/install) with a lot of advanced capabilities.
Please, don't confuse `Soda Library` with `Soda Core`. The former one is OSS framework that can't connect to Soda Cloud.

Soda define all checks in YAML instead of defining them programmatically using [Soda Checks Language](https://docs.soda.io/soda-cl-overview/quick-start-sodacl#sodacl-in-brief).
Same can be said about [configuring data sources connections](https://github.com/sodadata/soda-core/blob/main/docs/configuration.md), except Spark.
To use Apache Spark `DataFrames` we will need to crete them in Python first. But if you are using cloud solution for Apache Spark, it is strongly encouraged to check if more specific [Data Source](https://docs.soda.io/data-source-reference) configuration is present.

In the following sections we will install and setup Soda, write quality checks for previous dimensions and prepare evaluation report.
For sake of brevity code examples are shortened. Please, find complete code base [here](https://github.com/IvannKurchenko/blog-data-quality-on-spark)

### Setup
First, we need to install the following packages via a package manager of your choice: 
- [soda-core-spark](https://pypi.org/project/soda-core-spark/);
- [soda-core-spark-df](https://pypi.org/project/soda-core-spark-df/)

NOTE: At the moment of writing of this part there were some versions compatibility pitfalls:  
- Soda does not support spark 4.0 as of today. See links for more details: [1](https://github.com/sodadata/soda-core/blob/main/soda/spark_df/setup.py#L11), [2](https://github.com/sodadata/soda-core/issues/2217)
- Soda Core does not support Python 3.12 as of today. See GitHub issue for more details: [1](https://github.com/sodadata/soda-core/issues/2184)


### Data frames setup
After we have Soda in place, we can run it. Although Soda provides nice CLI, in case of Spark we need to launch it programmatically (see [doc](https://docs.soda.io/data-source-reference/connect-spark#connect-to-spark-dataframes)). 
First things first, we need to read and prepare DataFrames. For this example we have three of them: `flights_df`, `airline_id_df`, `faa_tail_numbers_df`.
After, to make them visible for Soda, views needs to be created:
```python
flights_df.createOrReplaceTempView("flights") # Main data frame to validate. Represents `On_Time_On_Time_Performance_2016_1` table from original dataset.
airline_id_df.createOrReplaceTempView("airline_id") # Airlines dimensional table. Represents `L_AIRLINE_ID` table from original dataset.
faa_tail_numbers_df.createOrReplaceTempView("faa_tail_numbers") # FAA databases with officially registered aircraft numbers.  
```


### Data quality checks
[Soda Checks Language](https://docs.soda.io/soda-cl-overview/quick-start-sodacl#sodacl-in-brief) is the YAML based language to define checks in declarative manner.
Although, Soda provides pretty wide range of checks, not all of them are supported by Soda Core. At the moment of writing, Soda Core supported the following:
- [Cross row checks](https://docs.soda.io/sodacl-reference/cross-row-checks)
- [Failed rows checks](https://docs.soda.io/sodacl-reference/failed-rows-checks)
- [For each dataset](https://docs.soda.io/sodacl-reference/for-each)
- [Freshness](https://docs.soda.io/sodacl-reference/freshness)
- [Missing metrics](https://docs.soda.io/sodacl-reference/missing-metrics)
- [Numeric metrics](https://docs.soda.io/sodacl-reference/numeric-metrics)
- [Reference checks](https://docs.soda.io/sodacl-reference/reference)
- [Schema checks](https://docs.soda.io/sodacl-reference/schema)
- [User defined checks](https://docs.soda.io/sodacl-reference/user-defined)
- [Validity metrics](https://docs.soda.io/sodacl-reference/validity-metrics)

Firstly, we need to define data set and list of cheks to perform in YAML:
```yaml
checks for flights:
  - {check condition}
      {check property}
```
where `flights` is a should match with data frame view name we previously created. 

Next, lets create checks per each category as it was done in previous part.

#### Category: Accuracy & Validity
Verification: 
> All values of `TailNum` column are valid "tail number" combinations (see [Aircraft registration](https://en.wikipedia.org/wiki/Aircraft_registration))

To implement this verification, we use [validity checks](https://docs.soda.io/sodacl-reference/validity-metrics#specify-valid-or-invalid-values) for regular expression:
```yaml
  - invalid_count(TailNum) = 0: # `TailNum` is a column name in `flights` view. This line asserts 0 rows should fail condition. 
      valid regex: ^N(?:[1-9]\d{0,4}|[1-9]\d{0,3}[A-Z]|[1-9]\d{0,2}[A-Z]{2})$ # Regular expression to check.
      name: Invalid TailNum format # Name of check to be visible in report
      samples limit: 10 # Number of invalid samples to collect
```

Verification: 
> All values in column `OriginState` contain valid state abbreviations (see [States Abbreviations](https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/appendix_a.html))

This verification can be implemented similar way, by specifying enumeration of valid values explicitly:
```yaml
  - invalid_count(OriginState) = 0:
      valid values: [ "AL", "KY", "OH", "AK", "LA", "OK", "AZ", "ME", "OR", "AR", "MD", "PA", "AS", "MA", "PR", "CA", "MI", "RI", "CO", "MN", "SC", "CT", "MS", "SD", "DE", "MO", "TN", "DC", "MT", "TX", "FL", "NE", "TT", "GA", "NV", "UT", "GU", "NH", "VT", "HI", "NJ", "VA", "ID", "NM", "VI", "IL", "NY", "WA", "IN", "NC", "WV", "IA", "ND", "WI", "KS", "MP", "WY" ]
      name: Invalid OriginState value
```

Verification: 
> All rows have `ActualElapsedTime` that is more than `AirTime`.

This is a bit of tricky case, because Soda does not have a check that exactly meets this requirement. One of the option, implement check saying no rows found that matches opposite condition.
This is pretty easy to do by leveraging [filtering feature](https://docs.soda.io/sodacl-reference/filters#configure-in-check-filters):
```yaml
  - row_count = 0:
      name: ActualElapsedTime that is more than AirTime
      filter: AirTime > ActualElapsedTime # Condition to filter rows by 
```

#### Completeness
Verification:
> All values in columns `FlightDate`, `AirlineID`, `TailNum` are not null.

Soda make null checks ease with [Missing metrics](https://docs.soda.io/sodacl-reference/missing-metrics). Moreover, by "missing" Soda means much wider spectrum of cases then just a `null`, such as `NaN` or empty strings. 
To make it more interesting, lets showcase also severity levels on this example. See [Configure multiple alerts](https://docs.soda.io/sodacl-reference/optional-config#configure-multiple-alerts) on this topic.
```yaml
  - missing_count(FlightDate) = 0: # No rows should be with missing `FlightDate` column.
      name: FlightDate is null
  - missing_count(AirlineID) = 0:
      name: AirlineID is null
  - missing_percent(TailNum):
      name: TailNum is null
      warn: when > 0.5% # Warn if missing values percentage is between 0.5 to 5
      fail: when > 5% # Error if 5 and more.
```
Checks severity later will be visible in final report.

#### Consistency & Integrity
Verification:
>- All values in column `AirlineID` match `Code` in `L_AIRLINE_ID` table, etc.

Soda provides [Reference checks](https://docs.soda.io/sodacl-reference/reference) exactly for this type of case.
```yaml
  - values in (AirlineID) must exist in airline_id (Code): # `AirlineID` if column in `flights` view; `Code` is column from `airline_id` view. 
      name: AirlineID must exist in airline dimensional table.
```

#### Credibility / Accuracy
Verification:
> At least 80% of `TailNum` column values can be found in [Federal Aviation Agency Database](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download)

Although, this sounds like another case for reference check, requirement about matching percentage blocks its usage. Hence, something more custom is needed.
Luckily, Soda provides easy to use mechanism for [user defined checks](https://docs.soda.io/sodacl-reference/user-defined) via using custom SQL.
Idea of the following check, find number of rows (flights) where aircraft `TailNum` has a value that is absent in [FAA Database](https://www.faa.gov):
If percentage is matches is less than 80 then this is a fail and 95 is warning.
To make troubleshooting of such issues easier we can customise collecting of invalid rows with SQL as well. See [Customize a failed row samples query](https://docs.soda.io/run-a-scan/failed-row-samples#customize-a-failed-row-samples-query) for more details.
Implementation looks the following:
```yaml
  - tail_num_faa_registered_percent:
      name: TailNum values should be registered FAA number.
      fail: when < 80 # Fail percentage threshold
      warn: when < 95 # Warn percentage threshold
      tail_num_faa_registered_percent query: | # Query to calculate mismatch percentage
        SELECT (COUNT(FaaTailNum) / COUNT(*)) * 100 as TailNumFaaRegisteredPercentage
        FROM flights
        LEFT JOIN faa_tail_numbers ON TailNum == FaaTailNum
      failed rows query: | # Select rows with invalid `TailNum` to include in report.
          SELECT *
          FROM flights
          LEFT ANTI JOIN faa_tail_numbers ON TailNum == FaaTailNum
          LIMIT 20
```

#### Currentness / Currency
Verification:
> All values in column `FlightDate` are not older than 2016.

Soda provides dedicate class of [freshness checks](https://docs.soda.io/sodacl-reference/freshness) that can also evaluate 
date and time values relative to a date of execution. 
We can express this verification in similar way to demonstrate its usage:
```yaml
  - freshness(FlightDate) < 11y:
      name: Flights should not be older then 11 years.
```

#### Reasonableness
Verification:
> Average speed calculated based on `AirTime` (in minutes) and `Distance` is close to the average cruise speed of modern aircraft - 885 KpH.

Although Soda supports statistics checks, we need to precalculate data for average. This job is still can be easily done with 
[user defined checks](https://docs.soda.io/sodacl-reference/user-defined):
```yaml
  - avg_speed between 800 and 900: # checking `avg_speed` values is within expected boundaries
      name: Average speed should be close to 885 KpH
      avg_speed expression: AVG(Distance / (AirTime / 60)) # expression to calculate average speed in Kilometers per Hour
      failed rows query: SELECT * FROM flights WHERE  (Distance / (AirTime / 60)) < 800 # Collect invalid wrong samples
```


Verification:
> 90th percentile of `DepDelay` is under 60 minutes; 

Although, `percentile` check exists in [Numeric metrics](https://docs.soda.io/sodacl-reference/numeric-metrics#list-of-numeric-metrics) **this is not supported by Spark**.
To make it work, we can leverage Spark's [percentile_approx](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html) for this job: 
```yaml
  - departure_delay_90th_percentile < 60:
      name: 90th percentile of `DepDelay` should be under 60 minutes
      departure_delay_90th_percentile expression: PERCENTILE_APPROX(DepDelay, 0.90)
```

#### Uniqueness
Verification:
> Proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.

To meet this verification `duplicate_percent` function from [Duplicate checks](https://docs.soda.io/soda-cl-overview/quick-start-sodacl#duplicate-check) is all what we need:
```yaml
  - duplicate_percent(FlightDate, AirlineId, TailNum, OriginAirportID, DestAirportID) < 10%:
      name: Flight duplicates are less than 10%
```

This was last piece. Having SodaCL checks in place we can now invoke them. 

### Invoke
The following snippet demonstrates how easy it is to setup and run Soda locally:
```python
from soda.scan import Scan

scan = Scan()
# Ingest instantiated SparkSession
scan.add_spark_session(spark, data_source_name="all_flights")

# Necessary to make `all_flights` data source visible checks yaml file
scan.set_data_source_name("all_flights")

# Supply YAML file with checks created before
scan.add_sodacl_yaml_file(file_path="evaluation_soda_checks.yaml")

# Sampler to dump collected examples of invalid rows. To be shown later.
scan.sampler = CustomSampler()

# Setting data source wide samples limit.
# See for more details:
# https://docs.soda.io/run-a-scan/failed-row-samples#customize-failed-row-samples-for-datasets-and-columns
scan._configuration.samples_limit = 20

# Execute the scan itself.
scan.execute() 
```

More about invoking Soda library you can fine [here](https://docs.soda.io/quick-start-sip/programmatic) and [here](https://docs.soda.io/data-source-reference/connect-spark)

### Samples
TODO

# References
- [Set sample limit per check](https://docs.soda.io/run-a-scan/failed-row-samples#set-a-sample-limit)
