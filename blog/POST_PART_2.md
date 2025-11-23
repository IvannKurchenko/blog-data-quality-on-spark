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
The following snippet demonstrates how easy it is to set up and run Soda locally:
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
scan._configuration.samples_limit = 3

# Execute the scan itself.
scan.execute() 
```

`CustomSampler` implementation will be shown later.

More about invoking Soda library you can fine [here](https://docs.soda.io/quick-start-sip/programmatic) and [here](https://docs.soda.io/data-source-reference/connect-spark)

### Sampling
As it was shown bellow, Soda can collect rows samples that don't meet check requirements. Keep in mind, not all the checks able to do this.
In Soda Library collected samples are submitted to Soda Cloud. However, in Soda core handling samples requires additional implementation. 
More details you can find in [Configure a Python custom sampler](https://docs.soda.io/run-a-scan/failed-row-samples#configure-a-python-custom-sampler) documentation.
We are going to extend `soda.sampler.sampler.Sampler` and save all samples into CSV files, later to show at once. 

```python
import json
import os
from datetime import datetime, date
from decimal import Decimal

import pandas as pd
from pyspark.sql import SparkSession
from soda.sampler.sample_context import SampleContext
from soda.sampler.sampler import Sampler

SODA_SAMPLES_FOLDER = "soda_samples"


class CustomSampler(Sampler):
    """
    Custom sampler that dumps collected samples of failed rows into the local filesystem
    for later display as part of the data quality evaluation report.
    """

    # Custom serialized for Spark types
    @staticmethod
    def json_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    # Main method to override
    def store_sample(self, sample_context: SampleContext):  # type: ignore
        check_name = sample_context.check_name or sample_context.sample_name
        exceptions_df = self._create_exceptions_df(sample_context, check_name)
        check_file_path = self._get_sample_file_path(check_name)
        os.makedirs(SODA_SAMPLES_FOLDER, exist_ok=True)
        exceptions_df.to_csv(check_file_path, index=False, encoding="utf-8")

    def _create_exceptions_df(
        self, sample_context: SampleContext, check_name: str
    ) -> pd.DataFrame:
        rows = sample_context.sample.get_rows()
        json_data = json.dumps(rows, default=CustomSampler.json_serializer)
        exceptions_df = pd.read_json(json_data)
        exceptions_df.columns = [
            col["name"] for col in sample_context.sample.get_schema().get_dict()
        ]
        # Add column with a check for which row has failed. 
        exceptions_df.insert(0, "Failed Check", check_name)
        return exceptions_df

    def _get_sample_file_path(self, check_name: str) -> str:
        check_file_name = f"{check_name.lower().strip().replace(' ', '_')}.csv"
        return f"{SODA_SAMPLES_FOLDER}/{check_file_name}"
```

### Report
To get evaluation details as a report, we can get simply logs:
```python
scan.set_verbose(False)
logs_text = scan.get_logs_text()
```

Saved samples we can now collect and print at the end with help of Pandas:
```python
csv_files = list(Path(SODA_SAMPLES_FOLDER).glob("*.csv"))
df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
print("Samples:")
print(df.to_string())
```

And the end we can get nice report:

```text
Soda evaluation report:
Soda Core 3.5.6
Scan summary:
8/12 checks PASSED: 
    flights in all_flights
      ActualElapsedTime that is more than AirTime [PASSED]
      AirlineID must exist in airline dimensional table. [PASSED]
      90th percentile of `DepDelay` should be under 60 minutes [PASSED]
      Invalid OriginState value [PASSED]
      FlightDate is null [PASSED]
      Flights should not be older then 11 years. [PASSED]
      Flight duplicates are less than 10% [PASSED]
      AirlineID is null [PASSED]
2/12 checks WARNED: 
    flights in all_flights
      TailNum values should be registered FAA number. [WARNED]
        check_value: 90.16905660715928
      TailNum is null [WARNED]
        check_value: 0.95
        row_count: 445827
        missing_count: 4244
2/12 checks FAILED: 
    flights in all_flights
      Average speed should be close to 885 KpH [FAILED]
        check_value: 409.3332658268516
      Invalid TailNum format [FAILED]
        check_value: 29554
Oops! 2 failures. 2 warnings. 0 errors. 8 pass.
Samples:
                                       Failed Check  Year  Quarter  Month  DayofMonth  DayOfWeek  FlightDate UniqueCarrier  AirlineID Carrier TailNum  FlightNum  OriginAirportID  OriginAirportSeqID  OriginCityMarketID Origin         OriginCityName OriginState  OriginStateFips OriginStateName  OriginWac  DestAirportID  DestAirportSeqID  DestCityMarketID Dest           DestCityName DestState  DestStateFips DestStateName  DestWac  CRSDepTime  DepTime  DepDelay  DepDelayMinutes  DepDel15  DepartureDelayGroups DepTimeBlk  TaxiOut  WheelsOff  WheelsOn  TaxiIn  CRSArrTime  ArrTime  ArrDelay  ArrDelayMinutes  ArrDel15  ArrivalDelayGroups ArrTimeBlk  Cancelled CancellationCode  Diverted  CRSElapsedTime  ActualElapsedTime  AirTime  Flights  Distance  DistanceGroup  CarrierDelay  WeatherDelay  NASDelay  SecurityDelay  LateAircraftDelay  FirstDepTime  TotalAddGTime  LongestAddGTime  DivAirportLandings  DivReachedDest  DivActualElapsedTime  DivArrDelay  DivDistance Div1Airport  Div1AirportID  Div1AirportSeqID  Div1WheelsOn  Div1TotalGTime  Div1LongestGTime  Div1WheelsOff Div1TailNum  Div2Airport  Div2AirportID  Div2AirportSeqID  Div2WheelsOn  Div2TotalGTime  Div2LongestGTime
0                                   TailNum is null  2016        1      1          23          6  2016-01-23            AA      19805      AA     NaN         44            14747             1474703               30559    SEA            Seattle, WA          WA               53      Washington         93          12478           1247803             31703  JFK           New York, NY        NY             36      New York       22         640      NaN       NaN              NaN       NaN                   NaN  0600-0659      NaN        NaN       NaN     NaN        1501      NaN       NaN              NaN       NaN                 NaN  1500-1559          1                B         0             321                NaN      NaN        1      2422             10           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
1                                   TailNum is null  2016        1      1          23          6  2016-01-23            AA      19805      AA     NaN         45            12478             1247803               31703    JFK           New York, NY          NY               36        New York         22          14747           1474703             30559  SEA            Seattle, WA        WA             53    Washington       93        1830      NaN       NaN              NaN       NaN                   NaN  1800-1859      NaN        NaN       NaN     NaN        2152      NaN       NaN              NaN       NaN                 NaN  2100-2159          1                B         0             382                NaN      NaN        1      2422             10           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
2                                   TailNum is null  2016        1      1          24          7  2016-01-24            AA      19805      AA     NaN         45            12478             1247803               31703    JFK           New York, NY          NY               36        New York         22          14747           1474703             30559  SEA            Seattle, WA        WA             53    Washington       93        1830      NaN       NaN              NaN       NaN                   NaN  1800-1859      NaN        NaN       NaN     NaN        2152      NaN       NaN              NaN       NaN                 NaN  2100-2159          1                B         0             382                NaN      NaN        1      2422             10           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
3   TailNum values should be registered FAA number.  2016        1      1           4          1  2016-01-04            AA      19805      AA  N010AA         62            12266             1226603               31453    IAH            Houston, TX          TX               48           Texas         74          13303           1330303             32467  MIA              Miami, FL        FL             12       Florida       33        1325   1326.0       1.0              1.0       0.0                   0.0  1300-1359     12.0     1338.0    1633.0     7.0        1647   1640.0      -7.0              0.0       0.0                -1.0  1600-1659          0              NaN         0             142              134.0    115.0        1       964              4           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
4   TailNum values should be registered FAA number.  2016        1      1          24          7  2016-01-24            AA      19805      AA  N010AA        183            10423             1042302               30423    AUS             Austin, TX          TX               48           Texas         74          12892           1289203             32575  LAX        Los Angeles, CA        CA              6    California       91         705    655.0     -10.0              0.0       0.0                  -1.0  0700-0759     10.0      705.0     801.0     5.0         832    806.0     -26.0              0.0       0.0                -2.0  0800-0859          0              NaN         0             207              191.0    176.0        1      1242              5           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
5   TailNum values should be registered FAA number.  2016        1      1          12          2  2016-01-12            AA      19805      AA  N010AA        351            11298             1129804               30194    DFW  Dallas/Fort Worth, TX          TX               48           Texas         74          12266           1226603             31453  IAH            Houston, TX        TX             48         Texas       74        1412   1426.0      14.0             14.0       0.0                   0.0  1400-1459      8.0     1434.0    1516.0     5.0        1519   1521.0       2.0              2.0       0.0                 0.0  1500-1559          0              NaN         0              67               55.0     42.0        1       224              1           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
6   TailNum values should be registered FAA number.  2016        1      1          12          2  2016-01-12            AA      19805      AA  N010AA        351            12266             1226603               31453    IAH            Houston, TX          TX               48           Texas         74          11298           1129804             30194  DFW  Dallas/Fort Worth, TX        TX             48         Texas       74        1604   1600.0      -4.0              0.0       0.0                  -1.0  1600-1659     12.0     1612.0    1654.0    35.0        1719   1729.0      10.0             10.0       0.0                 0.0  1700-1759          0              NaN         0              75               89.0     42.0        1       224              1           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
7   TailNum values should be registered FAA number.  2016        1      1           2          6  2016-01-02            AA      19805      AA  N010AA        367            11298             1129804               30194    DFW  Dallas/Fort Worth, TX          TX               48           Texas         74          12012           1201203             32012  GUC           Gunnison, CO        CO              8      Colorado       82        1205   1200.0      -5.0              0.0       0.0                  -1.0  1200-1259     11.0     1211.0    1258.0     4.0        1318   1302.0     -16.0              0.0       0.0                -2.0  1300-1359          0              NaN         0             133              122.0    107.0        1       678              3           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
8   TailNum values should be registered FAA number.  2016        1      1           2          6  2016-01-02            AA      19805      AA  N010AA        367            12012             1201203               32012    GUC           Gunnison, CO          CO                8        Colorado         82          11298           1129804             30194  DFW  Dallas/Fort Worth, TX        TX             48         Texas       74        1358   1354.0      -4.0              0.0       0.0                  -1.0  1300-1359      9.0     1403.0    1633.0     9.0        1655   1642.0     -13.0              0.0       0.0                -1.0  1600-1659          0              NaN         0             117              108.0     90.0        1       678              3           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
9   TailNum values should be registered FAA number.  2016        1      1           9          6  2016-01-09            AA      19805      AA  N010AA        340            11298             1129804               30194    DFW  Dallas/Fort Worth, TX          TX               48           Texas         74          13198           1319801             33198  MCI        Kansas City, MO        MO             29      Missouri       64        1210   1207.0      -3.0              0.0       0.0                  -1.0  1200-1259     16.0     1223.0    1331.0     5.0        1348   1336.0     -12.0              0.0       0.0                -1.0  1300-1359          0              NaN         0              98               89.0     68.0        1       460              2           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
10  TailNum values should be registered FAA number.  2016        1      1           9          6  2016-01-09            AA      19805      AA  N010AA        340            13198             1319801               33198    MCI        Kansas City, MO          MO               29        Missouri         64          11298           1129804             30194  DFW  Dallas/Fort Worth, TX        TX             48         Texas       74        1433   1426.0      -7.0              0.0       0.0                  -1.0  1400-1459     13.0     1439.0    1556.0    11.0        1612   1607.0      -5.0              0.0       0.0                -1.0  1600-1659          0              NaN         0              99              101.0     77.0        1       460              2           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
11  TailNum values should be registered FAA number.  2016        1      1          20          3  2016-01-20            AA      19805      AA  N010AA        390            11298             1129804               30194    DFW  Dallas/Fort Worth, TX          TX               48           Texas         74          13495           1349503             33495  MSY        New Orleans, LA        LA             22     Louisiana       72        1020   1036.0      16.0             16.0       1.0                   1.0  1000-1059     15.0     1051.0    1150.0     3.0        1145   1153.0       8.0              8.0       0.0                 0.0  1100-1159          0              NaN         0              85               77.0     59.0        1       447              2           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
12  TailNum values should be registered FAA number.  2016        1      1          26          2  2016-01-26            AA      19805      AA  N010AA        390            11298             1129804               30194    DFW  Dallas/Fort Worth, TX          TX               48           Texas         74          13495           1349503             33495  MSY        New Orleans, LA        LA             22     Louisiana       72        1020   1015.0      -5.0              0.0       0.0                  -1.0  1000-1059     11.0     1026.0    1130.0     2.0        1145   1132.0     -13.0              0.0       0.0                -1.0  1100-1159          0              NaN         0              85               77.0     64.0        1       447              2           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
13  TailNum values should be registered FAA number.  2016        1      1          20          3  2016-01-20            AA      19805      AA  N010AA        390            13495             1349503               33495    MSY        New Orleans, LA          LA               22       Louisiana         72          13303           1330303             32467  MIA              Miami, FL        FL             12       Florida       33        1300   1248.0     -12.0              0.0       0.0                  -1.0  1300-1359     13.0     1301.0    1522.0     9.0        1550   1531.0     -19.0              0.0       0.0                -2.0  1500-1559          0              NaN         0             110              103.0     81.0        1       675              3           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
14  TailNum values should be registered FAA number.  2016        1      1          26          2  2016-01-26            AA      19805      AA  N010AA        390            13495             1349503               33495    MSY        New Orleans, LA          LA               22       Louisiana         72          13303           1330303             32467  MIA              Miami, FL        FL             12       Florida       33        1300   1253.0      -7.0              0.0       0.0                  -1.0  1300-1359      9.0     1302.0    1527.0    15.0        1550   1542.0      -8.0              0.0       0.0                -1.0  1500-1559          0              NaN         0             110              109.0     85.0        1       675              3           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
15  TailNum values should be registered FAA number.  2016        1      1           3          7  2016-01-03            AA      19805      AA  N010AA       1044            12266             1226603               31453    IAH            Houston, TX          TX               48           Texas         74          13303           1330303             32467  MIA              Miami, FL        FL             12       Florida       33         515    514.0      -1.0              0.0       0.0                  -1.0  0001-0559     14.0      528.0    1051.0    11.0         839   1102.0       NaN              NaN       NaN                 NaN  0800-0859          0              NaN         1             144                NaN      NaN        1       964              4           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   1             1.0                 288.0        143.0          0.0         RSW        14635.0         1463502.0         922.0            64.0              64.0         1026.0      N010AA          NaN            NaN               NaN           NaN             NaN               NaN
16  TailNum values should be registered FAA number.  2016        1      1           4          1  2016-01-04            AA      19805      AA  N010AA       1044            12266             1226603               31453    IAH            Houston, TX          TX               48           Texas         74          13303           1330303             32467  MIA              Miami, FL        FL             12       Florida       33         515    511.0      -4.0              0.0       0.0                  -1.0  0001-0559     13.0      524.0     816.0     5.0         839    821.0     -18.0              0.0       0.0                -2.0  0800-0859          0              NaN         0             144              130.0    112.0        1       964              4           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
17  TailNum values should be registered FAA number.  2016        1      1           6          3  2016-01-06            AA      19805      AA  N010AA       1127            10397             1039705               30397    ATL            Atlanta, GA          GA               13         Georgia         34          13303           1330303             32467  MIA              Miami, FL        FL             12       Florida       33         645    643.0      -2.0              0.0       0.0                  -1.0  0600-0659     14.0      657.0     833.0     6.0         837    839.0       2.0              2.0       0.0                 0.0  0800-0859          0              NaN         0             112              116.0     96.0        1       594              3           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
18  TailNum values should be registered FAA number.  2016        1      1          10          7  2016-01-10            AA      19805      AA  N010AA       1168            11298             1129804               30194    DFW  Dallas/Fort Worth, TX          TX               48           Texas         74          11503           1150303             31503  EGE              Eagle, CO        CO              8      Colorado       82         900    855.0      -5.0              0.0       0.0                  -1.0  0900-0959     13.0      908.0    1002.0     5.0        1025   1007.0     -18.0              0.0       0.0                -2.0  1000-1059          0              NaN         0             145              132.0    114.0        1       721              3           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
19  TailNum values should be registered FAA number.  2016        1      1          10          7  2016-01-10            AA      19805      AA  N010AA       1168            11503             1150303               31503    EGE              Eagle, CO          CO                8        Colorado         82          11298           1129804             30194  DFW  Dallas/Fort Worth, TX        TX             48         Texas       74        1115   1107.0      -8.0              0.0       0.0                  -1.0  1100-1159     10.0     1117.0    1354.0     9.0        1425   1403.0     -22.0              0.0       0.0                -2.0  1400-1459          0              NaN         0             130              116.0     97.0        1       721              3           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
20  TailNum values should be registered FAA number.  2016        1      1          22          5  2016-01-22            AA      19805      AA  N010AA       1187            10423             1042302               30423    AUS             Austin, TX          TX               48           Texas         74          12892           1289203             32575  LAX        Los Angeles, CA        CA              6    California       91        2010   2003.0      -7.0              0.0       0.0                  -1.0  2000-2059     11.0     2014.0    2108.0     9.0        2136   2117.0     -19.0              0.0       0.0                -2.0  2100-2159          0              NaN         0             206              194.0    174.0        1      1242              5           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
21  TailNum values should be registered FAA number.  2016        1      1          22          5  2016-01-22            AA      19805      AA  N010AA       1187            12892             1289203               32575    LAX        Los Angeles, CA          CA                6      California         91          10423           1042302             30423  AUS             Austin, TX        TX             48         Texas       74        1435   1434.0      -1.0              0.0       0.0                  -1.0  1400-1459     20.0     1454.0    1924.0     5.0        1927   1929.0       2.0              2.0       0.0                 0.0  1900-1959          0              NaN         0             172              175.0    150.0        1      1242              5           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
22  TailNum values should be registered FAA number.  2016        1      1          23          6  2016-01-23            AA      19805      AA  N010AA       1187            12892             1289203               32575    LAX        Los Angeles, CA          CA                6      California         91          10423           1042302             30423  AUS             Austin, TX        TX             48         Texas       74        1435   1424.0     -11.0              0.0       0.0                  -1.0  1400-1459     19.0     1443.0    1908.0     7.0        1927   1915.0     -12.0              0.0       0.0                -1.0  1900-1959          0              NaN         0             172              171.0    145.0        1      1242              5           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
23              Flight duplicates are less than 10%  2016        1      1           1          5  2016-01-01            WN      19393      WN  N285WN       1680            13495             1349503               33495    MSY        New Orleans, LA          LA               22       Louisiana         72          12191           1219102             31453  HOU            Houston, TX        TX             48         Texas       74        1405   1416.0      11.0             11.0       0.0                   0.0  1400-1459      6.0     1422.0    1518.0     3.0        1515   1521.0       6.0              6.0       0.0                 0.0  1500-1559          0              NaN         0              70               65.0     56.0        1       302              2           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
24              Flight duplicates are less than 10%  2016        1      1           1          5  2016-01-01            WN      19393      WN  N285WN       3158            13495             1349503               33495    MSY        New Orleans, LA          LA               22       Louisiana         72          12191           1219102             31453  HOU            Houston, TX        TX             48         Texas       74        1035   1032.0      -3.0              0.0       0.0                  -1.0  1000-1059      9.0     1041.0    1136.0     4.0        1145   1140.0      -5.0              0.0       0.0                -1.0  1100-1159          0              NaN         0              70               68.0     55.0        1       302              2           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
25              Flight duplicates are less than 10%  2016        1      1           1          5  2016-01-01            WN      19393      WN  N486WN       2939            11140             1114005               31140    CRP     Corpus Christi, TX          TX               48           Texas         74          12191           1219102             31453  HOU            Houston, TX        TX             48         Texas       74        1755   1812.0      17.0             17.0       1.0                   1.0  1700-1759      9.0     1821.0    1856.0     3.0        1900   1859.0      -1.0              0.0       0.0                -1.0  1900-1959          0              NaN         0              65               47.0     35.0        1       187              1           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
26                           Invalid TailNum format  2016        1      1           6          3  2016-01-06            AA      19805      AA  N4YBAA         43            11298             1129804               30194    DFW  Dallas/Fort Worth, TX          TX               48           Texas         74          11433           1143302             31295  DTW            Detroit, MI        MI             26      Michigan       43        1100   1057.0      -3.0              0.0       0.0                  -1.0  1100-1159     15.0     1112.0    1424.0     8.0        1438   1432.0      -6.0              0.0       0.0                -1.0  1400-1459          0              NaN         0             158              155.0    132.0        1       986              4           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
27                           Invalid TailNum format  2016        1      1          12          2  2016-01-12            AA      19805      AA  N4YBAA         43            11298             1129804               30194    DFW  Dallas/Fort Worth, TX          TX               48           Texas         74          11433           1143302             31295  DTW            Detroit, MI        MI             26      Michigan       43        1100   1059.0      -1.0              0.0       0.0                  -1.0  1100-1159     14.0     1113.0    1429.0     9.0        1438   1438.0       0.0              0.0       0.0                 0.0  1400-1459          0              NaN         0             158              159.0    136.0        1       986              4           NaN           NaN       NaN            NaN                NaN           NaN            NaN              NaN                   0             NaN                   NaN          NaN          NaN         NaN            NaN               NaN           NaN             NaN               NaN            NaN         NaN          NaN            NaN               NaN           NaN             NaN               NaN
```

# Conclusion
At this post, we covered Soda, another powerful tool for data quality evaluation. All the code from you find in this [GitHub repository](https://github.com/IvannKurchenko/blog-data-quality-on-spark) 
In the next part we will discover [DQX](https://databrickslabs.github.io/dqx/) library developed by Databricks.
