## Data Quality on Spark, Part 4: Pandera

## Introduction

In this series of blog posts, we explore Data Quality from both a theoretical perspective and a practical implementation standpoint using the Spark framework. We also compare several tools designed to support Data Quality assessments. 
In this part, we continue exploring the [Airline](https://relational.fel.cvut.cz/dataset/Airline) dataset using the same Data Quality checks, this time with the [Pandera](https://pandera.readthedocs.io/) library.

Previous parts:
- [Data Quality on Spark, Part 1: GreatExpectations](https://medium.com/gitconnected/data-quality-on-spark-part-1-greatexpectations-fd4ffa126ca0)
- [Data Quality on Spark, Part 2: Soda](https://medium.com/gitconnected/data-quality-on-spark-part-2-soda-97d5d32e2d8b)
- [Data Quality on Spark, Part 3: DQX](https://medium.com/gitconnected/data-quality-on-spark-part-3-dqx-f0335b8ff07d)
- [Data Quality on Spark, Part 4: Deequ](https://medium.com/gitconnected/data-quality-on-spark-part-4-deequ-d82e8c2344ae)

## Pandera
Pandera, as the documentation describes it, is:

> Pandera is a Union.ai open source project that provides a flexible and expressive API for performing data validation on dataframe-like objects. The goal of Pandera is to make data processing pipelines more readable and robust with statistically typed dataframes.

Pandera, similarly to some of previously considered technologies like GreatExpectations and Soda, provides capabilities to perform quality checks for a number of other technologies.
However, unlike them Pandera focuses does not focus on underlying storage, but instead it targets on data frame libraries. 
Complete list of supported backends you can find [here](https://pandera.readthedocs.io/en/stable/#supported-features-by-dataframe-backend). Spark specific implementation has some differences that you can read more about at [Data Validation with Pyspark SQL](https://pandera.readthedocs.io/en/stable/pyspark_sql.html#registering-custom-checks).

### Setup
First, we need to install the following packages using your preferred package manager:
- [`pandera[pyspark]`](https://pypi.org/project/pandera/)

### Create a data frame to validate
First thing first, to start validate our flights data from [Airline](https://relational.fel.cvut.cz/dataset/Airline) dataset we need to load a data-frame which we about to validate.
As it will be shown later, at this stage we need to perform some pre-calculation, such as join to validate foreign keys for dimensional table or create additional column with average air speed. 

NOTE: For the sake of readability shown the most important code examples. Complete codebase can be found at [this repo](https://github.com/IvannKurchenko/blog-data-quality-on-spark).  

```python
def prepare_data(spark: SparkSession) -> DataFrame:
    airline_dataset = AirlineDataset(spark)
    faa_dataset = FaaDataset(spark)

    # Read dimensional table with airline codes
    airline_id_df = airline_dataset.l_airline_id_df().withColumnRenamed("Code", "AirlineCode")
    # Read FAA tail numbers data set
    faa_tail_numbers_df = faa_dataset.all_tail_numbers_df()
    # Read main flights data set
    on_time_on_time_performance_df = airline_dataset.on_time_on_time_performance_2016_1_df()

    # Prepare data set for validation
    input_df = (
        on_time_on_time_performance_df
        .withColumn("FlightSpeed", F.col("Distance") / (F.col("AirTime") / F.lit(60)))
        .withColumn(
            "FlightCompoundId",
            F.concat_ws(
                "-",
                F.col("FlightDate"),
                F.col("AirlineId"),
                F.col("TailNum"),
                F.col("OriginAirportID"),
                F.col("DestAirportID"),
            )
        )
        .join(
            airline_id_df,
            on=on_time_on_time_performance_df.AirlineID == airline_id_df.AirlineCode,
            how="left",
        )
        .join(
            faa_tail_numbers_df,
            on=on_time_on_time_performance_df.TailNum == faa_tail_numbers_df.FaaTailNum,
            how="left",
        )
    )
    return input_df
```
As you may see, there was added `FlightSpeed`, `FlightCompoundId` and other columns which are not a part of originally validated data-set for will serve for validation.

### Create a data frame schema
Pandera's main abstraction for data frames validation is [`DataFrameSchema`](https://pandera.readthedocs.io/en/stable/dataframe_schemas.html). Within schema, we can describe both dataframe schema to validate along with checks to perform.
`DataFrameSchema` consists of a number of [`Column`](https://pandera.readthedocs.io/en/stable/dataframe_schemas.html#column-validation)'s within which it is possible to define a number of [`Check`](https://pandera.readthedocs.io/en/stable/checks.html)'s.
On the one hand, this is convenient abstraction allowing to describe both expected schema and data quality checks in one place. On the other hand, it is not feasible to perform cross data-set or cross-column checks, such as foreign keys validation.
Because of these reasons, in previously prepared data frame there were preliminarily added some columns.

Since, we can't define checks without columns, we will go though the data quality checks implementation on column basis.

#### `TailNum`
For this column we need to implement two checks for the following categories:
Accuracy & Validity: 
> All values of `TailNum` column are valid "tail number" combinations (see [Aircraft registration](https://en.wikipedia.org/wiki/Aircraft_registration))

Completeness: 
> All values in columns `TailNum` are not null.

First, we need to implement custom check for regexp matching:
```python
@register_check_method
def matches_regexp(pyspark_obj, *, regexp) -> bool:
    cond = F.regexp(F.col(pyspark_obj.column_name), F.lit(regexp))
    return pyspark_obj.dataframe.filter(~cond).count() == 0
```

Then we can create column definition with the check itself: 
```python
def create_tail_num_column() -> pa.Column:
    return pa.Column(
        dtype=T.StringType(),
        required=True,
        nullable=False,  # With this flag Pandera does null checks for us
        name="TailNum",
        checks=[
            pa.Check(
                check_fn=matches_regexp,
                element_wise=True,
                name="Invalid TailNum format",
                description="Invalid TailNum format",
                error="Invalid TailNum format",
                n_failure_cases=10,
                regexp="^N(?:[1-9]\\d{0,4}|[1-9]\\d{0,3}[A-Z]|[1-9]\\d{0,2}[A-Z]{2})$",
            )
        ],
    )
```

#### `OriginState`
For this column we have the only one check to perform: 
> All values in column `OriginState` contain valid state abbreviations (see [States Abbreviations](https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/appendix_a.html))

Luckily, Pandera has built in check for this: 
```python
def create_origin_state_column() -> pa.Column:
    return pa.Column(
        dtype=T.StringType(),
        required=True,
        name="OriginState",
        checks=[pa.Check.isin(STATE_CODES)],
    )
```

### `ActualElapsedTime`
For this column the only condition to test is:
> All rows have `ActualElapsedTime` that is more than `AirTime

This is sort of check for which we need to have custom implementation as well. 
```python
@register_check_method
def greater_then_column(pyspark_obj, *, limit) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    condition_col = F.col(pyspark_obj.column_name) > F.col(limit)
    condition = data_frame.filter(~condition_col).count() == 0
    return condition

def create_airtime_column() -> pa.Column:
    return pa.Column(
        dtype=T.DoubleType(),
        nullable=False,
        required=True,
        name="ActualElapsedTime",
        checks=[
            Check(
                check_fn=greater_then_column,
                limit="AirTime",
                element_wise=True,
                name="ActualElapsedTime is more than AirTime",
                description="ActualElapsedTime is more than AirTime",
                error="ActualElapsedTime that is less than AirTime",
                n_failure_cases=10,
            )
        ]
    )
```

### `FlightDate`
Quality check to verify for this column is:
> All values in column `FlightDate` are not older than 2016.

This can be checked with also custom method: 
```python
@register_check_method
def max_age_days(pyspark_obj, *, age_days: int) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    condition = (
        data_frame
        .where(F.date_diff(F.now(), F.col(column_name)) > F.lit(age_days))
        .count() == 0
    )
    return condition

def create_flight_date_column() -> pa.Column:
    return pa.Column(
        dtype=T.DateType(),
        nullable=False,
        name="FlightDate",
        checks=[
            Check(
                check_fn=max_age_days,
                age_days=367 * 10,
                element_wise=True,
                name="FlightDate is older then 10 years ago",
                description="ActualElapsedTime that is more than AirTime",
                error="FlightDate is older then 10 years ago",
                n_failure_cases=10,
            )
        ],
    )
```

### `AirlineId`
For this column essentially there are to checks to verify:
> All values in columns `AirlineID` are not null.
> All values in column `AirlineID` match `Code` in `L_AIRLINE_ID` table, etc.

But because we joined `L_AIRLINE_ID` table preliminary with adding new column `AirlineCode` to check foreign key, all is left to do is to verify that foreign key is not null.
Pandera does this job for us by specifying columns with `nullable=False` parameter: 
```python
def create_airline_code_column() -> pa.Column:
    return pa.Column(
        dtype=T.IntegerType(),
        nullable=False,
        name="AirlineCode",
    )


def create_airline_id_column() -> pa.Column:
    return pa.Column(
        dtype=T.IntegerType(),
        nullable=False,
        name="AirlineID",
    )
```

### `TailNum`
The data quality check for this column:
> At least 80% of `TailNum` column values can be found in [Federal Aviation Agency Database](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download)

Because requirement states to check certain proportion of null values we can't simply declare the column as not nullable, but instead use custom check:
```python
@register_check_method
def null_values_max_percentage(pyspark_obj, *, percent: int) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    nulls_count = data_frame.where(F.isnull(F.col(column_name))).count()
    total_count = data_frame.count()
    condition = int((nulls_count / total_count) * 100) < percent
    return condition

def create_faa_tail_num_column() -> pa.Column:
    return pa.Column(
        dtype=T.StringType(),
        nullable=True,
        name="FaaTailNum",
        checks=[
            Check(
                check_fn=null_values_max_percentage,
                percent=20,
                element_wise=False,
                name="FaaTailNum has more than 20 percent of null values",
                error="FaaTailNum has more than 20 percent of null values",
                n_failure_cases=10,
            )
        ]
    )
```

### `FlightSpeed`
This is another column pre-calculated in original data frame for the following data quality check:
> Average speed calculated based on `AirTime` (in minutes) and `Distance` is close to the average cruise speed of modern aircraft - 885 KpH.

For which, we do need to specify another custom check method:
```python
@register_check_method
def average_within_boundaries(pyspark_obj, *, bottom_limit: int, upper_limit: int) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    column = F.avg(F.col(pyspark_obj.column_name))
    condition = (
            data_frame.select(column.alias(column_name))
            .where(F.col(column_name) >= F.lit(bottom_limit))
            .where(F.col(column_name) <= F.lit(upper_limit))
            .count() == 0
    )
    return condition

def create_flight_speed_column() -> pa.Column:
    return pa.Column(
        dtype=T.DoubleType(),
        nullable=False,
        name="FlightSpeed",
        checks=[
            Check(
                check_fn=average_within_boundaries,
                bottom_limit=800,
                upper_limit=900,
                element_wise=False,
                error="Average FlightSpeed is not within boundaries of 800 Km and 900 Km",
                n_failure_cases=10,
            )
        ],
    )
```

### `DepDelay`
Data quality check for this column is:
> 90th percentile of `DepDelay` is under 60 minutes; 

Similarly to the previously considered `FlightSpeed`, we need to use custom check method with aggregation:

```python
@register_check_method
def percentile_within_boundaries(
        pyspark_obj, *, percentile: float, bottom_limit: int, upper_limit: int
) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    column = F.percentile_approx(F.col(pyspark_obj.column_name), F.lit(percentile))
    condition = (
            data_frame.select(column.alias(column_name))
            .where(F.col(column_name) >= F.lit(bottom_limit))
            .where(F.col(column_name) <= F.lit(upper_limit))
            .count() == 0
    )
    return condition


def create_dep_delay_column() -> pa.Column:
    return pa.Column(
        dtype=T.DecimalType(38, 2),
        nullable=False,
        name="DepDelay",
        checks=[
            Check(
                check_fn=percentile_within_boundaries,
                percentile=0.9,
                bottom_limit=0,
                upper_limit=60,
                element_wise=False,
                error="DepDelay is above 60 minutes",
            )
        ],
    )
```

### `FlightCompoundId`
This is yet another auxiliary column added for the following data quality check:  
> The proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.

So, we can now specify column with a check:
```python
@register_check_method
def duplicates_percentage(pyspark_obj, *, percentage: int) -> bool:
    data_frame: DataFrame = pyspark_obj.dataframe
    column_name = pyspark_obj.column_name
    duplicates_percentage = (F.count("*") - F.count_distinct(column_name)) / F.count("*") * 100
    condition = (
            data_frame.select(duplicates_percentage.alias("duplicates"))
            .where(F.col("duplicates") > percentage)
            .count() == 0
    )
    return condition


def create_flight_compound_id_column() -> pa.Column:
    return pa.Column(
        dtype=T.StringType(),
        nullable=False,
        coerce=False,
        required=True,
        name="FlightCompoundId",
        checks=[
            Check(
                check_fn=duplicates_percentage,
                percentage=10,
                element_wise=False,
                error="FlightCompoundId is duplicate percentage",
                n_failure_cases=10,
            )
        ]
    )
```

Once we have all columns in place, we can finally specify a whole schema: 
```python
def create_validate_schema() -> pa.DataFrameSchema:
    schema = pa.DataFrameSchema(
        {
            "TailNum": create_tail_num_column(),
            "OriginState": create_origin_state_column(),
            "AirTime": create_airtime_column(),
            "FlightDate": create_flight_date_column(),
            "AirlineID": create_airline_id_column(),
            "AirlineCode": create_airline_code_column(),
            "FaaTailNum": create_faa_tail_num_column(),
            "FlightSpeed": create_flight_speed_column(),
            "DepDelay": create_dep_delay_column(),
            "FlightCompoundId": create_flight_compound_id_column(),
        }
    )
    return schema
```

### Run validation
After declaring a data frame schema, we can run validate it against the data frame:
```python
def evaluate(spark: SparkSession):
    input_df = prepare_data(spark)
    schema = create_validate_schema()
    df_out = schema.validate(input_df, lazy=True)

    df_out_errors = df_out.pandera.errors
    print(json.dumps(dict(df_out_errors), indent=4))
```
Which will produce the following resulting report in json format:
```json
{
    "SCHEMA": {
        "SERIES_CONTAINS_NULLS": [
            {
                "schema": null,
                "column": "TailNum",
                "check": "not_nullable",
                "error": "non-nullable column 'TailNum' contains null"
            },
            {
                "schema": null,
                "column": "AirTime",
                "check": "not_nullable",
                "error": "non-nullable column 'AirTime' contains null"
            },
            {
                "schema": null,
                "column": "FlightSpeed",
                "check": "not_nullable",
                "error": "non-nullable column 'FlightSpeed' contains null"
            },
            {
                "schema": null,
                "column": "DepDelay",
                "check": "not_nullable",
                "error": "non-nullable column 'DepDelay' contains null"
            }
        ]
    },
    "DATA": {
        "DATAFRAME_CHECK": [
            {
                "schema": null,
                "column": "TailNum",
                "check": "Invalid TailNum format",
                "error": "column 'TailNum' with type StringType() failed validation Invalid TailNum format"
            },
            {
                "schema": null,
                "column": "AirTime",
                "check": "ActualElapsedTime that is less than AirTime",
                "error": "column 'AirTime' with type DoubleType() failed validation ActualElapsedTime that is less than AirTime"
            },
            {
                "schema": null,
                "column": "DepDelay",
                "check": "DepDelay is above 60 minutes",
                "error": "column 'DepDelay' with type DecimalType(38,2) failed validation DepDelay is above 60 minutes"
            }
        ]
    }
}
```

## Conclusion
Although pandera proses rich set functionality, only a part of this available for Spark and pandas is way better supported.  
Additionally, `DataframeSchema` abstraction has a number of limitations, that forces to do a lot of work for certain problems like foreign keys checks.
All the code you find in this [GitHub repository](https://github.com/IvannKurchenko/blog-data-quality-on-spark).
