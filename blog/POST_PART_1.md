## Data quality on Spark, Part 1: GreatExpectations

### Introduction
In the following series of blog posts, we will reveal a topic of Data Quality from both theoretical and practical implementation using the Spark framework and compare corresponding tools for this job.
Although the commercial market for Data Quality evaluation is pretty wide and worth looking at, the focus of this series is 
OSS solutions. In particular, the series will cover the following technologies:
* [GreatExpectations](https://greatexpectations.io)
* [Soda](https://soda.io)
* [DQX](https://databrickslabs.github.io/dqx/)
* [deequ](https://github.com/awslabs/deequ)
* [pandera](https://pandera.readthedocs.io/en/stable/)

This first part of the series gives a short introduction to the data quality topic and covers the first framework - GreatExpectations. 

### Definition of Data Quality
The definition of Data Quality given in many resources, can be summarised measurement of how well the data represents
real-world facts. Although this definition gives a good sense of what is meant by Data Quality, this is not detailed
enough to be used for any implementation. So let's review a more detailed definition by public standards and professional  literature.

#### ISO-25000
ISO-25000 is a series of international standards describing the evaluation and management of the quality of informational systems. 
ISO-25012 and ISO-25024 are two standards from the series that have a primary focus is data quality.  

ISO-25012 defines a high-level model for data quality. It defines two main categories of data quality characteristics:
- "Inherent Data Quality" - a set of characteristics related to the data per se.
- "System-Dependent Data Quality" - a set of characteristics related to data serving implementation.

The standard specifies the list of characteristics and corresponding "data quality properties" for each characteristic can be measured by for the "Inherent Data Quality" category:

Accuracy - reflects how well data represents real world facts or measurements.
For example: proportion of correct email addresses.
Data quality properties: "Syntactic Accuracy", "Semantic Accuracy", "Accuracy range";

Completeness - how many of the expected entity properties are present. 
For example: proportion of empty or null values.
Data quality properties: "Record Completeness", "File completeness", "Data Value Completeness", "False Completeness Of File", etc.

Consistency - how much data is coherent.
For example: proportion of denormalized data that is inconsistent across tables.
Data quality properties:  "Referential Integrity", "Risk Of Inconsistency", "Semantic Consistency", "Format Consistency".

Credibility - how much we can be trusted. For example: proportion of geo-addresses confirmed via external sources like OSM.
Data quality properties: "Data values Credibility", "Source Credibility";

Currentness - how fresh the data is.
For example: proportion of records upserted in the past 24 hours.   
Data quality properties: "Update Frequency", "Timelines of Update";

#### DAMA-DMBOK
["Data Management Body of Knowledge"](https://dama.org/learning-resources/dama-data-management-body-of-knowledge-dmbok/) is one of the important books in the field of Data Management 
giving valuable recommendations, in particular covering the subject of Data Quality (Chapter 16).
The book reveals a subject of Data Quality from the perspective of the following dimensions:

Validity - whether values are correct from a domain knowledge perspective.
For example: proportion of numerical records within the expected range for an age in a socio-demographic data-set.

Completeness - whether the required data is present. 
For example: proportion of absent or blank values for required columns in a data set.

Consistency - whether data is encoded the same way all over a place.
For example: proportion of records complying same address format between tables in the CRM dataset.

Integrity - whether references across the data sets are consistent.
For example, the proportion of records with broken foreign keys between the department and employee in the data set of the organization structure. 

Timeliness - whether data is updated within the expected time range.
For example, the data set reflecting sales quarter results should be completely updated within two weeks.   

Currency - whether the data is fresh enough and reflects the current state of the world.
For example: proportion of records that were inserted not more than 1 hour ago in the data set of IoT devices readings.  

Reasonableness - whether high-level value patterns are close to expectations. 
For example, house prices are close to a normal distribution in the real estate dataset. 

Uniqueness / Deduplication - whether duplicated records are present in the dataset.
For example: proportion of lead contacts records in the CRM dataset. 

Accuracy - whether records in the data-set accurately reflect real-world facts or knowledge.
For example: proportion of customer address records that were verified using trusted external data sources such
as data from government bodies like the Chamber of Commerce or the Ministry of Trade.

#### Summary
Aforementioned Data Quality Dimensions (DQD*) could be summarized in the following list:

* Accuracy (IS0) & Validity (DMBOK) - Whether data comply syntactic and semantic rules.
* Completeness (ISO & DMBOK) -Whether expected data is present in full form. 
* Consistency (ISO) and Integrity (DMBOK) - Whether components of data set are coherent between each other.
* Credibility (ISO) & Accuracy (DMBOK) - Whether data can be trusted and reflects correctly state of real world.
* Currentnes (ISO) & Currency (DMBOK) - Whether data is fresh enough and reflect recent state of real world.
* Timeliness (DMBOK)
* Reasonableness (DMBOK)
* Uniqueness (DMBOK)

Although Data Quality definitions above are proposed by reputable bodies, it is worth mentioning that final implementation of Data Quality measurements and their importance are highly depend on the specific use case.

### Methodology
In order to fairly compare and evaluate each Data Quality framework in this series, they will be tested under the same conditions.
We will check the quality of [Airline](https://relational.fel.cvut.cz/dataset/Airline) data-set from "CTU Relational Learning Repository" repository.
This data set represents flight data in the US for 2016, consisting of the main `On_Time_On_Time_Performance_2016_1` table and several dimensions. 
The data set is not too complex, represents real-world data, and is slightly messy, which is a good combination for the case study.
The goal is to measure this data set quality for each dimension with corresponding metrics. 
Surely the list of measured metrics can be extended a lot more, but for the sake of brevity, let's keep it short, having 1-3 metrics per category:

Accuracy & Validity 
- All values of `TailNum` column are valid "tail number" combinations (see [Aircraft registration](https://en.wikipedia.org/wiki/Aircraft_registration))
- All values in column `OriginState` contain valid state abbreviations (see [States Abbreviations](https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/appendix_a.html))
- All rows have `ActualElapsedTime` that is more than `AirTime`. 

Completeness
- All values in columns `FlightDate`, `AirlineID`, `TailNum` are not null. 

Consistency & Integrity
- All values in column `AirlineID` match `Code` in `L_AIRLINE_ID` table, etc.

Credibility / Accuracy
- At least 80% of `TailNum` column values can be found in [Federal Aviation Agency Database](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download)

Currentness / Currency
- All values in column `FlightDate` are not older than 2016.

Reasonableness
- Average speed calculated based on `AirTime` (in minutes) and `Distance` is close to the average cruise speed of modern aircraft - 885 KpH.
- 90th percentile of `DepDelay` is under 60 minutes; 

Uniqueness
- Proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID`, and `DestAirportID` is less than 10%.

### Great Expectations
[Great Expectations](https://greatexpectations.io) is a powerful framework that provides the capability to test your data in multiple storage and platforms, including Spark. One of the core elements and building blocks is [Expectation](https://docs.greatexpectations.io/docs/core/define_expectations/create_an_expectation) that essentially defines a validation rule for a data. The framework provides many capabilities (such as [checkpointing](https://docs.greatexpectations.io/docs/reference/api/checkpoint_class/)) 
which won't be covered here. For the sake of brevity, let's keep the focus on the Airline case study and data validation.

First, we need to set up basic infrastructure for the framework:
```python
import great_expectations as gx
context = gx.get_context()

data_source_name = "airline"
data_source = context.data_sources.add_spark(name=data_source_name)

data_asset_name = "flights"
data_asset = data_source.add_dataframe_asset(name=data_asset_name)

batch_definition_name = "airline_batch_definition"
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

batch_parameters = {"dataframe": data_frame}
batch = batch_definition.get_batch(batch_parameters=batch_parameters)
```
where
- `data_source` - [Datasource](https://docs.greatexpectations.io/docs/reference/api/datasource/fluent/Datasource_class). Where we're going to get data from.
- `data_asset` - [DataAsset](https://docs.greatexpectations.io/docs/reference/api/datasource/fluent/DataAsset_class). A records within data source.
- `batch_definition` - [BatchDefinition](https://docs.greatexpectations.io/docs/reference/api/core/batch_definition/batchdefinition_class/). Batch from asset we're going to test. 
- `data_frame` - PySpark data frame under the testing.

After the base is ready, we can proceed to testing the data itself. The plan now is to create a set of expectations, create a [suite](https://docs.greatexpectations.io/docs/0.18/reference/learn/terms/expectation_suite/), and validate it.

#### Accuracy & Validity
To validate `TailNum` values are syntactically correct, we can leverage [ExpectColumnValuesToMatchRegex](https://greatexpectations.io/expectations/expect_column_values_to_match_regex/) to check it against regular expression: 
```python
tail_num_syntactic_validity_expectation = ExpectColumnValuesToMatchRegex(
    column="TailNum",
    regex="^N(?:[1-9]\d{0,4}|[1-9]\d{0,3}[A-Z]|[1-9]\d{0,2}[A-Z]{2})$"
)
```

State codes in `OriginState` column are two-character state abbreviations that can be validated against [known list of codes](https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/appendix_a.html).
[ExpectColumnDistinctValuesToBeInSet](https://greatexpectations.io/expectations/expect_column_distinct_values_to_be_in_set/) expectation can help us with this:
```python
origin_state_semantic_validity_expectation = ExpectColumnDistinctValuesToBeInSet(
    column="OriginState",
    value_set=STATE_CODES
)
```

To validate that `ActualElapsedTime` and `AirTime` are reasonably correct, we can use [ExpectColumnPairValuesAToBeGreaterThanB](https://greatexpectations.io/expectations/expect_column_pair_values_a_to_be_greater_than_b/)
to verify that one total flight time is less than the time in the air:  
```python
elapsed_time_greater_then_air_timey_expectation = ExpectColumnPairValuesAToBeGreaterThanB(
    column_A="ActualElapsedTime",
    column_B="AirTime",
    or_equal=False
)
```

#### Completeness
This one is relatively simple, we just need to validate that the aforementioned columns are not null, which GreatExpectations made easy:
```python
flight_date_not_null_expectation = ExpectColumnValuesToNotBeNull(column="FlightDate")
airline_id_not_null_expectation = ExpectColumnValuesToNotBeNull(column="AirlineID")
tail_num_not_null_expectation = ExpectColumnValuesToNotBeNull(column="TailNum")
```

#### Consistency & Integrity
To validate foreign keys are correct between `AirlineID` and `Code` in `L_AIRLINE_ID` in table, we can consider a couple of options:
- [ExpectColumnValuesToBeInSet](https://greatexpectations.io/expectations/expect_column_values_to_be_in_set/) - collect all `Code` values into a collection. That works fine if the list of values is small to fit in memory.  
- [ExpectQueryResultsToMatchComparison](https://greatexpectations.io/expectations/expect_query_results_to_match_comparison/) - use SQL to join and on a dimension table. Although this works only for Databricks, there is no support for plain Spark.
- [ExpectColumnPairValuesToBeEqual](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_equal/) - perform a join for the target dataset and add the dimension table ID. 

For the sake of simplicity, the last option was used:
```python
airline_id_expectation = ExpectColumnPairValuesToBeEqual(
        column_A="AirlineID", # Main flights table column
        column_B="AirlineCode" # joined `Code` column from `L_AIRLINE_ID`
)
```
More about integrity verification in GreatExpectation you can find in this [documentation](https://docs.greatexpectations.io/docs/reference/learn/data_quality_use_cases/integrity)

#### Credibility & Accuracy
To verify `TailNum` column values accuracy we will use registration numbers from [Federal Aviation Agency Database](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download).
This database will be loaded into a data frame with `FaaTailNum` column, which we can join to the main flights data frame.
Having these two columns in place, we can use the same expectation as in the previous example: 

```python
tail_num_faa_validity_expectation = ExpectColumnPairValuesToBeEqual(
    column_A="TailNum", # Main flights table column
    column_B="FaaTailNum"  # joined `FaaTailNum` column from FAA Database
)
```

#### Currentnes & Currency
To verify `FlightDate` is not older than 2016, we can leverage `ExpectColumnMinToBeBetween` with setting expected min date: 
```python
flight_date_expectation = ExpectColumnMinToBeBetween(
    column="FlightDate",
    min_value="2016-01-01",
)
```
More about freshness verification in GreatExpectation you can find in this [documentation](https://docs.greatexpectations.io/docs/reference/learn/data_quality_use_cases/freshness)

#### Reasonableness
To verify the average speed is within expected boundaries, let's first calculate:
```python
.withColumn('FlightSpeed', F.col('Distance') / (F.col('AirTime') / F.lit(60)))
```

Having this column in place, we can now check the assumption that it should be close to 885 KpH (± 15KpH):  
```python
flight_speed_expectation = ExpectColumnMeanToBeBetween(
    column='FlightSpeed',
    min_value=870,
    max_value=900
)
```

Now, we can proceed to `DepDelay`. GreatExpectation provides the out-of-the-box `ExpectColumnQuantileValuesToBeBetween` expectation to test that its 90th percentile is under 60 minutes:
```python
dep_delay_expectation = ExpectColumnQuantileValuesToBeBetween(
    column="DepDelay",
    quantile_ranges={
        "quantiles": [0.9],
        "value_ranges": [[0, 60]]
    }
)
```

More about distribution verification in GreatExpectation you can find in this [documentation](https://docs.greatexpectations.io/docs/reference/learn/data_quality_use_cases/distribution)

### Uniqueness
Last but not least, we can test proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID` and `DestAirportID` is less than 1%.
For this, we can use `ExpectCompoundColumnsToBeUnique` that does all the work for us:

```python
flight_compound_id_expectation = ExpectCompoundColumnsToBeUnique(
    column_list=['FlightDate', 'AirlineId', 'TailNum', 'OriginAirportID', 'DestAirportID'],
    mostly=0.9,
)
```
More about uniqueness testing in GreatExpectation you can find in this [documentation](https://docs.greatexpectations.io/docs/reference/learn/data_quality_use_cases/uniqueness) 

### Conclusion
In this article, we covered just a small portion of the capabilities that GreatExpectations provides, but you can find more in their docs.
Complete source code you can find [here](https://github.com/IvannKurchenko/blog-data-quality-on-spark).
In the next part, we will cover the [Soda](https://soda.io) framework on the same example.

### References
Bellow is the list of other sources used to write this post:
- https://iso25000.com/index.php/en/iso-25000-standards/iso-25012
- https://arxiv.org/pdf/2102.11527
- https://medium.com/the-thoughtful-engineer/part-5-iso-25012-in-action-what-data-quality-really-means-5e334b828595