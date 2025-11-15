## Data quality on Spark: Theory

### Introduction
In the following series of blog posts we will reveal a topic of Data Quality from both theoretical and practical 
implementation using Spark framework and compare corresponding tools for this job. 
Although commercial market for Data Quality evaluation is pretty wide and worth looking, the focus of this series is 
OSS solutions.

This first part of the series gives a short introduction into of data quality topic and cover first framework - 
Greate Expectations. 

### Definition of Data Quality
The definition of Data Quality given in many resource, which can be summarised measurement of how well the data represents
a real world facts. Although this definition gives a good sense what is meant by Data Quality, this is not detailed
enough to be used for any implementation. So let's review more detailed definition by public standards and professional 
literature.

#### ISO-25000
ISO-25000 is a series of international standards describing evaluation and management of quality of informational systems. 
ISO-25012 and ISO-25024 are two standards from the series that are primary focus is data quality.  

ISO-25012 defines high level model for the data quality. It defines two main categories of data quality characteristics:
- "Inherent Data Quality" - set of characteristics related to the data per-se.
- "System-Dependent Data Quality" - set of characteristics related to data serving implementation.

The standard specifies the list of characteristics and corresponding "data quality properties" that each characteristic
can be measured by for "Inherent Data Quality" category:

Accuracy - reflects how well data represents real world facts or measurements.
For example: proportion of correct email addresses.
Data quality properties: "Syntactic Accuracy", "Semantic Accuracy", "Accuracy range";

Completeness - how many of expected entity properties are present. 
For example: proportion of empty or null values.
Data quality properties: "Record Completeness", "File completeness", "Data Value Completeness", "False Completeness Of File" etc.

Consistency - how much data is coherent.
For example: proportion of denormalized data that is inconsistent across tables.
Data quality properties:  "Referential Integrity", "Risk Of Inconsistency", "Semantic Consistency", "Format Consistency".

Credibility - how much we can be trusted. For example: proportion of geo-addresses confirmed via external sources like OSM.
Data quality properties: "Data values Credibility", "Source Credibility";

Currentnes - how fresh the data is.
For example: proportion of records upserted in the past 24 hours.   
Data quality properties: "Update Frequency", "Timelines of Update";

#### DAMA-DMBOK
["Data Management Body of Knowledge"](https://dama.org/learning-resources/dama-data-management-body-of-knowledge-dmbok/) one of the important books in a field of Data Management 
giving valuable recommendations, in particular covering subject of Data Quality (Chapter 16).
The book reveals a subject of Data Quality from the perspective of the following dimensions:

Validity - whether values are correct from domain knowledge perspective.
For example: proportion of numerical records within expected range for an age in socio-demographic data-set.

Completeness - whether required data is present. 
For example: proportion of absent or blank values for required columns in a data-set.

Consistency - whether data encoded the same way all over a places.
For example: proportion of records complying same address format between tables in CRM dataset.

Integrity - whether references across the data-sets are consistent.
For example: proportion of records with broken foreign keys between department and employee in data-set of organization structure. 

Timeliness - whether data is updated within expected time range.
For example: data-set reflecting sales quarter results should be completely updated within two weeks.   

Currency - whether data is fresh enough and reflects current state of world.
For example: proportion of records which were inserted not more than 1 hour ago in data-set of IoT devices readings.  

Reasonableness - whether high level values patterns are close to expectations. 
For example: house prices are close to normal distribution in real estate dataset. 

Uniqueness / Deduplication - whether duplicated records are present in data-set.
For example: proportion of lead contacts records in CRM dataset. 

Accuracy - whether records in data-set accurately reflect real world facts or knowledge.
For example: proportion of customer address records that were verified using trusted external data-sources such
as data from government bodies like Chamber of Commerce or Ministry of Trade.

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

Although Data Quality definitions above are proposed by reputable bodies, it worth mention that final implementation
of Data Quality measurements and their importance are highly depends on specific use case.


### Methodology
In order to fairly compare and evaluate each Data Quality framework in this series, they will be tested under same conditions.
We will check quality of [Airline](https://relational.fel.cvut.cz/dataset/Airline) data-set from "CTU Relational Learning Repository" repository.
This data-set represents flight data in US for 2016 year, consisting of main `On_Time_On_Time_Performance_2016_1` table and several dimensions. 
The data set is not too complex, represents real world data and slightly messy, which is a good combination to for case study.
The goal is to measure this data set quality per each dimension with corresponding metrics:

Accuracy & Validity 
- Proportion of `TailNum` column values that are valid combinations (see [Aircraft registration](https://en.wikipedia.org/wiki/Aircraft_registration))
- Proportion of valid `OriginState` and `DestinationState` (see [States Abbreviations](https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/appendix_a.html))

Completeness
- Proportion of `null` values in `On_Time_On_Time_Performance_2016_1` fact table.

Consistency & Integrity
- Proportion of wrong foreign key columns like `AirlineID`, `OriginAirportID` etc.

Credibility / Accuracy
- Validate `TailNum` in https://aerobasegroup.com 

Currentnes / Currency
- Proportion of records older than a year ago.

Reasonableness
- Average speed calculated based on `AirTime` (in minutes) and `Distance` is withing modern Aircraft average speed.
- 90th percentile of `DepDelay` is under 60 minutes; 

Uniqueness
- Proportion of duplicates by `FlightDate`, `AirlineId`, `TailNum`, `OriginAirportID` and `DestAirportID`.

# References
Bellow is the list of other sources used to write this post:
- https://iso25000.com/index.php/en/iso-25000-standards/iso-25012
- https://arxiv.org/pdf/2102.11527
- https://medium.com/the-thoughtful-engineer/part-5-iso-25012-in-action-what-data-quality-really-means-5e334b828595