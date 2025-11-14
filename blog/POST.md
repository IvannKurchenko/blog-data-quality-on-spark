## Data quality on Spark

### Introduction
In the following series of blog posts we will reveal a topic of Data Quality from both theoretical and practical 
implementation using Spark framework and compare corresponding tools for this job. 
Although commercial market for Data Quality evaluation is pretty wide and worth looking, the focus of this series is 
OSS solutions.

### Definition of Data Quality
The definition of Data Quality given in many resource, which can be summarised measurement of how well the data represents
a real world facts. Although this definition gives a good sense what is meant by Data Quality, this is not detailed
enough to be used for any implementation. So let's review more detailed definition by public standards and professional 
literature.

### ISO-25000
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

Currents - how fresh the data is.
For example: proportion of records upserted in the past 24 hours.   
Data quality properties: "Update Frequency", "Timelines of Update";


# References
Bellow is the list of other sources used to write this post:
- https://iso25000.com/index.php/en/iso-25000-standards/iso-25012
- https://arxiv.org/pdf/2102.11527