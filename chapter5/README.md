## Mobility Data Science: From Data to Insights
### Mahmoud Sakr, Alejandro Vaisman, Esteban Zimányi

### Chapter 5: Querying Mobility Databases

This is the companion website of the book published by Springer.
It contains the datasets used in the book alongside with the scripts
allowing you to input these datasets in PostgreSQL and reproduce the
examples and exercises in the book.

### Air Traffic Analysis

opensky.sql: script to create the tables for the opensky use case. 
The dataset can be downloaded from https://opensky-network.org/datasets/states/2020-06-01/
A local copy of the file for June 1st, 2020 is available [here](https://docs.mobilitydb.com/pub/opensky20200601.zip)

openskyDashboards.sql: SQL queries for feeding the three example panelk in the chapter.

### Maritime Movement

input_ais.sql: script to create the tables for the omaritime movement use case. 
The dataset used can be downloaded from https://web.ais.dk/aisdata/aisdk-2024-03-01.zip
A local copy of the file for March 1st, 2024 is available [here](https://docs.mobilitydb.com/pub/ais20250301.zip)

To run the script:

\i input_ais.sql

SELECT input_ais();

