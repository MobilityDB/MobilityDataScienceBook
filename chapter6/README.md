## Mobility Data Science: From Data to Insights
### Mahmoud Sakr, Alejandro Vaisman, Esteban Zimányi

### Chapter 6: Query Processing and Indexing

This is the companion website of the book published by Springer.
It contains the datasets used in the book alongside with the scripts
allowing you to input these datasets in PostgreSQL and reproduce the
examples and exercises in the book.


### BerlinMOD Data Generator

These files create the BerlinMOD benchmark for the city of Brussels, based on the brussels.osm file that can be obtained from OpenStreetMap. To produce the database you shoul follow the step next.

--------------------------
Create the database brussels_sf0.1 (or your database name)

CREATE EXTENSION MobilityDB Cascade;
create extension pgrouting;
create extension hstore;

osm2pgrouting -h localhost -p 5432 -U postgres -W postgres -f brussels.osm --dbname brussels_sf0.1 -c mapconfig_brussels.xml

osm2pgsql   -P 5432 -H localhost -d brussels_sf0.01 -c -U postgres -W --proj=3857 brussels.osm

psql -d brussels_sf0.01 -p 5432 (or your port) -h localhost

 \i brussels_preparedata.sql
 \i berlinmod_datagenerator.sql
 
 select berlinmod_datagenerator(0.1);

----------------------------------------

### Using Prebuilt CSVs

Alternatively, you can use prebuilt CSV files with different scale factors. You can find them
at https://github.com/MobilityDB/MobilityDB-BerlinMOD/blob/master/README.md.

After downloading the right file, you create the mobility database using the 
berlinmod_load.sql script.  

