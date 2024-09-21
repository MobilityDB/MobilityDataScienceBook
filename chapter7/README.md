## Mobility Data Science: From Data to Insights
### Mahmoud Sakr, Alejandro Vaisman, Esteban Zimányi

### Chapter 7: Mobility Data Warehouses

This is the companion website of the book published by Springer.
It contains the datasets used in the book alongside with the scripts
allowing you to input these datasets in PostgreSQL and reproduce the
examples and exercises in the book.

### Northwind Data Warehouse

NorthwindDW_PSQL.zip: Nortwind datawarehouse script and data, to be downloaded on a PostgreSQL database rihght away.

### Northwind Mobility Data Warehouse

Creates the deliveries data warehouse used in the chapter.

The steps to follow are detailed next.

createdb deliveries_sf0.1

psql deliveries_sf0.1
  create extension mobilitydb cascade;
  create extension pgrouting;
  create extension hstore;
  exit

osm2pgrouting -h yourhost -p 5432 (or your port)  -U yourUser  -W  yourPassword -f brussels.osm --dbname deliveries_sf0.1 -c mapconfig_brussels.xml

-- mapconfig_brussels.xml is given in the material for Chapter 6.

osm2pgsql -H yourhost  -P 5432 (or your port) -d deliveries_sf0.1 -c -U 
yourUser -W --proj=3857 brussels.osm

psql deliveries_sf0.1

\i brussels_preparedata.sql

\i berlinmod_datagenerator.sql

\i deliveries_datagenerator.sql
  
select deliveries_datagenerator(scalefactor:= 0.1);


----------------------------------------

### Using Prebuilt CSVs

Alternatively, you can use prebuilt CSV files with different scale factors. You can find them
at https://github.com/MobilityDB/MobilityDB-BerlinMOD/blob/master/README.md.

After downloading the right file, you create the mobility database using the 
deliveries_load.sql script.  

