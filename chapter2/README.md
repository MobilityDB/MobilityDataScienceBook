## Mobility Data Science: From Data to Insights
### Mahmoud Sakr, Alejandro Vaisman, Esteban Zimányi


### Chapter 2: Spatial and Temporal Databases


### Spatial Databases

There is a database dump for the spartial database, called belgium.sql.
It can be restored from the command prompt as

psql  -U postgres -p 5432 -d Belgium    -f /file-location-path/belgium.sql

### Temporal Databases

There are two versions of the same database 
*  A version using standard SQL, in which the valid time of tuples is
   represented in two columns `FromDate` and `ToDate` of type `Date`.
*  A MobilityDB version, in which the valid time of tuples is
   represented in a single column `VT` or `LS` of type `tstzspanset`

To install the standard SQL version using `psql` proceed as follows
```bash
$ createdb tempcompany
$ psql tempcompany
psql (16.2)
Type "help" for help.

tempcompany=# \i create_tables.sql
SET
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
tempcompany=# \i dbload.sql
SET
INSERT 0 8
INSERT 0 8
INSERT 0 10
INSERT 0 8
INSERT 0 7
INSERT 0 3
INSERT 0 10
INSERT 0 3
INSERT 0 5
INSERT 0 6
INSERT 0 6
INSERT 0 16
tempcompany=#
```

To install the MobilityDB version using `psql` proceed as follows
```bash
$ createdb tempcompany_mobdb
$ psql tempcompany_mobdb
psql (16.2)
Type "help" for help.

tempcompany_mobdb=# create extension mobilitydb cascade;
NOTICE:  installing required extension "postgis"
CREATE EXTENSION
tempcompany_mobdb=# \i create_tables_mobdb.sql
SET
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
tempcompany_mobdb=# \i dbload_mobdb.sql
SET
INSERT 0 8
INSERT 0 10
INSERT 0 8
INSERT 0 7
INSERT 0 3
INSERT 0 10
INSERT 0 3
INSERT 0 5
INSERT 0 6
INSERT 0 6
INSERT 0 16
tempcompany_mobdb=#
```

   
   

