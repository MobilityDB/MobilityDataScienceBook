createdb brussels_sf0.2
psql brussels_sf0.2



osm2pgrouting --f brussels.osm --conf mapconfig.xml --dbname brussels_sf0.005 -p 5432 --username esteban 
osm2pgsql -c -H localhost -P 5432 -U esteban -W -d brussels_sf0.005 brussels.osm



osm2pgrouting --f brussels.osm --conf mapconfig.xml --dbname brussels_sf0.2 -p 5432 --username esteban 

osm2pgsql -c -H localhost -P 5432 -U esteban -W -d brussels_sf0.2 brussels.osm

-----------------------------------------------------------

CREATE TABLE TripsPart(LIKE Trips) PARTITION BY RANGE(day);

  end_at             := '2020-06-05' -> 0.005
  end_at             := '2020-06-12' -> 0.1
  end_at             := '2020-06-16' -> 0.2
  end_at             := '2020-06-23' -> 0.5
  end_at             := '2020-07-01' -> 1

SELECT create_time_partitions(
  table_name         := 'tripspart',
  partition_interval := '1 day',
  start_from         := '2020-06-01',
  end_at             := '2020-06-05'
);

COPY (SELECT * FROM Trips) TO '/home/esteban/src/trips_berlinmod_sf0.005.csv' CSV DELIMITER ','  HEADER;
COPY TripsPart FROM '/home/esteban/src/trips_berlinmod_sf0.005.csv' CSV DELIMITER ','  HEADER;

-----------------------------------------------------------

ALTER TABLE licences DROP CONSTRAINT licences_pkey;

DROP VIEW Licences1;
DROP VIEW Licences2;
CREATE TABLE Licences1 (LicenceId, Licence, VehId) AS
SELECT LicenceId, Licence, VehId
FROM Licences
LIMIT 10;
CREATE TABLE Licences2 (LicenceId, Licence, VehId) AS
SELECT LicenceId, Licence, VehId
FROM Licences
LIMIT 10 OFFSET 10;

DROP VIEW Instants1;
CREATE TABLE Instants1 (InstantId, Instant) AS
SELECT InstantId, Instant 
FROM Instants
LIMIT 10;

DROP VIEW Periods1;
CREATE TABLE Periods1 (PeriodId, BeginP, EndP, Period) AS
SELECT PeriodId, BeginP, EndP, Period
FROM Periods
LIMIT 10;

DROP VIEW Points1;
CREATE TABLE Points1 (PointId, PosX, PosY, geom) AS
SELECT PointId, PosX, PosY, geom
FROM Points
LIMIT 10;

DROP VIEW Regions1;
CREATE TABLE Regions1 (RegionId, geom) AS
SELECT RegionId, geom
FROM Regions
LIMIT 10;
  
SELECT create_distributed_table('tripspart', 'vehid');
SELECT create_distributed_table('licences', 'vehid');
SELECT create_distributed_table('licences1', 'vehid');
SELECT create_distributed_table('licences2', 'vehid');
SELECT create_distributed_table('vehicles', 'vehid');

SELECT create_reference_table('regions');
SELECT create_reference_table('regions1');
SELECT create_reference_table('points');
SELECT create_reference_table('points1');
SELECT create_reference_table('periods');
SELECT create_reference_table('periods1');
SELECT create_reference_table('instants');
SELECT create_reference_table('instants1');

CREATE INDEX Licences_VehId_idx ON Licences USING btree (VehId);
CREATE INDEX Instants_Instant_idx ON Instants USING btree (Instant);
CREATE INDEX Periods_Period_gist_idx ON Periods USING gist (Period);
CREATE INDEX Points_geom_gist_idx ON Points USING gist(Geom);
CREATE INDEX Regions_geom_gist_idx ON Regions USING gist(Geom);
CREATE INDEX Trips_gist_idx ON Trips USING gist(trip);

-----------------------------------------------

explain 
SELECT L1.Licence AS Licence1, T2.VehId AS Car2Id,
whenTrue(tdwithin(T1.Trip, T2.Trip, 3.0)) AS Periods
FROM TripsPart T1, Licences1 L1, TripsPart T2, Vehicles V
WHERE T1.VehId = L1.VehId AND T2.VehId = V.VehId AND T1.VehId <> T2.VehId
AND T2.Trip && expandSpace(T1.trip, 3);

-----------------------------------------------




SELECT create_tile_partitions(
  table_name := 'trips',
  grid_table := 'grid'
);


SELECT inhrelid::regclass AS child -- optionally cast to text
FROM   pg_catalog.pg_inherits
WHERE  inhparent = 'publi.trips'::regclass;

explain
WITH Temp AS (
  SELECT TripId
  FROM trips_tile_t2 t, Communes c
  WHERE c.Name LIKE '%Ixelles%' AND ST_Intersects(t.trajectory, c.Geom)
  UNION
  SELECT TripId
  FROM trips_tile_t3 t, Communes c
  WHERE c.Name LIKE '%Ixelles%' AND ST_Intersects(t.trajectory, c.Geom)
  UNION
  SELECT TripId
  FROM trips_tile_t6 t, Communes c
  WHERE c.Name LIKE '%Ixelles%' AND ST_Intersects(t.trajectory, c.Geom)
  UNION
  SELECT TripId
  FROM trips_tile_t7 t, Communes c
  WHERE c.Name LIKE '%Ixelles%' AND ST_Intersects(t.trajectory, c.Geom) )
SELECT COUNT(*) 
FROM Temp;