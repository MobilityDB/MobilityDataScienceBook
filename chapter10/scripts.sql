CREATE EXTENSION PostGIS;

--loading data (similar to CH5)
DROP TABLE AISInput;
CREATE TABLE AISInput(
T timestamp,
TypeOfMobile varchar(50),
MMSI integer,
Latitude float,
Longitude float,
NavigationalStatus varchar(60),
ROT float,
SOG float,
COG float,
Heading integer,
IMO varchar(50),
CallSign varchar(50),
Name varchar(100),
ShipType varchar(50),
CargoType varchar(100),
Width float, Length float,
TypeOfPositionFixingDevice varchar(50),
Draught float,
Destination varchar(50),
ETA varchar(50),
DataSourceType varchar(50),
SizeA float,
SizeB float,
SizeC float,
SizeD float,
Geom geometry(Point, 4326)
);

SET TimeZone = 'UTC';
SET DateStyle = 'ISO, DMY';

COPY AISInput(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus,
  ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length,
  TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType,
  SizeA, SizeB, SizeC, SizeD)
FROM '/home/mahmoud/Desktop/MobilityDataScience/Book/my_notebooks/aisdk-2024-03-01.csv' DELIMITER  ',' CSV HEADER;

-- Initial filtering and transformation:
UPDATE AISInput
SET Latitude= NULL, Longitude= NULL
WHERE Longitude not between -180 and 180 OR Latitude not between -90 and 90;
-- 192,330 rows affected in 4 s 302 ms

UPDATE AISInput SET
   Geom = ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326);
-- 15,512,927 rows affected in 58 s 703 ms


ALTER TABLE AISInput
  ADD COLUMN GeomProj geometry(Point, 25832);

UPDATE AISInput SET
  GeomProj = ST_Transform(Geom, 25832);
-- 15,512,927 rows affected in 1 m 5 s 145 ms

DROP TABLE IF EXISTS AISInputSample;
CREATE TABLE AISInputSample AS
    SELECT *
    FROM AISInput
    WHERE EXTRACT(HOUR FROM T) BETWEEN 9 AND 10;

CREATE INDEX AIS_geom_x
  ON AISInputSample
  USING GIST (GeomProj);

CREATE TABLE clusters2 AS
    SELECT mmsi, t, GeomProj, ST_ClusterDBSCAN(GeomProj, eps => 100, minpoints => 20) over () AS cid
    FROM AISInputSample;


SELECT distinct cid from clusters2