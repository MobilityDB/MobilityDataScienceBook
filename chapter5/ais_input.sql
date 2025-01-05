/*****************************************************************************
 * SQL script to input one day of AIS data and store it in MobilityDB
 *****************************************************************************/

CREATE OR REPLACE FUNCTION ais_input()
RETURNS text AS $$
DECLARE
  table_size text;
  table_count text;
BEGIN
  -- Set parameters for input timestamps in the CSV file
  SET TimeZone = 'UTC';
  SET DateStyle = 'ISO, DMY';

  -- Create input table to hold CSV records
  DROP TABLE IF EXISTS AISInput;
  CREATE TABLE AISInput(
    T timestamp,
    TypeOfMobile varchar(100),
    MMSI integer,
    Latitude float,
    Longitude float,
    NavigationalStatus varchar(100),
    ROT float,
    SOG float,
    COG float,
    Heading integer,
    IMO varchar(100),
    CallSign varchar(100),
    Name varchar(100),
    ShipType varchar(100),
    CargoType varchar(100),
    Width float,
    Length float,
    TypeOfPositionFixingDevice varchar(100),
    Draught float,
    Destination varchar(100),
    ETA varchar(100),
    DataSourceType varchar(100),
    SizeA float,
    SizeB float,
    SizeC float,
    SizeD float,
    Geom geometry(Point, 4326)
  );

  -- Input CSV records
  RAISE INFO 'Reading CSV file into table AISInput ...';
  COPY AISInput(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus,
    ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length,
    TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType,
    SizeA, SizeB, SizeC, SizeD)
  FROM '/home/esteban/src/ais/aisdk-2024-03-01.csv' DELIMITER ',' CSV HEADER;

  RAISE INFO 'Updating AISInput table ...';
  -- Set to NULL out-of-range values of latitude and longitude
  UPDATE AISInput
  SET Latitude = NULL, Longitude = NULL
  WHERE Longitude NOT BETWEEN -180 AND 180 OR Latitude NOT BETWEEN -90 AND 90;
  -- Create point geometry 
  UPDATE AISInput SET
    Geom = ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326);
  -- Set to NULL 'undefined' values and add geometry to the records
  UPDATE AISInput SET
    IMO = CASE WHEN IMO = 'Unknown' THEN NULL ELSE IMO END,
    Destination = CASE WHEN Destination ILIKE 'Unknown' THEN NULL ELSE
      Destination END,
    NavigationalStatus = CASE WHEN NavigationalStatus = 'Unknown value' THEN
      NULL ELSE NavigationalStatus END,
    ShipType = CASE WHEN ShipType = 'Undefined' OR ShipType = 'Other' THEN
      NULL ELSE ShipType END,
    CargoType = CASE WHEN CargoType = 'No additional information' THEN
      NULL ELSE CargoType END,
    CallSign = CASE WHEN CallSign = 'Unknown' THEN NULL ELSE CallSign END,
    TypeOfPositionFixingDevice = CASE WHEN TypeOfPositionFixingDevice = 'Undefined' 
      THEN NULL ELSE TypeOfPositionFixingDevice END
  WHERE IMO = 'Unknown' OR Destination ILIKE 'Unknown' OR 
    NavigationalStatus = 'Unknown value' OR ShipType = 'Undefined' OR 
    ShipType = 'Other' OR CargoType = 'No additional information' OR 
    CallSign = 'Unknown' OR TypeOfPositionFixingDevice = 'Undefined';

  -- Filter out duplicate timestamps and valid but out-of-range values of
  -- latitude and longitude
  RAISE INFO 'Creating the AISInputFiltered table ...';
  DROP TABLE IF EXISTS AISInputFiltered;
  CREATE TABLE AISInputFiltered AS
  SELECT DISTINCT ON (MMSI,T) *
  FROM AISInput
  WHERE Longitude BETWEEN -16.1 AND 32.88 AND Latitude BETWEEN 40.18 AND 84.17
  ORDER BY MMSI, T;

  -- Create table with only the columns used for creating temporal types
  RAISE INFO 'Creating AISInputTarget table ...';
  DROP TABLE IF EXISTS AISInputTarget;
  CREATE TABLE AISInputTarget AS
  SELECT MMSI, Length, T, SOG, COG, Geom
  FROM AISInputFiltered;

  -- Create table with temporal types
  RAISE INFO 'Creating the Ships table ...';
  DROP TABLE IF EXISTS Ships;
  -- Notice that we do not fill the Length attribute since for a single MMSI
  -- some records have NULL and others have a value
  CREATE TABLE Ships(MMSI, Trip, SOG, COG) AS
  SELECT MMSI,
    tgeompointSeq(array_agg(tgeompoint(ST_Transform(Geom, 25832), T) ORDER BY T)),
    tfloatSeq(array_agg(tfloat(SOG, T) ORDER BY T) FILTER (WHERE SOG IS NOT NULL)),
    tfloatSeq(array_agg(tfloat(COG, T) ORDER BY T) FILTER (WHERE COG IS NOT NULL))
  FROM AISInputTarget
  GROUP BY MMSI;
  -- Add the trajectory colum
  ALTER TABLE Ships ADD COLUMN Traj geometry;
  UPDATE Ships SET Traj = trajectory(Trip);
  -- Fill the length attribute if possible
  /* 
    SELECT MMSI, array_agg(DISTINCT length) 
    FROM AISInputFiltered GROUP BY MMSI HAVING COUNT(DISTINCT length) > 1;
       mmsi    |   array_agg
    -----------+----------------
     211913000 | {89,90,NULL}
     219000429 | {141,142,NULL}
     219015338 | {15,183,NULL}
     219016555 | {96,98,NULL}
     219016938 | {96,98,NULL}
     219027893 | {26,28,NULL}
     235113838 | {92,93,NULL}
     265610950 | {15,149,NULL}
  */
  DROP TABLE IF EXISTS Lengths;
  CREATE TABLE Lengths(MMSI, Length) AS 
    SELECT MMSI, MAX(Length) FROM AISInputTarget GROUP BY MMSI; 
  ALTER TABLE Ships ADD COLUMN Length float;
  UPDATE Ships s SET 
    length = (SELECT length FROM Lengths l WHERE s.MMSI = l.MMSI);

  -- Print statistics about the tables
  RAISE INFO '--------------------------------------------------------------';
  SELECT pg_size_pretty(pg_total_relation_size('AISInput')) INTO table_size;
  SELECT to_char(COUNT(*), 'fm999G999G999') FROM AISInput INTO table_count;
  RAISE INFO 'Size of the AISInput table: %, % rows', table_size, table_count;
  SELECT pg_size_pretty(pg_total_relation_size('AISInputFiltered')) INTO table_size;
  SELECT to_char(COUNT(*), 'fm999G999G999') FROM AISInputFiltered INTO table_count;
  RAISE INFO 'Size of the AISInputFiltered table: %, % rows', table_size, table_count;
  SELECT pg_size_pretty(pg_total_relation_size('AISInputTarget')) INTO table_size;
  SELECT to_char(COUNT(*), 'fm999G999G999') FROM AISInputTarget INTO table_count;
  RAISE INFO 'Size of the AISInputTarget table: %, % rows', table_size, table_count;
  SELECT pg_size_pretty(pg_total_relation_size('Ships')) INTO table_size;
  SELECT to_char(COUNT(*), 'fm999G999G999') FROM Ships INTO table_count;
  RAISE INFO 'Size of the Ships table: %, % rows', table_size, table_count;
  RAISE INFO '--------------------------------------------------------------';

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql' STRICT;
