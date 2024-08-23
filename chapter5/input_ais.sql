/*****************************************************************************
 * SQL script to input one day of AIS data and store it in MobilityDB
 *****************************************************************************/

CREATE OR REPLACE FUNCTION input_ais()
RETURNS text AS $$
DECLARE
  table_size text;
  table_count text;
BEGIN
  -- Set parameters for input timestamps in the CSV file
  SET TimeZone = 'UTC';
  SET DateStyle = 'ISO, DMY';

  -- Create input table to hold CSV records
  DROP TABLE IF EXISTS AISInputCSV;
  CREATE TABLE AISInputCSV(
    T timestamp,
    TypeOfMobile varchar(100),
    MMSI integer,
    Latitude float,
    Longitude float,
    navigationalStatus varchar(100),
    ROT float,
    SOG float,
    COG float,
    Heading integer,
    IMO varchar(100),
    Callsign varchar(100),
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
  RAISE INFO 'Reading CSV file ...';
  COPY AISInputCSV(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus,
    ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length,
    TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType,
    SizeA, SizeB, SizeC, SizeD)
  FROM '/home/.../aisdk-2023-08-01.csv' DELIMITER ',' CSV HEADER;

  -- Set null values and add geometry to the records
  RAISE INFO 'Updating AISInputCSV table ...';
  UPDATE AISInputCSV SET
    NavigationalStatus = CASE NavigationalStatus WHEN 'Unknown value' THEN NULL END,
    IMO = CASE IMO WHEN 'Unknown' THEN NULL END,
    ShipType = CASE ShipType WHEN 'Undefined' THEN NULL END,
    TypeOfPositionFixingDevice = CASE TypeOfPositionFixingDevice
    WHEN 'Undefined' THEN NULL END,
    Geom = ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326);

  -- Filter out erroneous records
  RAISE INFO 'Computing AISInputFiltered table ...';
  DROP TABLE IF EXISTS AISInputFiltered;
  CREATE TABLE AISInputFiltered AS
  SELECT DISTINCT ON(MMSI,T) *
  FROM AISInputCSV
  WHERE MMSI <> 0 AND
    (Longitude BETWEEN -16.1 and 32.88 AND Latitude BETWEEN 40.18 AND 84.17)
  ORDER BY MMSI, T;


  -- Create table with only the columns used for creating temporal types
  RAISE INFO 'Creating AISInput table ...';
  DROP TABLE IF EXISTS AISInput;
  CREATE TABLE AISInput AS
  SELECT MMSI, Length, T, SOG, COG, Geom
  FROM AISInputFiltered;

  -- Create table with temporal types
  RAISE INFO 'Creating Ships table ...';
  DROP TABLE IF EXISTS Ships;
  -- Notice that we do not fill the Length attribute since for a single MMSI
  -- some records have NULL and others have a value
  CREATE TABLE Ships(MMSI, Trip, SOG, COG) AS
  SELECT MMSI,
    tgeompointSeq(array_agg(tgeompoint(ST_Transform(Geom, 25832), T) ORDER BY T)),
    tfloatSeq(array_agg(tfloat(SOG, T) ORDER BY T) FILTER (WHERE SOG IS NOT NULL)),
    tfloatSeq(array_agg(tfloat(COG, T) ORDER BY T) FILTER (WHERE COG IS NOT NULL))
  FROM AISInput
  GROUP BY MMSI;
  -- Add the trajectory column
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
    SELECT MMSI, MAX(Length) FROM AISInput GROUP BY MMSI; 
  ALTER TABLE Ships ADD COLUMN Length float;
  UPDATE Ships s SET 
    length = (SELECT length FROM Lengths l WHERE s.MMSI = l.MMSI);

  -- Print statistics about the tables
  RAISE INFO '--------------------------------------------------------------';
  SELECT pg_size_pretty(pg_total_relation_size('AISInputCSV')) INTO table_size;
  SELECT to_char(COUNT(*), 'fm999G999G999') FROM AISInputCSV INTO table_count;
  RAISE INFO 'Size of the AISInputCSV table: %, % rows', table_size, table_count;
  SELECT pg_size_pretty(pg_total_relation_size('AISInputFiltered')) INTO table_size;
  SELECT to_char(COUNT(*), 'fm999G999G999') FROM AISInputFiltered INTO table_count;
  RAISE INFO 'Size of the AISInputFiltered table: %, % rows', table_size, table_count;
  SELECT pg_size_pretty(pg_total_relation_size('AISInput')) INTO table_size;
  SELECT to_char(COUNT(*), 'fm999G999G999') FROM AISInput INTO table_count;
  RAISE INFO 'Size of the AISInput table: %, % rows', table_size, table_count;
  SELECT pg_size_pretty(pg_total_relation_size('Ships')) INTO table_size;
  SELECT to_char(COUNT(*), 'fm999G999G999') FROM Ships INTO table_count;
  RAISE INFO 'Size of the Ships table: %, % rows', table_size, table_count;
  RAISE INFO '--------------------------------------------------------------';

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql' STRICT;
