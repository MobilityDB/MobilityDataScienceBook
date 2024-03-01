CREATE OR REPLACE FUNCTION input_ais()
RETURNS text AS $$
DECLARE
  ais_size text;
  ais_count bigint;
  ships_size text;
  ships_count bigint;
BEGIN

  DROP TABLE IF EXISTS AISInputCSV;
  CREATE TABLE AISInputCSV(
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
    Callsign varchar(50),
    Name varchar(100),
    ShipType varchar(50),
    CargoType varchar(100),
    Width float,
    Length float,
    TypeOfPositionFixingDevice varchar(50),
    Draught float,
    Destination varchar(50),
    ETA varchar(50),
    DataSourceType varchar(50),
    SizeA float,
    SizeB float,
    SizeC float,
    SizeD float,
    Geom geometry(Point, 25832)
  );

  RAISE INFO 'Reading CSV files ...';

  COPY AISInputCSV(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus,
    ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length,
    TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType,
    SizeA, SizeB, SizeC, SizeD)
  FROM '/home/.../aisdk-2023-08-01.csv' DELIMITER ',' CSV HEADER;

  RAISE INFO 'Updating AISInputCSV table ...';
  
  UPDATE AISInputCSV SET
    NavigationalStatus = CASE NavigationalStatus WHEN 'Unknown value' THEN NULL END,
    IMO = CASE IMO WHEN 'Unknown' THEN NULL END,
    ShipType = CASE ShipType WHEN 'Undefined' THEN NULL END,
    TypeOfPositionFixingDevice = CASE TypeOfPositionFixingDevice
    WHEN 'Undefined' THEN NULL END,
    Geom = ST_SetSRID(ST_MakePoint( Longitude, Latitude ), 4326);

  RAISE INFO 'Computing AISInputCSVFiltered table ...';

  DROP TABLE IF EXISTS AISInputFiltered;
  CREATE TABLE AISInputFiltered AS
  SELECT DISTINCT ON(MMSI,T) *
  FROM AISInputCSV
  WHERE Longitude BETWEEN -16.1 and 32.88 AND Latitude BETWEEN 40.18 AND 84.17;

  RAISE INFO 'Creating AISInput table ...';

  CREATE TABLE AISInput AS
  SELECT MMSI, T, SOG, COG, Geom
  FROM AISInputCSVFiltered;

  SELECT pg_size_pretty(pg_total_relation_size('aisinput')) INTO ais_size;
  RAISE INFO 'Size of the AISInput table: %', ais_size;
  SELECT COUNT(*) INTO ais_count FROM AISInput;
  RAISE INFO 'Number of rows in the AISInput table: %', ais_count;

  RAISE INFO 'Creating Ships table ...';

  DROP TABLE IF EXISTS Ships;
  CREATE TABLE Ships(MMSI, Trip, SOG, COG) AS
  SELECT MMSI,
    tgeompoint_seq(array_agg(tgeompoint(ST_Transform(Geom, 25832), T) ORDER BY T)),
    tfloatseq(array_agg(tfloat_inst(SOG, T) ORDER BY T) FILTER (WHERE SOG IS NOT NULL)),
    tfloatseq(array_agg(tfloat_inst(COG, T) ORDER BY T) FILTER (WHERE COG IS NOT NULL))
  FROM AISInput
  GROUP BY MMSI;

  -- DROP TABLE IF EXISTS Ships;
  -- CREATE TABLE Ships(MMSI, Trip) AS
  -- SELECT MMSI, tgeompoint_seqset_gaps(
    -- array_agg(tgeompoint_inst(ST_Transform(Geom, 25832), T) ORDER BY T),
    -- interval '1 hour')
  -- FROM AISInputFiltered
  -- GROUP BY MMSI;

  SELECT pg_size_pretty(pg_total_relation_size('ships')) INTO ships_size;
  RAISE INFO 'Size of the Ships table: %', ships_size;
  SELECT COUNT(*) INTO ships_count FROM Ships;
  RAISE INFO 'Number or rows in the Ships table: %', ships_count;

  ALTER TABLE Ships ADD COLUMN Traj geometry;
  UPDATE Ships SET Traj = trajectory(Trip);

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql' STRICT;
