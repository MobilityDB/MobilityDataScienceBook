CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;

DROP TABLE IF EXISTS FlightsInput;
CREATE TABLE FlightsInput(
  Et BIGINT,
  ICAO24 VARCHAR(20), 
  Lat FLOAT,
  Lon FLOAT, 
  Velocity FLOAT,
  Heading FLOAT, 
  VertRate FLOAT, 
  CallSign VARCHAR(10),
  OnGround BOOLEAN,
  Alert BOOLEAN, 
  SPI BOOLEAN, 
  Squawk INTEGER, 
  BaroAltitude NUMERIC(7,2),
  GeoAltitude NUMERIC(7,2), 
  LastPosUpdate NUMERIC(13,3),
  LastContact NUMERIC(13,3)
);

DO 
$$DECLARE 
-- prefixpath text= '/home/user/data/opensky/states-2020-06-01/states_2020-06-01-'; 
prefixpath text= '/mnt/e/Data/opensky/states_2020-06-01-'; 
path text;


BEGIN
  FOR rec in 0..23 LOOP
    path:= prefixpath || trim(to_char(rec, '09')) || '.csv'; -- fill with 0s
    EXECUTE format('COPY FlightsInput(Et, ICAO24, Lat, Lon, Velocity, Heading,
      VertRate, CallSign, OnGround, Alert, SPI, Squawk, BaroAltitude, 
      GeoAltitude, LastPosUpdate, LastContact)
      FROM %L WITH DELIMITER '','' CSV HEADER', path);
    COMMIT;
    RAISE Notice 'Inserting %', path;
  END LOOP;
END
$$;

ALTER TABLE FlightsInput
  ADD COLUMN EtTs TIMESTAMP,
  ADD COLUMN LastPosUpdateTs TIMESTAMP,
  ADD COLUMN LastContactTs TIMESTAMP;
UPDATE FlightsInput SET
  EtTs = to_timestamp(Et),
  LastPosUpdateTs = to_timestamp(LastPosUpdate),
  LastContactTs = to_timestamp(LastContact);

-- SELECT pg_size_pretty( pg_total_reLation_size('FlightsInput') );

ALTER TABLE FlightsInput
ADD COLUMN Geo geometry(Point, 4326);
UPDATE FlightsInput SET Geo = ST_Point(Lon, Lat, 4326);

WITH ICAO24_WithNullLon AS (
  SELECT ICAO24, COUNT(Lat)
  FROM FlightsInput
  GROUP BY ICAO24
  HAVING COUNT(Lon) = 0 )
DELETE FROM FlightsInput
WHERE ICAO24 IN (SELECT ICAO24 FROM ICAO24_WithNullLon);
DELETE FROM FlightsInput WHERE Squawk IS NULL;

CREATE INDEX ICAO24_time_index ON FlightsInput (ICAO24, EtTs);
 
DROP TABLE IF EXISTS FlightsDay;
CREATE TABLE FlightsDay(ICAO24, Flight, Velocity, Heading, VertRate,
  CallSign, Alert, GeoAltitude) AS
SELECT ICAO24,
  tgeogpointSeq(array_agg(tgeogpoint(Geo, EtTs) ORDER BY EtTs)
    FILTER (WHERE Geo IS NOT NULL)),
  tfloatSeq(array_agg(tfloat(Velocity, EtTs) ORDER BY EtTs)
    FILTER (WHERE Velocity IS NOT NULL)),
  tfloatSeq(array_agg(tfloat(Heading, EtTs) ORDER BY EtTs)
    FILTER (WHERE Heading IS NOT NULL)),
  tfloatSeq(array_agg(tfloat(VertRate, EtTs) ORDER BY EtTs)
    FILTER (WHERE VertRate IS NOT NULL)),
  ttextSeq(array_agg(ttext(CallSign, EtTs) ORDER BY EtTs)
    FILTER (WHERE CallSign IS NOT NULL)),
  tboolSeq(array_agg(tbool(Alert, EtTs) ORDER BY EtTs)
    FILTER (WHERE Alert IS NOT NULL)),
  tfloatSeq(array_agg(tfloat(GeoAltitude, EtTs) ORDER BY EtTs)
    FILTER (WHERE GeoAltitude IS NOT NULL))
FROM FlightsInput
GROUP BY ICAO24;

DROP TABLE IF EXISTS Flights;
CREATE TABLE Flights(ICAO24, CallSign, FlightPeriod, Flight, Velocity, Heading,
  VertRate, Alert, GeoAltitude) AS
SELECT ICAO24, (Rec).Value, (Rec).Time,
  atTime(Flight, (Rec).Time), atTime(Velocity, (Rec).Time),
  atTime(Heading, (Rec).Time), atTime(VertRate, (Rec).Time),
  atTime(Alert, (Rec).Time), atTime(GeoAltitude, (Rec).Time)
FROM FlightsDay f, unnest(f.CallSign) AS Rec;

-- Selection of the flights that intersect the area covered by SRID 31

DROP TABLE IF EXISTS SRID3112;
CREATE TABLE SRID3112(Env) AS
SELECT ST_MakeEnvelope(93.41, -60.55, 173.34, -8.47,  4326);

 DROP TABLE IF EXISTS  FlightsAU;
CREATE TABLE FlightsAU(ICAO24, CallSign, FlightPeriod, Flight, Velocity, Heading, 
 VertRate, Alert, GeoAltitude) AS
 SELECT ICAO24, CallSign, FlightPeriod, transform(Flight, 3112)::tgeompoint,
  Velocity, Heading, VertRate, Alert, GeoAltitude
FROM Flights f, SRID3112 s
WHERE ST_GeometryType(trajectory(flight::tgeompoint))='ST_LineString' AND 
  eintersects(f.flight, s.Env);
