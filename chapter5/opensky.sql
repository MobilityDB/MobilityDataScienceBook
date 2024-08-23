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
 

CREATE EXTENSION MobilityDB Cascade;

-- COPY FlightsInput(Et, ICAO24, Lat, Lon, Velocity, Heading,VertRate, CallSign, 
-- OnGround, Alert, SPI, Squawk, BaroAltitude, GeoAltitude,  
-- LastPosUpdate, LastContact)
-- FROM '/Users/.../tmp/states-2020-06-01' DELIMITER ',' CSV HEADER;

-- DELETE FROM FlightsInput

DO 
$$DECLARE 
prefixpath text= '/Users/.../tmp/states-2020-06-01/states_2020-06-01-'; 
path text;

BEGIN
    FOR rec in 0..23 LOOP
	  path:= prefixpath || trim(to_char(rec, '09')) ||   
       '.csv'; -- fill with 0s
	 
	 EXECUTE format('COPY FlightsInput(Et, ICAO24, Lat, Lon, Velocity, Heading,VertRate, CallSign, OnGround, Alert, SPI, Squawk, BaroAltitude, GeoAltitude,  LastPosUpdate, LastContact)
         FROM %L WITH DELIMITER '','' CSV HEADER', path);
	 COMMIT;
	 Raise Notice 'Inserting %', path;
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

SELECT pg_size_pretty( pg_total_relation_size('FlightsInput') );

ALTER TABLE FlightsInput
ADD COLUMN  geom geometry(Point, 4326);
UPDATE FlightsInput SET geom = ST_SetSRID(ST_MakePoint(Lon, Lat), 4326);


WITH ICAO24_WithNullLon AS (
 SELECT icao24, COUNT(lat)
FROM FlightsInput
GROUP BY icao24
HAVING COUNT(lon) = 0
)

DELETE
FROM FlightsInput
WHERE ICAO24 IN
--   this SELECT statement is needed for the IN statement to 
--   compare against a list
            (SELECT ICAO24 FROM ICAO24_WithNullLon);


CREATE INDEX icao24_time_index ON FlightsInput (ICAO24, EtTs);
DELETE from  FlightsInput WHERE Squawk IS NULL



===================================================

CREATE TABLE FlightsDay(ICAO24, Trip, Velocity, Heading, VertRate,
CallSign, Alert, GeoAltitude) AS (
SELECT ICAO24,
tgeompointSeq(array_agg(tgeompoint(Geom, EtTs) ORDER BY EtTs)
  FILTER (WHERE Geom IS NOT NULL)),
tfloatSeq(array_agg(tfloat(Velocity, EtTs) ORDER BY EtTs)
  FILTER (WHERE Velocity IS NOT NULL)),
tfloatSeq(array_agg(tfloat(Heading, EtTs) ORDER BY EtTs)
  FILTER (WHERE Heading IS NOT NULL)),
tfloatSeq(array_agg(tfloat(VertRate, EtTs) ORDER BY EtTs)
  FILTER (WHERE VertRate IS NOT NULL)),
ttextSeq(array_agg(ttext(CallSign, EtTs) ORDER BY EtTs)
  FILTER (WHERE CallSign IS NOT NULL)),
tbooleanSeq(array_agg(tboolean(Alert, EtTs) ORDER BY EtTs)
  FILTER (WHERE Alert IS NOT NULL)),
tfloatSeq(array_agg(tfloat(GeoAltitude, EtTs) ORDER BY EtTs)
  FILTER (WHERE GeoAltitude IS NOT NULL))
FROM FlightsInput
GROUP BY ICAO24 );

===============================================

CREATE TABLE Flight(ICAO24, CallSign, FlightPeriod, Trip, Velocity, Heading,
  VertRate, Alert, GeoAltitude) AS (
-- Now, CallSign sequence is unpacked into rows to split all other temporal sequences
  WITH airframeTrajWithUnpackedCallSign AS (
    SELECT ICAO24, Trip, Velocity, Heading, VertRate, Alert, GeoAltitude,
      startValue(unnest(segments(CallSign))) AS StartValueCallSign,
      unnest(segments(CallSign))::tstzspan AS CallSignSegmentPeriod
  FROM AirframeTraj)
SELECT ICAO24 AS ICAO24,
  StartValueCallSign AS CallSign,
  CallSignSegmentPeriod AS FlightPeriod,
  atTime(Trip, CallSignSegmentPeriod) AS Trip,
  atTime(Velocity, CallSignSegmentPeriod) AS Velocity,
  atTime(heading, CallSignSegmentPeriod) AS heading,
  atTime(VertRate, CallSignSegmentPeriod) AS VertRate,
  atTime(alert, CallSignSegmentPeriod) AS Alert,
  atTime(GeoAltitude, CallSignSegmentPeriod) AS GeoAltitude
FROM AirframeTrajWithUnpackedCallSign;

===========================================

