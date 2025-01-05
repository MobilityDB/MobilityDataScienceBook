CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

DROP TABLE IF EXISTS SingaporeInput;
CREATE TABLE SingaporeInput(
  trj_id integer,
  driving_mode text,
  osname text,
  pingtimestamp bigint,
  rawlat float,
  rawlng float,
  speed float,
  bearing float,
  accuracy float,
  time timestamp,
  geom geometry
);

COPY SingaporeInput(trj_id, driving_mode, osname, pingtimestamp, rawlat,
  rawlng, speed, bearing, accuracy)
FROM '/home/esteban/src/MobilityDataScienceBook/chapter4/singapore/singapore_part-00.csv' 
  DELIMITER ',' CSV HEADER;

SELECT COUNT(*) FROM SingaporeInput;
-- 3034553

/* Do the same for part-01 -> part-09 */

UPDATE SingaporeInput SET 
  time = to_timestamp(pingtimestamp),
  geom = ST_Transform(ST_Point(rawlng, rawlat, 4326), 3414);

DROP TABLE IF EXISTS SingaporeFiltered;
CREATE TABLE SingaporeFiltered(trj_id, driving_mode, osname, rawlat, rawlng,
  speed, bearing, accuracy, geom, time) AS
SELECT trj_id, driving_mode, osname, rawlat, rawlng, speed, bearing, accuracy,
  geom, time
FROM SingaporeInput
WHERE rawlat IS NOT NULL AND rawlng IS NOT NULL AND geom IS NOT NULL AND
  time IS NOT NULL;
  
DROP TABLE IF EXISTS trips;
CREATE TABLE trips(tripid integer PRIMARY KEY, trip tgeompoint, trajectory geometry);
INSERT INTO trips(tripid, trip)
SELECT trj_id, 
  tgeompointSeq(array_agg(tgeompoint(geom, time) ORDER BY time))
FROM SingaporeFiltered
GROUP BY trj_id;

UPDATE trips SET trajectory = trajectory(trip);

DROP INDEX IF EXISTS trips_trip_idx;
CREATE INDEX trips_trip_idx ON trips USING GiST(trip);

