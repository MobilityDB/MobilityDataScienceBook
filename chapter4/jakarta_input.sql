CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

DROP TABLE IF EXISTS JakartaInput;
CREATE TABLE JakartaInput(
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
  geom geometry(Point, 4813)
);

COPY JakartaInput(trj_id, driving_mode, osname, pingtimestamp, rawlat,
  rawlng, speed, bearing, accuracy)
FROM '/home/esteban/src/MobilityDataScienceBook/chapter4/jakarta/Jakarta.csv' 
  DELIMITER ',' CSV HEADER;

SELECT COUNT(*) FROM JakartaInput;
-- 3034553

/* Do the same for part-01 -> part-09 */

UPDATE JakartaInput SET 
  time = to_timestamp(pingtimestamp),
  geom = ST_Transform(ST_Point(rawlng, rawlat, 4326), 4813);

DROP TABLE IF EXISTS JakartaFiltered;
CREATE TABLE JakartaFiltered(trj_id, driving_mode, osname, rawlat, rawlng,
  speed, bearing, accuracy, geom, time) AS
SELECT trj_id, driving_mode, osname, rawlat, rawlng, speed, bearing, accuracy,
  geom, time
FROM JakartaInput
WHERE rawlat IS NOT NULL AND rawlng IS NOT NULL AND geom IS NOT NULL AND
  time IS NOT NULL;
  
DROP TABLE IF EXISTS trips;
CREATE TABLE trips(tripId integer PRIMARY KEY, drivingMode text,
  trip tgeompoint, speed tfloat, traj geometry);
INSERT INTO trips(tripId, drivingMode, trip, speed)
SELECT trj_id, driving_mode,
  tgeompointSeq(array_agg(tgeompoint(geom, time) ORDER BY time)),
  tfloatSeq(array_agg(tfloat(speed, time) ORDER BY time))
FROM JakartaFiltered
GROUP BY trj_id, driving_mode;

UPDATE trips SET traj = trajectory(trip);

DROP INDEX IF EXISTS trips_trip_idx;
CREATE INDEX trips_trip_idx ON trips USING GiST(trip);





