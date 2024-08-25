DROP TABLE IF EXISTS SingaporeInputCSV;
CREATE TABLE SingaporeInputCSV(
  trj_id integer,
  driving_mode text,
  osname text,
  pingtimestamp bigint,
  rawlat float,
  rawlng float,
  speed float,
  bearing float,
  accuracy float
);

COPY SingaporeInputCSV(trj_id, driving_mode, osname, pingtimestamp, rawlat,
  rawlng, speed, bearing, accuracy)
FROM '/home/esteban/src/MobilityDataScience/chapter4/singapore_part-00.csv' 
  DELIMITER ',' CSV HEADER;

SELECT COUNT(*) FROM SingaporeInputCSV;
-- 3034553

/* Do the same for part-01 -> part-09 */

ALTER TABLE SingaporeInputCSV ADD COLUMN time timestamp;
UPDATE SingaporeInputCSV SET
time = to_timestamp(pingtimestamp);

ALTER TABLE SingaporeInputCSV ADD COLUMN geom geometry;
UPDATE SingaporeInputCSV SET
geom = ST_SetSRID(ST_Point(rawlng, rawlat), 4326);

ALTER TABLE SingaporeInputCSV ADD COLUMN geom3414 geometry;
UPDATE SingaporeInputCSV SET
geom3414 = ST_Transform(geom, 3414);

CREATE TABLE SingaporeInput (trj_id, driving_mode, osname, rawlat, rawlng,
  speed, bearing, accuracy, geom, time) AS
SELECT trj_id, driving_mode, osname, rawlat, rawlng, speed, bearing, accuracy,
  geom3414, time
FROM SingaporeInputCSV
WHERE rawlat IS NOT NULL AND rawlng IS NOT NULL AND geom IS NOT NULL AND
  time IS NOT NULL;
  
CREATE TABLE trips(trj_id, trip) AS
SELECT trj_id, tgeompoint_seq(array_agg(tgeompoint_inst(Geom, time) 
  ORDER BY time))
FROM SingaporeInput
GROUP BY trj_id;

ALTER TABLE trips ADD COLUMN traj geometry;
UPDATE trips SET traj= trajectory(trip);

CREATE INDEX trips_trip_idx ON trips USING GiST(trip);

