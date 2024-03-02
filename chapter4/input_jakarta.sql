DROP TABLE IF EXISTS JakartaInputCSV;
CREATE TABLE JakartaInputCSV(
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

COPY JakartaInputCSV(trj_id, driving_mode, osname, pingtimestamp, rawlat,
  rawlng, speed, bearing, accuracy)
FROM '/Users/avaisman/tmp/DataMisc/Jakarta.csv' DELIMITER ',' CSV HEADER;

SELECT COUNT(*) FROM JakartaInputCSV;
-- 3034553

/* Do the same for part-01 -> part-09 */

ALTER TABLE JakartaInputCSV ADD COLUMN time timestamp;
UPDATE JakartaInputCSV SET
time = to_timestamp(pingtimestamp);

ALTER TABLE JakartaInputCSV ADD COLUMN geom geometry;
UPDATE JakartaInputCSV SET
geom = ST_SetSRID(ST_Point(rawlng, rawlat), 4326);

ALTER TABLE JakartaInputCSV ADD COLUMN geom4813 geometry;
UPDATE JakartaInputCSV SET
geom4813 = ST_Transform(geom, 4813);

CREATE TABLE JakartaInput(trj_id, driving_mode, osname, rawlat, rawlng,
  speed, bearing, accuracy, geom, time) AS
SELECT trj_id, driving_mode, osname, rawlat, rawlng, speed, bearing, accuracy,
  geom4813 , time
FROM JakartaInputCSV
WHERE rawlat IS NOT NULL AND rawlng IS NOT NULL AND geom IS NOT NULL AND
  time IS NOT NULL;
  
DROP TABLE IF EXISTS trips;
CREATE TABLE trips(trj_id, driving_mode,  trip,speed) AS
SELECT trj_id, driving_mode,  tgeompoint_seq(array_agg(tgeompoint_inst(Geom, time) 
  ORDER BY time)), tfloat_seq(array_agg(tfloat_inst(speed, time) 
  ORDER BY time))
FROM JakartaInput
GROUP BY trj_id, driving_mode;
ALTER TABLE trips ADD COLUMN traj geometry;
UPDATE trips SET traj= trajectory(trip);



CREATE INDEX trips_trip_idx ON trips USING GiST(trip);





