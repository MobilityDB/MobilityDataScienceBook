/*****************************************************************************/

SET timezone = 'America/New_York';

-- SCHEMA FOR 2024
DROP TABLE IF EXISTS citibikeInput;
CREATE TABLE citibikeInput(
  ride_id text, rideable_type text, started_at timestamptz,
  ended_at timestamptz, start_station_name text, start_station_id text, 
  end_station_name text, end_station_id text, start_lat float, 
  start_lng float, end_lat float, end_lng float, member_casual text, 
  start_point geometry(Point), end_point geometry(Point));
COPY citibikeInput(ride_id, rideable_type, started_at, ended_at, 
  start_station_name, start_station_id, end_station_name, end_station_id, 
  start_lat, start_lng, end_lat, end_lng, member_casual)
FROM '/Users/avaisman/tmp/DataMisc/NYCBike1-24/202403-citibike-tripdata.csv' 
DELIMITER ',' CSV HEADER;

UPDATE citibikeInput SET 
  start_point = ST_Point(start_lng, start_lat, 4326),
  end_point = ST_Point(end_lng, end_lat, 4326);

UPDATE citibikeInput SET 
 start_point = ST_Transform(start_point, 32118);
UPDATE citibikeInput SET 
 start_point = ST_Transform(end_point, 32118);

/******************************************************************************/
 

DROP TABLE IF EXISTS station_information;
CREATE TABLE station_information (
  station_id text,
  station_name text,
  station_lat float,
  station_lng float,
  geom geometry(Point)
);

INSERT INTO station_information(station_id, station_name, station_lat, station_lng)
SELECT DISTINCT ON (start_station_id)
  start_station_id, start_station_name,
  end_lat, end_lng
FROM citibikeInput
ORDER BY start_station_id; 

INSERT INTO station_information(station_id, station_name, station_lat, station_lng)
SELECT DISTINCT ON (end_station_id)
  end_station_id, end_station_name,
  end_lat, end_lng
FROM citibikeInput
WHERE end_station_id NOT IN (
  SELECT DISTINCT station_id FROM station_information)
ORDER BY end_station_id;

UPDATE station_information 
SET geom = ST_Point(station_lng, station_lat, 4326);
  
UPDATE station_information 
 SET geom = ST_Transform(geom, 32118);
  
/*****************************************************************************/

-- Needed to speed up the flow computation

CREATE INDEX citibikeInput_started_at_idx ON citibikeInput USING btree(started_at); 
CREATE INDEX citibikeInput_ended_at_idx ON citibikeInput USING btree(ended_at); 

DROP TABLE IF EXISTS time_bins;
CREATE TABLE time_bins(start_time, stoptime) AS 
SELECT h, h + interval '30 minutes'
FROM generate_series(timestamptz '2024-03-01 00:00:00',
  timestamptz '2024-03-31 23:30:00', interval '30 minutes') AS h;

/*****************************************************************************/

-- Temporary tables to compute the in and out flow


DROP TABLE IF EXISTS station_in;
CREATE TABLE station_in(station_id, start_time, in_count) AS
SELECT end_station_id, t.start_time, COUNT(*)
FROM time_bins t, citibikeInput c
WHERE t.start_time <= c.ended_at AND c.ended_at < t.stoptime
GROUP BY end_station_id, t.start_time;

DROP TABLE IF EXISTS station_out;
CREATE TABLE station_out(station_id, start_time, out_count) AS
SELECT start_station_id, t.start_time, COUNT(*)
FROM time_bins t, citibikeInput c
WHERE t.start_time <= c.started_at AND c.started_at < t.stoptime
GROUP BY start_station_id, t.start_time;

DROP TABLE IF EXISTS station_flow;
CREATE TABLE station_flow AS
WITH StationTimeBins(station_id, start_time) AS (
  SELECT station_id, start_time
  FROM station_information, time_bins ),
Temp1(station_id, start_time, in_count) AS (
  SELECT s.station_id, s.start_time, i.in_count
  FROM StationTimeBins s LEFT OUTER JOIN station_in i
    ON s.station_id = i.station_id AND s.start_time = i.start_time ),
Temp2(station_id, start_time, in_count, out_count) AS (
  SELECT t.station_id, t.start_time, t.in_count, o.out_count
  FROM Temp1 t LEFT OUTER JOIN station_out o
    ON t.station_id = o.station_id AND t.start_time = o.start_time )
SELECT * FROM Temp2 WHERE in_count IS NOT NULL OR out_count IS NOT NULL;

UPDATE station_flow SET
  in_count = COALESCE(in_count, 0),
  out_count = COALESCE(out_count, 0);

ALTER TABLE station_flow ALTER COLUMN in_count TYPE Float;
ALTER TABLE station_flow ALTER COLUMN out_count TYPE Float;

DROP TABLE station_in;
DROP TABLE station_out;

/*****************************************************************************/

DROP TABLE station_clusters_kmeans3;
CREATE TABLE station_clusters_kmeans3 AS
SELECT ST_ClusterKMeans(geom, 3) OVER () AS cid, station_id, geom
FROM station_information;

DROP TABLE station_clusters_kmeans4;
CREATE TABLE station_clusters_kmeans4 AS
SELECT ST_ClusterKMeans(geom, 4) OVER () AS cid, station_id, geom
FROM station_information;

DROP TABLE station_clusters_dbscan_500_3;
CREATE TABLE station_clusters_dbscan_500_3 AS
SELECT ST_ClusterDBSCAN(geom, 500, 3) OVER () AS cid, station_id, geom
FROM station_information;

DROP TABLE station_clusters_dbscan_500_4;
CREATE TABLE station_clusters_dbscan_500_4 AS
SELECT ST_ClusterDBSCAN(geom, 500, 4) OVER () AS cid, station_id, geom
FROM station_information;

/*****************************************************************************/


/*****************************************************************************/

SET timezone = 'America/New_York';
DROP TABLE IF EXISTS citibikeInput;
CREATE TABLE citibikeInput(
  ride_id text, rideable_type text, started_at timestamptz,
  ended_at timestamptz, start_station_name text, start_station_id text, 
  end_station_name text, end_station_id text, start_lat float, 
  start_lng float, end_lat float, end_lng float, member_casual text, 
  start_point geometry(Point), end_point geometry(Point));
COPY citibikeInput(ride_id, rideable_type, started_at, ended_at, 
  start_station_name, start_station_id, end_station_name, end_station_id, 
  start_lat, start_lng, end_lat, end_lng, member_casual)
FROM '/Users/avaisman/tmp/DataMisc/NYCBike1-24/202403-citibike-tripdata.csv' 
DELIMITER ',' CSV HEADER;
 
 
UPDATE citibikeInput SET 
  start_point = ST_Point(start_lng, start_lat, 4326),
  end_point = ST_Point(end_lng, end_lat, 4326);

UPDATE citibikeInput SET 
 start_point = ST_Transform(start_point, 32118);
UPDATE citibikeInput SET 
 start_point = ST_Transform(end_point, 32118);


DROP TABLE IF EXISTS station_information;
CREATE TABLE station_information (
  station_id text,
  station_name text,
  station_lat float,
  station_lng float,
  geom geometry(Point)
);

INSERT INTO station_information(station_id, station_name, station_lat, station_lng)
SELECT DISTINCT ON (start_station_id)
  start_station_id, start_station_name,
  -- OLD SCHEMA
  -- start_station_latitude, start_station_longitude
  end_lat, end_lng
FROM citibikeInput
ORDER BY start_station_id; 


INSERT INTO station_information(station_id, station_name, station_lat, station_lng)
SELECT DISTINCT ON (end_station_id)
  end_station_id, end_station_name,
  -- OLD SCHEMA
  -- end_station_latitude, end_station_longitude
  end_lat, end_lng
FROM citibikeInput
WHERE end_station_id NOT IN (
  SELECT DISTINCT station_id FROM station_information)
ORDER BY end_station_id;

UPDATE station_information 
SET geom = ST_Point(station_lng, station_lat, 4326);

UPDATE station_information 
SET geom = ST_Transform(geom, 32118);

CREATE INDEX citibikeInput_started_at_idx ON citibikeInput USING btree(started_at); 
CREATE INDEX citibikeInput_ended_at_idx ON citibikeInput USING btree(ended_at); 

DROP TABLE IF EXISTS time_bins;
CREATE TABLE time_bins(start_time, stoptime) AS 
SELECT h, h + interval '30 minutes'
FROM generate_series(timestamptz '2024-03-01 00:00:00',
  timestamptz '2024-03-31 23:30:00', interval '30 minutes') AS h;
 

DROP TABLE IF EXISTS station_in;
CREATE TABLE station_in(station_id, start_time, in_count) AS
SELECT end_station_id, t.start_time, COUNT(*)
FROM time_bins t, citibikeInput c
WHERE t.start_time <= c.ended_at AND c.ended_at < t.stoptime
GROUP BY end_station_id, t.start_time;

DROP TABLE IF EXISTS station_out;
CREATE TABLE station_out(station_id, start_time, out_count) AS
SELECT start_station_id, t.start_time, COUNT(*)
FROM time_bins t, citibikeInput c
WHERE t.start_time <= c.started_at AND c.started_at < t.stoptime
GROUP BY start_station_id, t.start_time;

DROP TABLE IF EXISTS station_flow;
CREATE TABLE station_flow AS
WITH StationTimeBins(station_id, start_time) AS (
  SELECT station_id, start_time
  FROM station_information, time_bins ),
Temp1(station_id, start_time, in_count) AS (
  SELECT s.station_id, s.start_time, i.in_count
  FROM StationTimeBins s LEFT OUTER JOIN station_in i
    ON s.station_id = i.station_id AND s.start_time = i.start_time ),
Temp2(station_id, start_time, in_count, out_count) AS (
  SELECT t.station_id, t.start_time, t.in_count, o.out_count
  FROM Temp1 t LEFT OUTER JOIN station_out o
    ON t.station_id = o.station_id AND t.start_time = o.start_time )
SELECT * FROM Temp2 WHERE in_count IS NOT NULL OR out_count IS NOT NULL;

UPDATE station_flow SET
  in_count = COALESCE(in_count, 0),
  out_count = COALESCE(out_count, 0);

DROP TABLE IF EXISTS  station_in;
DROP TABLE IF EXISTS  station_out;
 
ALTER TABLE station_flow ALTER COLUMN in_count TYPE float;
ALTER TABLE station_flow ALTER COLUMN out_count TYPE float;


DROP TABLE station_clusters_kmeans3;
CREATE TABLE station_clusters_kmeans3 AS
SELECT ST_ClusterKMeans(geom, 3) OVER () AS cid, station_id, geom
FROM station_information;

DROP TABLE station_clusters_kmeans4;
CREATE TABLE station_clusters_kmeans4 AS
SELECT ST_ClusterKMeans(geom, 4) OVER () AS cid, station_id, geom
FROM station_information;

DROP TABLE IF EXISTS  station_clusters_dbscan_500_10;
CREATE TABLE station_clusters_dbscan_500_10 AS
SELECT ST_ClusterDBSCAN(geom, 500, 10) OVER () AS cid, station_id, geom
FROM station_information;

DROP TABLE IF EXISTS  station_clusters_dbscan_750_10;
CREATE TABLE station_clusters_dbscan_750_10 AS
SELECT ST_ClusterDBSCAN(geom, 750, 10) OVER () AS cid, station_id, geom
FROM station_information;
