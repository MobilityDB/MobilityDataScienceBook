﻿11.5.1 Setting the scenario: Building Trajectories and Segments


CREATE EXTENSION MobilityDB Cascade
ALTER TABLE VehiclePosition ADD COLUMN Geom geometry (point,4326);

UPDATE VehiclePosition SET Geom = ST_Makepoint(longitude, latitude);


DROP TABLE IF EXISTS ActualTrips;

CREATE TABLE ActualTrips (Trip_id text, Trip tgeompoint);

INSERT INTO ActualTrips(Trip_id, Trip)
WITH Temp AS (
-- We use DISTINCT since we observed duplicate tuples
 SELECT DISTINCT Trip_id, ST_Transform(Geom, 3059) AS geom,
  to_timestamp(timestamp) AS t
  FROM VehiclePosition )

SELECT Trip_id, tgeompointSeq(array_agg(tgeompoint(Geom, t) ORDER BY t)
FILTER (WHERE Geom IS NOT NULL)) AS trip
FROM Temp
GROUP BY Trip_id;
-- 871 rows affected in 616 ms


ALTER TABLE ActualTrips ADD COLUMN traj geometry;
UPDATE ActualTrips set traj = trajectory(trip);


CREATE TABLE TripStops AS
  WITH Tstops AS (
    SELECT a.trip_id AS actual_trip_id, ad.trip_id AS schedule_trip_id, s.stop_id,
       s.stop_name, ad.stop_sequence, s.stop_loc::geometry, t_arrival AS schedule_time,
       nearestApproachInstant(a.trip, ST_Transform(s.stop_loc::geometry, 3059))
       AS stopInstant
    FROM ActualTrips a, arrivals_departures ad, Stops s
    WHERE ad.date= '2024-09-24'::timestamp AND ad.stop_id= s.stop_id AND
            regexp_replace(a.trip_id, '([^-]+-[^-]+-[^-]+)-[0-9]+(-.*)', '\1\2') =
            regexp_replace(ad.trip_id, '([^-]+-[^-]+-[^-]+)-[0-9]+(-.*)', '\1\2') AND
             nearestApproachDistance(a.trip, ST_Transform(s.stop_loc::geometry, 3059)) < 10)

SELECT actual_trip_id, schedule_trip_id, stop_id, stop_name, stop_sequence,
  stop_loc, schedule_time, getTimestamp(stopInstant) AS actual_time,
  ST_Transform(getValue(stopInstant), 4326) AS trip_Geom
FROM Tstops;


CREATE TABLE TripSegments AS
  SELECT actual_trip_id, schedule_trip_id, stop_id AS end_stop_id,
  schedule_time AS end_time_schedule, actual_time AS end_time_actual,
-- Use LAG to get the previous stop's information
  LAG(stop_id) OVER (PARTITION BY actual_trip_id ORDER BY stop_sequence)
   AS start_stop_id,
  LAG(schedule_time) OVER (PARTITION BY actual_trip_id ORDER BY stop_sequence)
  AS start_time_schedule,
  LAG(actual_time) OVER (PARTITION BY actual_trip_id ORDER BY stop_sequence)
  AS start_time_actual
FROM TripStops;



11.5.2 Speed Analysis Over Network Segments

SELECT AVG(s.distance_m / EXTRACT(
   EPOCH FROM (ts.end_time_actual - ts.start_time_actual)) * 3.6) AS speedKmH,
   s.geometry, ts.start_stop_id, ts.end_stop_id
FROM TripSegments ts, Segments s
WHERE ts.start_stop_id = s.start_stop_id AND ts.end_stop_id = s.end_stop_id AND
   ts.start_time_actual IS NOT NULL AND
   EXTRACT(EPOCH FROM (ts.end_time_actual - ts.start_time_actual)) > 0
GROUP BY s.geometry, ts.start_stop_id, ts.end_stop_id;


11.5.3 Delay Analysis in Public Transport


CREATE TABLE TripDelay AS
 SELECT actual_trip_id, schedule_trip_id,
  tgeompointSeq(array_agg(tgeompoint(stop_loc, schedule_time) ORDER BY schedule_time)) AS  
   schedule_trip,
  tgeompointSeq(array_agg(tgeompoint(trip_Geom, actual_time) ORDER BY actual_time)) AS actual_trip
FROM TripStops
WHERE actual_trip_id LIKE 'TRAM1-%-ab-%'
GROUP BY actual_trip_id, schedule_trip_id;

