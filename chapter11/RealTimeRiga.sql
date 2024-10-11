{\rtf1\ansi\ansicpg1252\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;\f1\fswiss\fcharset0 Helvetica-Oblique;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue255;\red192\green0\blue64;}
{\*\expandedcolortbl;;\csgenericrgb\c0\c0\c100000;\csgenericrgb\c75294\c0\c25098;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs36 \cf0 11.5.1 Setting the scenario: Building Trajectories and Segments
\fs24 \
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0
\cf0 \
\
\pard\pardeftab720\partightenfactor0
\cf2 CREATE \cf0 EXTENSION MobilityDB \cf2 Cascade\
ALTER TABLE \cf0 VehiclePosition \cf2 ADD COLUMN \cf0 Geom geometry (point,4326);\
\cf2 \
UPDATE \cf0 VehiclePosition \cf2 SET \cf0 Geom = ST_Makepoint(longitude, latitude);\
\cf2 \
\
DROP TABLE \cf0 IF \cf2 EXISTS \cf0 ActualTrips;\
\cf2 \
CREATE TABLE \cf0 ActualTrips (Trip_id text, Trip tgeompoint);\
\cf2 \
INSERT INTO \cf0 ActualTrips(Trip_id, Trip)\
WITH Temp \cf2 AS \cf0 (\
\pard\pardeftab720\partightenfactor0
\cf3 -- We use DISTINCT since we observed duplicate tuples\
\pard\pardeftab720\partightenfactor0
\cf2  SELECT DISTINCT \cf0 Trip_id, ST_Transform(Geom, 3059) \cf2 AS \cf0 geom,\
  to_timestamp(\cf2 timestamp\cf0 ) \cf2 AS \cf0 t\
\cf2   FROM \cf0 VehiclePosition )\
\
\cf2 SELECT \cf0 Trip_id, tgeompointSeq(array_agg(tgeompoint(Geom, t) \cf2 ORDER BY \cf0 t)\
FILTER (\cf2 WHERE \cf0 Geom IS \cf2 NOT NULL\cf0 )) \cf2 AS \cf0 trip\
\cf2 FROM \cf0 Temp\
\cf2 GROUP BY \cf0 Trip_id;\
\pard\pardeftab720\partightenfactor0
\cf3 -- 871 rows affected in 616 ms\
\
\
\pard\pardeftab720\partightenfactor0
\cf2 ALTER TABLE \cf0 ActualTrips \cf2 ADD COLUMN \cf0 traj geometry;\
\cf2 UPDATE \cf0 ActualTrips \cf2 set \cf0 traj = trajectory(trip);\
\
\
\cf2 CREATE TABLE \cf0 TripStops \cf2 AS\
\pard\pardeftab720\partightenfactor0
\cf0   WITH Tstops \cf2 AS \cf0 (\
\pard\pardeftab720\partightenfactor0
\cf2     SELECT \cf0 a.trip_id \cf2 AS \cf0 actual_trip_id, ad.trip_id \cf2 AS \cf0 schedule_trip_id, s.stop_id,\
       s.stop_name, ad.stop_sequence, s.stop_loc::geometry, t_arrival \cf2 AS \cf0 schedule_time,\
       nearestApproachInstant(a.trip, ST_Transform(s.stop_loc::geometry, 3059))\
\cf2        AS \cf0 stopInstant\
\cf2     FROM \cf0 ActualTrips a, arrivals_departures ad, Stops s\
\cf2     WHERE \cf0 ad.\cf2 date\cf0 = '2024-09-24'::\cf2 timestamp AND \cf0 ad.stop_id= s.stop_id \cf2 AND\
\pard\pardeftab720\partightenfactor0
\cf0             regexp_replace(a.trip_id, '([^-]+-[^-]+-[^-]+)-[0-9]+(-.*)', '\\1\\2') =\
            regexp_replace(ad.trip_id, '([^-]+-[^-]+-[^-]+)-[0-9]+(-.*)', '\\1\\2') \cf2 AND\
\cf0              nearestApproachDistance(a.trip, ST_Transform(s.stop_loc::geometry, 3059)) < 10)\
\
\pard\pardeftab720\partightenfactor0
\cf2 SELECT \cf0 actual_trip_id, schedule_trip_id, stop_id, stop_name, stop_sequence,\
  stop_loc, schedule_time, getTimestamp(stopInstant) \cf2 AS \cf0 actual_time,\
  ST_Transform(getValue(stopInstant), 4326) \cf2 AS \cf0 trip_Geom\
\cf2 FROM \cf0 Tstops;\
\pard\pardeftab720\partightenfactor0

\f1\i\fs16 \cf0 \
\
\pard\pardeftab720\partightenfactor0

\f0\i0\fs24 \cf2 CREATE TABLE \cf0 TripSegments \cf2 AS\
  SELECT \cf0 actual_trip_id, schedule_trip_id, stop_id \cf2 AS \cf0 end_stop_id,\
  schedule_time \cf2 AS \cf0 end_time_schedule, actual_time \cf2 AS \cf0 end_time_actual,\
\pard\pardeftab720\partightenfactor0
\cf3 -- Use LAG to get the previous stop's information\
\pard\pardeftab720\partightenfactor0
\cf0   LAG(stop_id) OVER (PARTITION \cf2 BY \cf0 actual_trip_id \cf2 ORDER BY \cf0 stop_sequence)\
\pard\pardeftab720\partightenfactor0
\cf2    AS \cf0 start_stop_id,\
  LAG(schedule_time) OVER (PARTITION \cf2 BY \cf0 actual_trip_id \cf2 ORDER BY \cf0 stop_sequence)\
\cf2   AS \cf0 start_time_schedule,\
  LAG(actual_time) OVER (PARTITION \cf2 BY \cf0 actual_trip_id \cf2 ORDER BY \cf0 stop_sequence)\
\cf2   AS \cf0 start_time_actual\
\cf2 FROM \cf0 TripStops;\
\
\
\
\pard\pardeftab720\partightenfactor0

\fs36 \cf0 11.5.2 Speed Analysis Over Network Segments
\fs24 \
\
\pard\pardeftab720\partightenfactor0
\cf2 SELECT AVG\cf0 (s.distance_m / \cf2 EXTRACT\cf0 (\
   EPOCH \cf2 FROM \cf0 (ts.end_time_actual - ts.start_time_actual)) * 3.6) \cf2 AS \cf0 speedKmH,\
   s.geometry, ts.start_stop_id, ts.end_stop_id\
\cf2 FROM \cf0 TripSegments ts, Segments s\
\cf2 WHERE \cf0 ts.start_stop_id = s.start_stop_id \cf2 AND \cf0 ts.end_stop_id = s.end_stop_id \cf2 AND\
\pard\pardeftab720\partightenfactor0
\cf0    ts.start_time_actual IS \cf2 NOT NULL AND\
   EXTRACT\cf0 (EPOCH \cf2 FROM \cf0 (ts.end_time_actual - ts.start_time_actual)) > 0\
\pard\pardeftab720\partightenfactor0
\cf2 GROUP BY \cf0 s.geometry, ts.start_stop_id, ts.end_stop_id;\
\
\
\pard\pardeftab720\partightenfactor0

\fs36 \cf0 11.5.3 Delay Analysis in Public Transport
\fs24 \
\
\pard\pardeftab720\partightenfactor0

\f1\i\fs16 \cf2 \

\f0\i0\fs24 CREATE TABLE \cf0 TripDelay \cf2 AS\
 SELECT \cf0 actual_trip_id, schedule_trip_id,\
  tgeompointSeq(array_agg(tgeompoint(stop_loc, schedule_time) \cf2 ORDER BY \cf0 schedule_time)) \cf2 AS  \
   \cf0 schedule_trip,\
  tgeompointSeq(array_agg(tgeompoint(trip_Geom, actual_time) \cf2 ORDER BY \cf0 actual_time)) \cf2 AS \cf0 actual_trip\
\cf2 FROM \cf0 TripStops\
\cf2 WHERE \cf0 actual_trip_id \cf2 LIKE \cf0 'TRAM1-%-ab-%'\
\cf2 GROUP BY \cf0 actual_trip_id, schedule_trip_id;\
}