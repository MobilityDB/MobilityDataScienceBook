=================== DASHBOARD 1 ================

 -- Noise Heatmap
WITH Sydney(geomsydney)
   AS (SELECT ST_makeEnvelope(151.3,-33.75,150.93,-34.1,4326)),
   
flight_traj_time_slice (icao24, callsign, time_slice_trip, time_slice_geoaltitude, time_slice_vertrate)  
AS
 (
       SELECT icao24, callsign, atTime(trip, '[2020-06-01 01:00:00, 2020-06-01 
        23:00:00)'::tstzspan),
        atTime(geoaltitude, '[2020-06-01 01:00:00, 2020-06-01 
        23:00:00)'::tstzspan), 
        atTime(vertrate,'[2020-06-01 01:00:00, 2020-06-01 
        23:00:00)'::tstzspan)
  FROM  flight_traj  , Sydney S 
  WHERE atTime(trip, '[2020-06-01 01:00:00, 2020-06-01 
        23:00:00)'::tstzspan) IS NOT NULL AND
        atGeometry((trip), S.geomsydney) is NOT NULL),
flight_traj_time_slice_ascent(icao24, callsign, ascending_trip, ascending_geoaltitude, ascending_vertrate) 
AS
 (SELECT icao24, callsign, atTime(time_slice_trip, sequenceN( 
       atValues(time_slice_vertrate, '[1,20]'::floatspan),  
       1)::tstzspan),
atTime(time_slice_geoaltitude, sequenceN(atValues( 
      time_slice_vertrate,'[1,20]'::floatspan),1)::tstzspan),
atTime(time_slice_vertrate, sequenceN(atValues  
      (time_slice_vertrate, '[1,20]'::floatspan) 
       ,1)::tstzspan)
FROM flight_traj_time_slice
WHERE 
atTime(time_slice_trip, sequenceN(
       atValues(time_slice_vertrate, '[1,20]'::floatspan),  
       1)::tstzspan) IS NOT NULL),
 
final_output AS
    (SELECT icao24, callsign,
getValue(unnest(instants(ascending_geoaltitude))) AS  
geoaltitude,
getValue(unnest(instants(ascending_vertrate))) AS 
vertrate,
ST_X(getValue(unnest(instants(ascending_trip)))) AS lon,
ST_Y(getValue(unnest(instants(ascending_trip)))) AS lat
FROM flight_traj_time_slice_ascent
)

SELECT *
FROM final_output
WHERE vertrate IS NOT NULL AND geoaltitude IS NOT NULL
AND (lat IS NOT NULL AND lon IS NOT NULL);



--------------------------------

--Altitude of Flights Leaving Sydney

flight_traj_time_slice (icao24, callsign, time_slice_trip, time_slice_geoaltitude, time_slice_vertrate)  
AS
 (
       SELECT icao24, callsign, atTime(trip, '[2020-06-01 01:00:00, 2020-06-01 
        23:00:00)'::tstzspan),
        atTime(geoaltitude, '[2020-06-01 01:00:00, 2020-06-01 
        23:00:00)'::tstzspan), 
        atTime(vertrate,'[2020-06-01 01:00:00, 2020-06-01 
        23:00:00)'::tstzspan)
  FROM  flight_traj  , Sydney S 
  WHERE atTime(trip, '[2020-06-01 01:00:00, 2020-06-01 
        23:00:00)'::tstzspan) IS NOT NULL AND
        atGeometry((trip), S.geomsydney) is NOT NULL),
flight_traj_time_slice_ascent(icao24, callsign, ascending_trip, ascending_geoaltitude, ascending_vertrate) 
AS
 (SELECT icao24, callsign, atTime(time_slice_trip, sequenceN( 
       atValues(time_slice_vertrate, '[1,20]'::floatspan),  
       1)::tstzspan),
atTime(time_slice_geoaltitude, sequenceN(atValues( 
      time_slice_vertrate,'[1,20]'::floatspan),1)::tstzspan),
atTime(time_slice_vertrate, sequenceN(atValues  
      (time_slice_vertrate, '[1,20]'::floatspan) 
       ,1)::tstzspan)
FROM flight_traj_time_slice
WHERE 
atTime(time_slice_trip, sequenceN(
       atValues(time_slice_vertrate, '[1,20]'::floatspan),  
       1)::tstzspan) IS NOT NULL),
 
final_output AS
    (SELECT icao24, callsign,
getValue(unnest(instants(ascending_geoaltitude))) AS  
geoaltitude,
getValue(unnest(instants(ascending_vertrate))) AS 
vertrate,
ST_X(getValue(unnest(instants(ascending_trip)))) AS lon,
ST_Y(getValue(unnest(instants(ascending_trip)))) AS lat
FROM flight_traj_time_slice_ascent
)

SELECT *
FROM final_output
WHERE vertrate IS NOT NULL AND geoaltitude IS NOT NULL
AND (lat IS NOT NULL AND lon IS NOT NULL);

----------------------------------
 

=================== DASHBOARD 2 ================

-- Distance Between Two Planes Panel

 
WITH mindist AS(
 SELECT transform(s1.trip, 3112) <-> transform(s2.trip,3112)  
        AS distance
 FROM flight_traj s1, flight_traj s2
 WHERE s1.icao24 > s2.icao24 AND 
      atMin(transform(s1.trip,3112) <->  
      transform(s2.trip,3112)) IS NOT  NULL   AND 
      s1.icao24='06a1bc' AND s2.icao24='06a1a5')
SELECT startTimestamp(unnest(instants(distance))) AS time, 
       getValue(unnest(instants(distance)))/1000 as distance
FROM mindist
-------------------------------------------------------------

-- Position of Close Flights Panel

SELECT et_ts, CASE
  WHEN icao24 ='06a1bc' THEN 1  ELSE 2 END, lat, lon
FROM flights TABLESAMPLE SYSTEM (25)
WHERE icao24 IN ('06a1bc','06a1a5') 
ORDER BY icao24
 

-------------------------------------------------------------

-- Geoaltituve vs Time for aircraft '07c4f1f'

SELECT et_ts AS "time", geoaltitude
FROM flights
WHERE icao24 IN ('07c4f1f') 
-------------------------------------------------------------


SELECT et_ts AS "time", geoaltitude
FROM flights
WHERE icao24 IN ('7c3349') 
-- Geoaltitude vs Time '7c3349'


======================== DASHBOARD 3 =======================

-- GPS Location Over Time Panel

WITH
Canada(geomcanada) AS 
   (SELECT ST_Transform(ST_makeEnvelope(-172.54, 23.81, 
						-47.74 ,86.46,4326),4326) ),
CanadaFlights AS (SELECT et_ts, icao24, geom
 FROM flights TABLESAMPLE SYSTEM (5), Canada C
 WHERE 
       et_ts between '2020-06-01 2:30:00' and '2020-06-01    
       4:30:00' AND ST_intersects(C.geomcanada,geom))
SELECT et_ts, icao24, lat, lon,to_number(icao24,'999999') as nro 
-- TABLESAMPLE SYSTEM (n) returns only n% of the data from the table.
FROM flights  
WHERE icao24 IN (SELECT icao24 from CanadaFlights limit 7)  

------------------------

-- Cruising Flights Panel

WITH
flight_traj_time_slice (icao24, callsign, time_slice_trip, time_slice_geoaltitude, time_slice_vertrate) AS
(SELECT icao24, callsign,
atTime(trip, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan),
atTime(geoaltitude, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan),
atTime(vertrate,'[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan)
FROM flight_traj TABLESAMPLE SYSTEM (20)
WHERE atTime(trip, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan) is not null
),
 
flight_traj_time_slice_ascent(icao24, callsign, ascending_trip, ascending_geoaltitude, ascending_vertrate) AS
(SELECT icao24, callsign,
atTime(time_slice_trip, sequenceN( atValues(time_slice_geoaltitude, '[3000,8000)'::floatspan), 1)::tstzspan),
atTime(time_slice_geoaltitude, sequenceN(atValues( time_slice_geoaltitude, '[3000,8000)'::floatspan) ,1)::tstzspan),
atTime(time_slice_vertrate, sequenceN(atValues (time_slice_geoaltitude, '[3000,8000)'::floatspan) ,1)::tstzspan)
FROM flight_traj_time_slice
WHERE 
atTime(time_slice_trip, sequenceN(
atValues(time_slice_geoaltitude, '[3000,8000)'::floatspan), 1)::tstzspan) is not null ),
final_output AS
(SELECT icao24, callsign,
getValue(unnest(instants(ascending_geoaltitude))) AS geoaltitude,
getValue(unnest(instants(ascending_vertrate))) AS vertrate,
ST_X(getValue(unnest(instants(ascending_trip)))) AS lon,
ST_Y(getValue(unnest(instants(ascending_trip)))) AS lat
FROM flight_traj_time_slice_ascent)
SELECT *
FROM final_output
WHERE vertrate IS NOT NULL
AND geoaltitude IS NOT NULL;

--------------------------

-- FLIGHTS TAKING-OFF PANEL

WITH
flight_traj_time_slice (icao24, callsign, time_slice_trip, time_slice_geoaltitude, time_slice_vertrate) AS
(SELECT icao24, callsign,
atTime(trip, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan),
atTime(geoaltitude, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan),
atTime(vertrate,'[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan)
FROM flight_traj TABLESAMPLE SYSTEM (20)
WHERE atTime(trip, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan) is not null
),
 
flight_traj_time_slice_ascent(icao24, callsign, ascending_trip, ascending_geoaltitude, ascending_vertrate) AS
(SELECT icao24, callsign,
atTime(time_slice_trip, sequenceN( atValues(time_slice_vertrate, '[1,20]'::floatspan), 1)::tstzspan),
atTime(time_slice_geoaltitude, sequenceN(atValues( time_slice_vertrate, '[1,20]'::floatspan) ,1)::tstzspan),
atTime(time_slice_vertrate, sequenceN(atValues (time_slice_vertrate, '[1,20]'::floatspan) ,1)::tstzspan)
FROM flight_traj_time_slice
WHERE 
atTime(time_slice_trip, sequenceN(
atValues(time_slice_vertrate, '[1,20]'::floatspan), 1)::tstzspan) is not null ),
final_output AS
(SELECT icao24, callsign,
getValue(unnest(instants(ascending_geoaltitude))) AS geoaltitude,
getValue(unnest(instants(ascending_vertrate))) AS vertrate,
ST_X(getValue(unnest(instants(ascending_trip)))) AS lon,
ST_Y(getValue(unnest(instants(ascending_trip)))) AS lat
FROM flight_traj_time_slice_ascent)
SELECT *
FROM final_output
WHERE vertrate IS NOT NULL
AND geoaltitude IS NOT NULL;

--------------------------

-- LANDING FLIGHTS PANEL


WITH
flight_traj_time_slice (icao24, callsign, time_slice_trip, time_slice_geoaltitude, time_slice_vertrate) AS
(SELECT icao24, callsign,
atTime(trip, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan),
atTime(geoaltitude, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan),
atTime(vertrate,'[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan)
FROM flight_traj TABLESAMPLE SYSTEM (20)
WHERE atTime(trip, '[2020-06-01 03:00:00, 2020-06-01 04:00:00)'::tstzspan) is not null
),
 
flight_traj_time_slice_descent(icao24, callsign, landing_trip, landing_geoaltitude, landing_vertrate) AS
(SELECT icao24, callsign,
atTime(time_slice_trip, sequenceN( atValues(time_slice_vertrate, '[-20,0)'::floatspan), 1)::tstzspan),
atTime(time_slice_geoaltitude, sequenceN(atValues( time_slice_vertrate, '[-20,0)'::floatspan) ,1)::tstzspan),
atTime(time_slice_vertrate, sequenceN(atValues (time_slice_vertrate, '[-20,0)'::floatspan) ,1)::tstzspan)
FROM flight_traj_time_slice
WHERE 
atTime(time_slice_trip, sequenceN(
atValues(time_slice_vertrate, '[-20,0)'::floatspan), 1)::tstzspan) is not null ),
final_output AS
(SELECT icao24, callsign,
getValue(unnest(instants(landing_geoaltitude))) AS geoaltitude,
getValue(unnest(instants(landing_vertrate))) AS vertrate,
ST_X(getValue(unnest(instants(landing_trip)))) AS lon,
ST_Y(getValue(unnest(instants(landing_trip)))) AS lat
FROM flight_traj_time_slice_descent)
SELECT *
FROM final_output
WHERE vertrate IS NOT NULL
AND geoaltitude IS NOT NULL;

 
