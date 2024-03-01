=================== DASHBOARD 1 ================

 -- Average Speed Histogram Panel

CREATE OR REPLACE VIEW public.sogvalues
 AS
 SELECT aisinputfiltered3.mmsi,
    avg(aisinputfiltered3.sog) * 1.852::double precision AS avgsog
   FROM aisinputfiltered3
  WHERE aisinputfiltered3.sog IS NOT NULL AND aisinputfiltered3.sog > 0::double precision
  GROUP BY aisinputfiltered3.mmsi
  ORDER BY (avg(aisinputfiltered3.sog)) DESC;

ALTER TABLE public.sogvalues
    OWNER TO postgres;

SELECT AVGSOG 
FROM SOGVALUES 
WHERE AVGSOG > 5

--------------------------------

-- Trip Lengths Panel

SELECT Length/1000 as Length FROM 
lengthspeeds   
WHERE Length/1000> 5  ORDER BY Length

----------------------------------

-- Destination Ports Panel

WITH destinations AS(
SELECT  mmsi, destination, count(*)
FROM  AISInput  
WHERE destination IS NOT NULL
GROUP BY mmsi, destination
ORDER BY destination) 

SELECT destination, count(*)
FROM destinations
GROUP BY destination
ORDER BY  count(*) DESC

--------------------------------------

--- Route Usage Frequency Heatmap Panel

SELECT latitude, longitude, sog , mmsi
FROM aisinputfiltered3 TABLESAMPLE SYSTEM (10)


=================== DASHBOARD 2 ================

-- SHIPS WITHIN 300m Panel

WITH
TimeShips AS (
SELECT MMSI,
atTime(S.Trip, '[2018-04-01 01:00:00, 2018-04-01 02:30:00)'::tstzspan ) AS trip
FROM Ships S
WHERE atTime(S.Trip, '[2018-04-01 01:00:00, 2018-04-01 02:30:00)'::tstzspan) is not null
and length(atTime(S.Trip, '[2018-04-01 01:00:00, 2018-04-01 02:30:00)'::tstzspan )) < 5000 limit 200),

TimeClosestShips As (
SELECT
S1.MMSI AS boat1, S2.MMSI AS boat2,
startValue(atMin(S1.trip <-> S2.trip)) as closest_distance,
startTimestamp( atMin(S1.trip <-> S2.trip)) as time_at_closest_dist,
S1.trip as b1trip, S2.trip as b2trip
FROM
TimeShips S1, TimeShips S2
WHERE
S1.MMSI > S2.MMSI AND edwithin(S1.Trip, S2.Trip, 300) IS NOT NULL
AND 
startValue(atMin(S1.trip <-> S2.trip)) < 300
)
SELECT t.boat1, t.boat2, t.closest_distance, t.time_at_closest_dist,
ST_X(ST_Transform(valueAtTimestamp(b1trip, time_at_closest_dist), 4326) ) as  b1_lng,
ST_Y(ST_Transform(valueAtTimestamp(b1trip, time_at_closest_dist), 4326) ) as  b1_lat,
ST_X(ST_Transform( valueAtTimestamp(b2trip, time_at_closest_dist), 4326) ) as  b2_lng,
ST_Y(ST_Transform( valueAtTimestamp(b2trip, time_at_closest_dist), 4326) ) as  b2_lat
FROM TimeClosestShips t;

-------------------------------------------------------------

--SHIPS WITHIN 300m Heatmap


WITH
TimeShips AS (
SELECT MMSI,
atTime(S.Trip, '[2018-04-01 01:00:00, 2018-04-01 02:30:00)'::tstzspan ) AS trip
FROM Ships S
WHERE atTime(S.Trip, '[2018-04-01 01:00:00, 2018-04-01 02:30:00)'::tstzspan) is not null
and length(atTime(S.Trip, '[2018-04-01 01:00:00, 2018-04-01 02:30:00)'::tstzspan )) < 5000 limit 200),

TimeClosestShips As (
SELECT
S1.MMSI AS boat1, S2.MMSI AS boat2,
startValue(atMin(S1.trip <-> S2.trip)) as closest_distance,
startTimestamp( atMin(S1.trip <-> S2.trip)) as time_at_closest_dist,
S1.trip as b1trip, S2.trip as b2trip
FROM
TimeShips S1, TimeShips S2
WHERE
S1.MMSI > S2.MMSI AND edwithin(S1.Trip, S2.Trip, 300) IS NOT NULL AND 
startValue(atMin(S1.trip <-> S2.trip)) < 300
)
SELECT t.boat1, t.boat2, t.closest_distance, t.time_at_closest_dist,
ST_X(ST_Transform(valueAtTimestamp(b1trip, time_at_closest_dist), 4326) ) as  b1_lng,
ST_Y(ST_Transform(valueAtTimestamp(b1trip, time_at_closest_dist), 4326) ) as  b1_lat,
ST_X(ST_Transform( valueAtTimestamp(b2trip, time_at_closest_dist), 4326) ) as  b2_lng,
ST_Y(ST_Transform( valueAtTimestamp(b2trip, time_at_closest_dist), 4326) ) as  b2_lat
FROM TimeClosestShips t;

-------------------------------------------------------------

-- NUMBER OF BOATS MOVING THOUGH A GIVEN AREA PANEL


WITH ports(port_name, port_geom, lat, lng)
AS (SELECT p.port_name, p.port_geom, lat, lng
FROM
-- ST_MakeEnvelope creates geometry against which to check intersection
(VALUES ('Rodby', ST_MakeEnvelope(651135, 6058230, 651422, 6058548, 25832)::geometry, 54.53, 11.06),
('Puttgarden', ST_MakeEnvelope(644339, 6042108, 644896, 6042487, 25832)::geometry, 54.64, 11.36)) AS p(port_name, port_geom, lat, lng))
-- p.lat and p.lng will be used to place the port location on the visualization
SELECT P.port_name, sum(numSequences(atGeometry(S.Trip, P.port_geom))) AS trips_intersect_with_port, p.lat, p.lng
FROM ports AS P,
Ships AS S
WHERE eintersects(S.Trip, P.port_geom) IS NOT NULL
GROUP BY P.port_name, P.lat, P.lng

-------------------------------------------------------------

-- Two Boats Close to Each Other Panel

WITH vessels AS
(
SELECT 
atTime(S1.Trip, '[2018-04-01 01:00:00, 2018-04-01 02:30:00)'::tstzspan ) AS itinerary1,
atTime(S2.Trip, '[2018-04-01 01:00:00, 2018-04-01 02:30:00)'::tstzspan ) AS itinerary2
FROM Ships S1, Ships S2
WHERE 
S1.MMSI=477121200 AND S2.MMSI=219017733
)
SELECT 
ST_X(ST_transform((getValue(unnest(instants(itinerary1)))),4326)) as boat1x,
ST_Y(ST_transform((getValue(unnest(instants(itinerary1)))),4326)) as boat1y,
ST_X(ST_transform((getValue(unnest(instants(itinerary2)))),4326)) as boat2x,
ST_Y(ST_transform((getValue(unnest(instants(itinerary2)))),4326)) as boat2y
FROM vessels