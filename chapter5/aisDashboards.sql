=================== DASHBOARD 1 ================

 -- Average Speed Histogram Panel

WITH SOGValues AS (
SELECT MMSI, AVG(SOG) * 1.852::double precision AS AvgSOG
FROM AISInput
WHERE SOG IS NOT NULL AND SOG > 0
GROUP BY MMSI )
SELECT AvgSOG
FROM SOGValues
WHERE AvgSOG > 5
--------------------------------

-- Trip Lengths Panel

SELECT MMSI, length(Trip)
FROM Ships;


----------------------------------

-- Destination Ports Panel

WITH Destinations AS (
SELECT MMSI, Destination, COUNT(*)
FROM AISInput
WHERE Destination IS NOT NULL
GROUP BY MMSSI, Destination )
SELECT Destination, COUNT(*)
FROM Destinations
GROUP BY Destination
ORDER BY COUNT(*) DESC

--------------------------------------

--- Route Usage Frequency Heatmap Panel

SELECT MMSI, SOG, Latitude, Longitude,
FROM AisInput 

NOTE: If response time is high, please use TABLESAMPLE SYSTEM(10)
=================== DASHBOARD 2 ================

-- SHIPS WITHIN 300m Panel

WITH TimeInterval(Period) AS (
SELECT tstzspan '[2024-03-01 01:00:00, 2024-03-01 23:30:00)' ),
174 5 Querying Mobility Databases
TimeShips(MMSI, Trip) AS (
SELECT MMSI, atTime(s.Trip, t.Period)
FROM Ships s, TimeInterval t
WHERE atTime(s.Trip, Period) IS NOT NULL ),
TimeClosestShips As (
SELECT s1.MMSI AS Boat1, s2.MMSI AS Boat2,
startValue(atMin(s1.Trip <-> s2.Trip)) AS ClosestDistance,
startTimestamp(atMin(s1.Trip <-> s2.Trip)) AS TimeClosDist,
s1.Trip AS Trip1, s2.Trip AS Trip2
FROM TimeShips s1, TimeShips s2
WHERE s1.MMSI < s2.MMSI AND eDwithin(s1.Trip, s2.Trip, 300) IS NOT NULL AND
startValue(atMin(s1.Trip <-> s2.Trip)) < 300)
SELECT t.Boat1, t.Boat2, t.ClosestDistance, t.TimeClosDist,
ST_X(ST_Transform(valueAtTimestamp(Trip1, TimeClosDist), 4326)) AS Long1,
ST_Y(ST_Transform(valueAtTimestamp(Trip1, TimeClosDist), 4326)) AS Lat1,
ST_X(ST_Transform(valueAtTimestamp(Trip2, TimeClosDist), 4326)) AS Long2,
ST_Y(ST_Transform(valueAtTimestamp(Trip2, TimeClosDist), 4326)) AS Lat2
FROM TimeClosestShips t;

-------------------------------------------------------------

--SHIPS WITHIN 300m Heatmap

Same query as above, with a different graphic.

-------------------------------------------------------------

-- NUMBER OF BOATS MOVING THOUGH A GIVEN AREA PANEL


WITH Ports(PortName, PortEnv, Long, Lat) AS (
SELECT * FROM (VALUES
('Rodby', ST_MakeEnvelope(651135, 6058230, 651422, 6058548, 25832),
11.06, 54.53),
('Puttgarden', ST_MakeEnvelope(644339, 6042108, 644896, 6042487, 25832),
11.36, 54.64)) AS p )
SELECT p.PortName, SUM(numSequences(atGeometry(s.Trip, p.PortEnv))) AS
tripsIntersectsPort, p.Long, p.Lat
FROM Ports p, Ships s
WHERE ST_Intersects(trajectory(s.Trip), p.PortEnv) IS NOT NULL
GROUP BY p.PortName, p.Long, p.Lat;
-------------------------------------------------------------

-- Two Boats Close to Each Other Panel
'[2024-03-01 01:00:00, 2024-03-01 23:30:00)' ),
WITH vessels AS
(
SELECT 
atTime(S1.Trip, '[2024-03-01 01:00:00, 2024-03-01 02:30:00)'::tstzspan ) AS itinerary1,
atTime(S2.Trip, '[2024-03-01 01:00:00, 2024-03-01 02:30:00)'::tstzspan ) AS itinerary2
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
