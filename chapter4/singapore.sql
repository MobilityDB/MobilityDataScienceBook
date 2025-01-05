 SELECT srid(trip) FROM trips limit 10
 
SELECT row_number() over() AS cid, trajectory 
FROM(
WITH Cities(Sydney, Auckland, Melbourne) AS 
(SELECT ST_Transform(ST_MakeEnvelope(103.9,1.29, 104.02, 1.29,4326),3414), ST_Transform(ST_MakeEnvelope (174.0, -37.67,176.17, -36.12, 4326),4326), ST_Transform(ST_MakeEnvelope (143.947935, -38.869629, 146.320982, -37.089844, 4326),4326)), 

WITH Airport(geomairp) AS (	
SELECT  ST_Transform(ST_MakeEnvelope(43448.255460, 32100.164699, 50057.878379, 38324.866882,3414),4326)),
	
Downtown(geomdown) AS (SELECT  ST_Transform(ST_MakeEnvelope(25766.253602, 26853.438763,30962.684184, 30800.601927,3414),4326))

WITH Places(airport, downtown) AS (
SELECT ST_MakeEnvelope(43448.255460, 32100.164699, 50057.878379, 38324.866882,3414),
  ST_MakeEnvelope(25766.253602, 26853.438763,30962.684184, 30800.601927,3414) )	
SELECT tripId, trajectory
FROM Trips t, Places p
WHERE ST_Intersects(t.trajectory, p.airport) AND ST_Intersects(t.trajectory, p.downtown);

	
WITH Places(airport, downtown) AS (
SELECT ST_MakeEnvelope(43448.255460, 32100.164699, 50057.878379, 38324.866882,3414),
  ST_MakeEnvelope(25766.253602, 26853.438763, 30962.684184, 30800.601927, 3414) )
SELECT tripId, astext(trip), trajectory, endValue(trip) 
FROM Trips t, Places p
WHERE ST_Intersects(t.trajectory, p.airport) AND 
  ST_Intersects(t.trajectory, p.downtown) AND
  ST_geometryType(endValue(trip)) = 'ST_Point';
	
	
WITH Places(airport, downtown) AS (
SELECT ST_MakeEnvelope(43448.255460, 32100.164699, 50057.878379, 38324.866882,3414),
  ST_MakeEnvelope(25766.253602, 26853.438763,30962.684184, 30800.601927, 3414) )
SELECT tripId, trajectory, endValue(trip), startValue(trip) 
FROM Trips t, Places p
WHERE ST_Intersects(startValue(trip), p.downtown) AND 
  ST_Intersects(t.trajectory, p.airport)	AND
  ST_geometryType(endValue(trip)) = 'ST_Point';
	
	
WITH Places(airport, downtown) AS (
SELECT ST_MakeEnvelope(43448.255460, 32100.164699, 50057.878379, 38324.866882,3414),
  ST_MakeEnvelope(25766.253602, 26853.438763,30962.684184, 30800.601927, 3414) )
SELECT tripId, trajectory, endValue(trip), startValue(trip) 
FROM Trips t, Places p
WHERE ST_Intersects(startValue(trip), p.airport) AND 
  ST_Intersects(endValue(trip), p.downtown) AND
  ST_geometryType(endValue(trip)) = 'ST_Point';

WITH Places(airport, downtown) AS (
  SELECT 
    ST_MakeEnvelope(43448.255460, 32100.164699, 50057.878379, 38324.866882, 3414),
    ST_MakeEnvelope(25766.253602, 26853.438763,30962.684184, 30800.601927, 3414) ),
Speeds(twAvgSpeed) AS (
  SELECT tripId, twAvg(speed(trip)) 
  FROM trips ),
Summaries(minSpeed, maxSpeed, avgSpeed) AS (
  SELECT MIN(twAvgSpeed), MAX(twAvgSpeed), AVG(twAvgSpeed)
  FROM Speeds ),
Veloc AS (
  SELECT t.*, A.*
  FROM Speeds t, Summaries A )
SELECT t.tripId, t.trajectory, endValue(t.trip), startValue(t.trip), s.twAvgSpeed
FROM Trips t, Places p, Speeds s
WHERE s.tripId = t.tripId AND ST_Intersects(startValue(trip), p.airport) AND
  ST_Intersects(endValue(trip), p.downtown) AND 
  ST_GeometryType(endValue(trip)) = 'ST_Point';
  
  
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS Places;
CREATE TABLE Places(Airport, HarborFront) AS
SELECT 
  ST_MakeEnvelope(43448.255460, 32100.164699, 50057.878379, 38324.866882, 3414),
  ST_MakeEnvelope(25766.253602, 26853.438763, 30962.684184, 30800.601927, 3414);

DROP TABLE IF EXISTS trips_harbor_airport;
CREATE TABLE trips_harbor_airport AS
SELECT TripId, Trajectory, startValue(Trip), endValue(Trip)
FROM Trips t, Places p
WHERE ST_Intersects(startValue(Trip), p.HarborFront) AND
  ST_Intersects(endValue(Trip), p.Airport);

DROP TABLE IF EXISTS trips_downtown_airport;
CREATE TABLE trips_downtown_airport AS
SELECT TripId, Trajectory, startValue(Trip), endValue(Trip)
FROM Trips t, Places p
WHERE NOT ST_Intersects(t.Trajectory, p.HarborFront) AND
  ST_Intersects(endValue(Trip), p.Airport);

DROP TABLE IF EXISTS trips_airport_speed;
CREATE TABLE trips_airport_speed AS
SELECT t.tripId, t.trajectory, endValue(t.trip), startValue(t.trip), 
  twAvg(speed(trip)) AS twAvgSpeed
FROM Trips t, Places p
WHERE ST_Intersects(endValue(trip), p.airport) AND 
  ST_GeometryType(endValue(trip)) = 'ST_Point';
  
-------------------------------------------------------------------------------

