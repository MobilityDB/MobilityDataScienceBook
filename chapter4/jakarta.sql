SELECT  trj_id, speed, trajectory(stops(trip, 0.001, '1 hour'))
FROM trips
WHERE stops(trip, 0.001, '1 hour') is not null

-- longest trips length
DROP TABLE IF EXISTS longTripsLen;
CREATE TABLE longTripsLen AS
SELECT *
FROM trips
WHERE DrivingMode = 'car'
ORDER BY length(trip) DESC
LIMIT 100;

-- longest trips length
DROP TABLE IF EXISTS longTripsDur;
CREATE TABLE longTripsDur AS
SELECT *
FROM trips
WHERE DrivingMode = 'car'
ORDER BY duration(trip) DESC
LIMIT 100;


-- slow trips 
DROP TABLE IF EXISTS slowTrips;
CREATE TABLE slowTrips AS
WITH averages(avglng, avgdur) AS (
  SELECT AVG(length(trip)), AVG(duration(trip))
  FROM trips )
SELECT tripId, length(trip) AS lng, avglng, duration(trip) AS dur, avgdur,
  extract(dow FROM starttimestamp(trip)) AS day, traj
FROM trips, averages  
WHERE length(trip) < 0.8 * avglng AND duration(trip) > 1.2 * avgdur AND
  DrivingMode = 'car'
ORDER BY length(trip) DESC
LIMIT 100;

SELECT day, COUNT(*)
FROM slowTrips
GROUP BY day
ORDER BY COUNT(*) DESC

--fast trips
DROP TABLE IF EXISTS fastTrips;
CREATE TABLE fastTrips AS
WITH averages(avglng, avgdur) AS (
  SELECT AVG(length(trip)), AVG(duration(trip))
  FROM trips )
SELECT tripId, length(trip) AS lng, avglng, duration(trip) AS dur, avgdur,
  extract(dow FROM starttimestamp(trip)) AS day, traj
FROM trips, averages  
WHERE length(trip) > 0.8 * avglng AND duration(trip) < 0.8 * avgdur AND
  DrivingMode = 'car'
ORDER BY length(trip) DESC
LIMIT 100;

SELECT day, COUNT(*)
FROM fastTrips
GROUP BY day
ORDER BY COUNT(*) DESC


-----
--Slow trips on weekdays for cars

WITH averages AS (SELECT avg(length(trip)) as avglng,avg(duration(trip)) as avgdur
				 FROM trips) 
SELECT trj_id, length(trip),avglng, duration(trip), avgdur, extract(dow from  starttimestamp(trip)) as day ,twavg(speed), traj
from trips, averages  
WHERE length(trip) < 0.8* avglng AND duration(trip) > 1.2*avgdur
	AND (extract(dow from  starttimestamp(trip))> 0 OR extract(dow from  starttimestamp(trip)) < 6)
    AND driving_mode = 'car'
order by length(trip) desc
limit 200 
---------

--Fast trips on weekdays for cars

WITH averages AS (SELECT avg(length(trip)) as avglng,avg(duration(trip)) as avgdur
				 FROM trips) 
				 
SELECT trj_id, length(trip),avglng, duration(trip), avgdur,    extract(dow from  starttimestamp(trip)) as day,  twavg(speed),traj
from trips, averages  
WHERE (length(trip) > 0.8 * avglng AND  length(trip) < 1.2 * avglng) 
AND duration(trip) < 0.7 * avgdur
			  AND (extract(dow from  starttimestamp(trip))> 0 OR extract(dow from  starttimestamp(trip)) < 6)
              AND driving_mode = 'car'
order by length desc
limit 200
-------------
--Slow trips on weekends for cars

WITH averages AS (SELECT avg(length(trip)) as avglng,avg(duration(trip)) as avgdur
				 FROM trips) 
 
SELECT trj_id, length(trip),avglng, duration(trip), avgdur, extract(dow from  starttimestamp(trip)) as day ,traj
from trips, averages  
WHERE length(trip) < 0.8* avglng AND duration(trip) > 1.2*avgdur
	AND (extract(dow from  starttimestamp(trip))= 0 OR extract(dow from  starttimestamp(trip)) = 6)
    AND driving_mode = 'car'
order by length(trip) desc
limit 200 
---------

Fast trips on weekends

WITH averages AS (SELECT avg(length(trip)) as avglng,avg(duration(trip)) as avgdur
				 FROM trips) 
				 
SELECT trj_id, length(trip),avglng, duration(trip), avgdur,    extract(dow from  starttimestamp(trip)) as day, traj
from trips, averages  
WHERE (length(trip) > 0.8 * avglng AND  length(trip) < 1.2 * avglng) 
AND duration(trip) < 0.7 * avgdur
			  AND (extract(dow from  starttimestamp(trip))= 0 OR extract(dow from  starttimestamp(trip)) = 6)
order by length desc
limit 200