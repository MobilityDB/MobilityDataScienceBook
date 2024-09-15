------------------------------------------------------------------------------
-- PostgreSQL plans
------------------------------------------------------------------------------

---------------------------------------
-- Range queries
---------------------------------------

-- Q1
-- List the vehicles that have passed at a region from Regions.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT DISTINCT r.RegionId, t.VehicleId
FROM Trips t, Regions r
WHERE ST_Intersects(trajectory(t.Trip), r.Geom)
ORDER BY r.RegionId, t.VehicleId;
/*
 Unique (actual rows=6128 loops=1)
   ->  Gather Merge (actual rows=11200 loops=1)
         Workers Planned: 2
         Workers Launched: 2
         ->  Unique (actual rows=3733 loops=3)
               ->  Sort (actual rows=18247 loops=3)
                     Sort Key: r.regionid, t.vehicleid
                     Sort Method: quicksort  Memory: 1647kB
                     Worker 0:  Sort Method: quicksort  Memory: 1556kB
                     Worker 1:  Sort Method: quicksort  Memory: 1668kB
                     ->  Nested Loop (actual rows=18247 loops=3)
                           ->  Parallel Seq Scan on trips t (actual rows=6303 loops=3)
                           ->  Index Scan using regions_geom_gist_idx on regions r (actual rows=3 loops=18910)
                                 Index Cond: (geom && trajectory(t.trip))
                                 Filter: st_intersects(trajectory(t.trip), geom)
                                 Rows Removed by Filter: 12
 Planning Time: 1.239 ms
 Execution Time: 105850.639 ms
(18 rows)

Time: 105852.822 ms (01:45.853)
*/

-- Q2
-- List the vehicles that have passed at a period from Periods.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT DISTINCT p.PeriodId, t.VehicleId
FROM Trips t, Periods p
WHERE t.Trip && p.Period
ORDER BY p.PeriodId, t.VehicleId;
/*
 Sort (actual rows=50273 loops=1)
   Sort Key: p.periodid, t.vehicleid
   Sort Method: quicksort  Memory: 3722kB
   ->  HashAggregate (actual rows=50273 loops=1)
         Group Key: p.periodid, t.vehicleid
         Batches: 1  Memory Usage: 5649kB
         ->  Nested Loop (actual rows=131837 loops=1)
               ->  Seq Scan on periods p (actual rows=100 loops=1)
               ->  Bitmap Heap Scan on trips t (actual rows=1318 loops=100)
                     Recheck Cond: (trip && p.period)
                     Heap Blocks: exact=76911
                     ->  Bitmap Index Scan on trips_trip_gist_idx (actual rows=1318 loops=100)
                           Index Cond: (trip && p.period) */

-- Q3
-- List the vehicles that were within a region from Regions1 during a period from Periods1.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT r.RegionId, p.PeriodId, t.VehicleId
FROM Trips t, Regions1 r, Periods1 p
WHERE t.Trip && stbox(r.Geom, p.Period) AND 
  eIntersects(atTime(t.Trip, p.Period), r.Geom)
ORDER BY r.RegionId, p.PeriodId, t.VehicleId;
/*
 Sort (actual rows=3165 loops=1)
   Sort Key: regions.regionid, p.periodid, t.vehicleid
   Sort Method: quicksort  Memory: 245kB
   ->  Nested Loop (actual rows=3165 loops=1)
         ->  Nested Loop (actual rows=100 loops=1)
               ->  Limit (actual rows=10 loops=1)
                     ->  Seq Scan on regions (actual rows=10 loops=1)
               ->  Materialize (actual rows=10 loops=10)
                     ->  Subquery Scan on p (actual rows=10 loops=1)
                           ->  Limit (actual rows=10 loops=1)
                                 ->  Seq Scan on periods (actual rows=10 loops=1)
         ->  Bitmap Heap Scan on trips t (actual rows=32 loops=100)
               Recheck Cond: (trip && stbox(regions.geom, p.period))
               Filter: eintersects(attime(trip, p.period), regions.geom)
               Rows Removed by Filter: 129
               Heap Blocks: exact=11509
               ->  Bitmap Index Scan on trips_trip_gist_idx (actual rows=161 loops=100)
                     Index Cond: (trip && stbox(regions.geom, p.period)) */

-- Q4
-- List the pairs of vehicles that were both located within a region from Regions during a period from Periods.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT DISTINCT t1.VehicleId AS VehicleId1, t2.VehicleId AS VehicleId2, r.RegionId, p.PeriodId
FROM Trips t1, Trips1 t2, Regions1 r, Periods1 p
WHERE t1.VehicleId < t2.VehicleId AND t1.Trip && stbox(r.Geom, p.Period) AND
  t2.Trip && stbox(r.Geom, p.Period) AND 
  eIntersects(atTime(t1.Trip, p.Period), r.Geom) AND
  eIntersects(atTime(t2.Trip, p.Period), r.Geom)
ORDER BY t1.VehicleId, t2.VehicleId, r.RegionId, p.PeriodId;
/*
 Unique (actual rows=111 loops=1)
   ->  Sort (actual rows=269 loops=1)
         Sort Key: t1.vehicleid, t2.vehicleid, regions.regionid, p.periodid
         Sort Method: quicksort  Memory: 39kB
         ->  Nested Loop (actual rows=269 loops=1)
               Join Filter: (t1.vehicleid < t2.vehicleid)
               Rows Removed by Join Filter: 1054
               ->  Nested Loop (actual rows=19 loops=1)
                     Join Filter: ((t2.trip && stbox(regions.geom, p.period)) AND eintersects(attime(t2.trip, p.period), regions.geom))
                     Rows Removed by Join Filter: 9981
                     ->  Nested Loop (actual rows=100 loops=1)
                           ->  Limit (actual rows=10 loops=1)
                                 ->  Seq Scan on regions (actual rows=10 loops=1)
                           ->  Materialize (actual rows=10 loops=10)
                                 ->  Subquery Scan on p (actual rows=10 loops=1)
                                       ->  Limit (actual rows=10 loops=1)
                                             ->  Seq Scan on periods (actual rows=10 loops=1)
                     ->  Materialize (actual rows=100 loops=100)
                           ->  Subquery Scan on t2 (actual rows=100 loops=1)
                                 ->  Limit (actual rows=100 loops=1)
                                       ->  Seq Scan on trips (actual rows=100 loops=1)
               ->  Bitmap Heap Scan on trips t1 (actual rows=70 loops=19)
                     Recheck Cond: (trip && stbox(regions.geom, p.period))
                     Filter: eintersects(attime(trip, p.period), regions.geom)
                     Rows Removed by Filter: 296
                     Heap Blocks: exact=4207
                     ->  Bitmap Index Scan on trips_trip_gist_idx (actual rows=366 loops=19)
                           Index Cond: (trip && stbox(regions.geom, p.period))
 Planning Time: 0.510 ms
 Execution Time: 11793.186 ms
(30 rows)

Time: 11794.553 ms (00:11.795)
*/

-- Q5
-- List the first time at which a vehicle visited a point in Points.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t.VehicleId, p.PointId, MIN(startTimestamp(atValues(t.Trip,p.Geom))) AS Instant
FROM Trips t, Points p
WHERE ST_Contains(trajectory(t.Trip), p.Geom)
GROUP BY t.VehicleId, p.PointId;
/*
 Finalize GroupAggregate (actual rows=413 loops=1)
   Group Key: t.vehicleid, p.pointid
   ->  Gather Merge (actual rows=625 loops=1)
         Workers Planned: 2
         Workers Launched: 2
         ->  Partial GroupAggregate (actual rows=208 loops=3)
               Group Key: t.vehicleid, p.pointid
               ->  Sort (actual rows=648 loops=3)
                     Sort Key: t.vehicleid, p.pointid
                     Sort Method: quicksort  Memory: 82kB
                     Worker 0:  Sort Method: quicksort  Memory: 78kB
                     Worker 1:  Sort Method: quicksort  Memory: 81kB
                     ->  Nested Loop (actual rows=648 loops=3)
                           ->  Parallel Seq Scan on trips t (actual rows=6303 loops=3)
                           ->  Index Scan using points_geom_gist_idx on points p (actual rows=0 loops=18910)
                                 Index Cond: (geom @ trajectory(t.trip))
                                 Filter: st_contains(trajectory(t.trip), geom)
                                 Rows Removed by Filter: 13
 Planning Time: 0.810 ms
 Execution Time: 111088.816 ms
(20 rows)

Time: 111090.666 ms (01:51.091)
*/

---------------------------------------
-- Temporal Aggregate Queries
---------------------------------------

-- Q6 
-- Compute how many vehicles were active at each period in Periods.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT p.PeriodID, COUNT(*), tCount(atTime(t.Trip, p.Period))
FROM Trips t, Periods p
WHERE t.Trip && p.Period
GROUP BY p.PeriodID
ORDER BY p.PeriodID;
/*
 GroupAggregate (actual rows=96 loops=1)
   Group Key: p.periodid
   ->  Nested Loop (actual rows=131837 loops=1)
         ->  Index Scan using periods_pkey on periods p (actual rows=100 loops=1)
         ->  Bitmap Heap Scan on trips t (actual rows=1318 loops=100)
               Recheck Cond: (trip && p.period)
               Heap Blocks: exact=76911
               ->  Bitmap Index Scan on trips_trip_gist_idx (actual rows=1318 loops=100)
                     Index Cond: (trip && p.period)
 Planning Time: 0.754 ms
 Execution Time: 75776.500 ms
(11 rows)

Time: 75778.240 ms (01:15.778)
*/

-- Q7
-- Count the number of trips that were active during each hour in May 29, 2007.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
WITH TimeSplit(Period) AS (
  SELECT span(H, H + interval '1 hour')
  FROM generate_series(timestamptz '2007-05-29 00:00:00', 
    timestamptz '2007-05-29 23:00:00', interval '1 hour') AS H )
SELECT Period, COUNT(*)
FROM TimeSplit s, Trips t
WHERE s.Period && t.Trip AND atTime(Trip, Period) IS NOT NULL
GROUP BY s.Period
ORDER BY s.Period;
/*
 Sort (actual rows=0 loops=1)
   Sort Key: (span(h.h, (h.h + '01:00:00'::interval), true, false))
   Sort Method: quicksort  Memory: 25kB
   ->  HashAggregate (actual rows=0 loops=1)
         Group Key: span(h.h, (h.h + '01:00:00'::interval), true, false)
         Batches: 1  Memory Usage: 40kB
         ->  Nested Loop (actual rows=0 loops=1)
               ->  Function Scan on generate_series h (actual rows=24 loops=1)
               ->  Index Scan using trips_trip_gist_idx on trips t (actual rows=0 loops=24)
                     Index Cond: (trip && span(h.h, (h.h + '01:00:00'::interval), true, false))
                     Filter: (attime(trip, span(h.h, (h.h + '01:00:00'::interval), true, false)) IS NOT NULL)
 Planning Time: 0.401 ms
 Execution Time: 0.635 ms
(13 rows)

Time: 2.167 ms
*/

---------------------------------------
-- Distance Queries
---------------------------------------

-- Q8
-- List the overall traveled distances of the vehicles during the periods from Periods.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t.VehicleId, p.PeriodId, p.Period,
  SUM(length(atTime(t.Trip, p.Period))) AS Distance
FROM Trips t, Periods p
WHERE t.Trip && p.Period
GROUP BY t.VehicleId, p.PeriodId, p.Period
ORDER BY t.VehicleId, p.PeriodId;
/*
 GroupAggregate (actual rows=50273 loops=1)
   Group Key: t.vehicleid, p.periodid
   ->  Sort (actual rows=131837 loops=1)
         Sort Key: t.vehicleid, p.periodid
         Sort Method: external merge  Disk: 8352kB
         ->  Nested Loop (actual rows=131837 loops=1)
               ->  Seq Scan on periods p (actual rows=100 loops=1)
               ->  Bitmap Heap Scan on trips t (actual rows=1318 loops=100)
                     Recheck Cond: (trip && p.period)
                     Heap Blocks: exact=76911
                     ->  Bitmap Index Scan on trips_trip_gist_idx (actual rows=1318 loops=100)
                           Index Cond: (trip && p.period)
 Planning Time: 0.644 ms
 Execution Time: 53078.525 ms
(14 rows)

Time: 53080.030 ms (00:53.080) */

-- Q9
-- List the minimum distance ever between each vehicle and each point from Points.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t.VehicleId, p.PointId, MIN(trajectory(t.Trip) <-> p.Geom) AS MinDistance
FROM Trips t, Points p
GROUP BY t.VehicleId, p.PointId
ORDER BY t.VehicleId, p.PointId;
/*
 Finalize GroupAggregate (actual rows=63200 loops=1)
   Group Key: t.vehicleid, p.pointid
   ->  Gather Merge (actual rows=177300 loops=1)
         Workers Planned: 2
         Workers Launched: 2
         ->  Sort (actual rows=59100 loops=3)
               Sort Key: t.vehicleid, p.pointid
               Sort Method: external merge  Disk: 1504kB
               Worker 0:  Sort Method: external merge  Disk: 1536kB
               Worker 1:  Sort Method: external merge  Disk: 1488kB
               ->  Partial HashAggregate (actual rows=59100 loops=3)
                     Group Key: t.vehicleid, p.pointid
                     Batches: 1  Memory Usage: 7697kB
                     Worker 0:  Batches: 1  Memory Usage: 7953kB
                     Worker 1:  Batches: 1  Memory Usage: 7697kB
                     ->  Nested Loop (actual rows=630333 loops=3)
                           ->  Parallel Seq Scan on trips t (actual rows=6303 loops=3)
                           ->  Seq Scan on points p (actual rows=100 loops=18910)
 Planning Time: 0.300 ms
 Execution Time: 324301.885 ms
(20 rows)

Time: 324303.032 ms (05:24.303) */

-- Q10
-- List the minimum temporal distance between each pair of vehicles.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t1.VehicleId AS Car1Id, t2.VehicleId AS Car2Id, minValue(t1.Trip <-> t2.Trip) AS MinDistance
FROM Trips t1, Trips1 t2
WHERE t1.VehicleId < t2.VehicleId AND timeSpan(t1.Trip) && timeSpan(t2.Trip)
GROUP BY t1.VehicleId, t2.VehicleId, t1.Trip, t2.Trip
ORDER BY t1.VehicleId, t2.VehicleId;
/*
 Group (actual rows=2567 loops=1)
   Group Key: t1.vehicleid, trips.vehicleid, t1.trip, trips.trip
   ->  Sort (actual rows=2567 loops=1)
         Sort Key: t1.vehicleid, trips.vehicleid, t1.trip, trips.trip
         Sort Method: quicksort  Memory: 298kB
         ->  Nested Loop (actual rows=2567 loops=1)
               ->  Limit (actual rows=100 loops=1)
                     ->  Seq Scan on trips (actual rows=100 loops=1)
               ->  Index Scan using trips_vehicleid_idx on trips t1 (actual rows=26 loops=100)
                     Index Cond: (vehicleid < trips.vehicleid)
                     Filter: (timespan(trip) && timespan(trips.trip))
                     Rows Removed by Filter: 1602
 Planning Time: 0.851 ms
 Execution Time: 11514.960 ms
(14 rows)

Time: 11517.109 ms (00:11.517)
*/

-- Q11
-- List the nearest approach time, distance, and shortest line between each pair of trips.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t1.VehicleId AS Car1Id, t1.TripId AS Trip1Id, t2.VehicleId AS Car2Id, 
  t2.TripId AS Trip2Id, timeSpan(nearestApproachInstant(t1.Trip, t2.Trip)) AS Time,
  nearestApproachDistance(t1.Trip, t2.Trip) AS Distance, 
  shortestLine(t1.Trip, t2.Trip) AS Line
FROM Trips t1, Trips1 t2
WHERE t1.VehicleId < t2.VehicleId AND timeSpan(t1.Trip) && timeSpan(t2.Trip)
ORDER BY t1.VehicleId, t1.TripId, t2.VehicleId, t2.TripId;
/*
 Sort (actual rows=2567 loops=1)
   Sort Key: t1.vehicleid, t1.tripid, trips.vehicleid, trips.tripid
   Sort Method: quicksort  Memory: 417kB
   ->  Nested Loop (actual rows=2567 loops=1)
         ->  Limit (actual rows=100 loops=1)
               ->  Seq Scan on trips (actual rows=100 loops=1)
         ->  Index Scan using trips_vehicleid_idx on trips t1 (actual rows=26 loops=100)
               Index Cond: (vehicleid < trips.vehicleid)
               Filter: (timespan(trip) && timespan(trips.trip))
               Rows Removed by Filter: 1602
 Planning Time: 0.450 ms
 Execution Time: 23169.294 ms
(12 rows)

Time: 23170.481 ms (00:23.170)
*/

-- Q12
-- List when and where a pairs of vehicles have been at 10 m or less from each other.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t1.VehicleId AS VehicleId1, t2.VehicleId AS VehicleId2, atTime(t1.Trip,
  getTime(tdwithin(t1.Trip, t2.Trip, 10.0, TRUE))) AS Position
FROM Trips t1, Trips1 t2
WHERE t1.VehicleId < t2.VehicleId AND t1.Trip && expandSpace(t2.Trip, 10) AND
  tdwithin(t1.Trip, t2.Trip, 10.0, TRUE) IS NOT NULL
ORDER BY t1.VehicleId, t2.VehicleId, Position;
/*
 Sort (actual rows=9 loops=1)
   Sort Key: t1.vehicleid, trips.vehicleid, (attime(t1.trip, gettime(tdwithin(t1.trip, trips.trip, '10'::double precision, true)
)))
   Sort Method: quicksort  Memory: 54kB
   ->  Nested Loop (actual rows=9 loops=1)
         ->  Limit (actual rows=100 loops=1)
               ->  Seq Scan on trips (actual rows=100 loops=1)
         ->  Bitmap Heap Scan on trips t1 (actual rows=0 loops=100)
               Recheck Cond: ((trip && expandspace(trips.trip, '10'::double precision)) AND (vehicleid < trips.vehicleid))
               Filter: (tdwithin(trip, trips.trip, '10'::double precision, true) IS NOT NULL)
               Rows Removed by Filter: 15
               Heap Blocks: exact=1519
               ->  BitmapAnd (actual rows=0 loops=100)
                     ->  Bitmap Index Scan on trips_trip_gist_idx (actual rows=147 loops=100)
                           Index Cond: (trip && expandspace(trips.trip, '10'::double precision))
                     ->  Bitmap Index Scan on trips_vehicleid_idx (actual rows=1628 loops=100)
                           Index Cond: (vehicleid < trips.vehicleid)
 Planning Time: 0.359 ms
 Execution Time: 6516.606 ms
(18 rows)

Time: 6518.117 ms (00:06.518) */

---------------------------------------
-- Nearest-Neighbor Queries
---------------------------------------

-- Q13
-- For each trip from Trips, list the three points from Points that have been closest to that vehicle.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t.VehicleId, P1.PointId, P1.Distance
FROM Trips t CROSS JOIN LATERAL (
  SELECT p.PointId, t.Trip |=| p.Geom AS Distance
  FROM Points1 p
  ORDER BY Distance LIMIT 3 ) AS P1
ORDER BY t.TripId, t.VehicleId, P1.Distance;
/*
 Incremental Sort (actual rows=56730 loops=1)
   Sort Key: t.tripid, t.vehicleid, ((t.trip |=| p.geom))
   Presorted Key: t.tripid
   Full-sort Groups: 1720  Sort Method: quicksort  Average Memory: 26kB  Peak Memory: 26kB
   ->  Nested Loop (actual rows=56730 loops=1)
         ->  Index Scan using trips_pkey on trips t (actual rows=18910 loops=1)
         ->  Limit (actual rows=3 loops=18910)
               ->  Sort (actual rows=3 loops=18910)
                     Sort Key: ((t.trip |=| p.geom))
                     Sort Method: top-N heapsort  Memory: 25kB
                     ->  Subquery Scan on p (actual rows=10 loops=18910)
                           ->  Limit (actual rows=10 loops=18910)
                                 ->  Seq Scan on points (actual rows=10 loops=18910)
 Planning Time: 0.336 ms
 Execution Time: 91040.445 ms
(15 rows)

Time: 91041.542 ms (01:31.042) */


-- Q14
-- For each trip from Trips, list the three vehicles that are closest to that vehicle

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t1.VehicleId AS VehicleId1, C2.VehicleId AS VehicleId2, C2.Distance
FROM Trips t1 CROSS JOIN LATERAL (
  SELECT t2.VehicleId, t1.Trip |=| t2.Trip AS Distance
  FROM Trips1 t2
  WHERE t1.VehicleId < t2.VehicleId AND timeSpan(t1.Trip) && timeSpan(t2.Trip)
  ORDER BY Distance LIMIT 3 ) AS C2
ORDER BY t1.VehicleId, C2.VehicleId;
/*
 Incremental Sort (actual rows=2557 loops=1)
   Sort Key: t1.vehicleid, t2.vehicleid
   Presorted Key: t1.vehicleid
   Full-sort Groups: 75  Sort Method: quicksort  Average Memory: 27kB  Peak Memory: 27kB
   ->  Nested Loop (actual rows=2557 loops=1)
         ->  Index Scan using trips_vehicleid_idx on trips t1 (actual rows=18910 loops=1)
         ->  Limit (actual rows=0 loops=18910)
               ->  Sort (actual rows=0 loops=18910)
                     Sort Key: ((t1.trip |=| t2.trip))
                     Sort Method: quicksort  Memory: 25kB
                     ->  Subquery Scan on t2 (actual rows=0 loops=18910)
                           Filter: ((t1.vehicleid < t2.vehicleid) AND (timespan(t1.trip) && timespan(t2.trip)))
                           Rows Removed by Filter: 100
                           ->  Limit (actual rows=100 loops=18910)
                                 ->  Seq Scan on trips (actual rows=100 loops=18910)
 Planning Time: 0.377 ms
 Execution Time: 10572.828 ms
(17 rows)

Time: 10573.980 ms (00:10.574) */

-- Q15
-- For each trip from Trips, list the points from Points that have that vehicle among their three nearest neighbors.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
WITH PointTrips AS (
  SELECT p.PointId, t2.VehicleId, t2.TripId, t2.Distance
  FROM Points1 p CROSS JOIN LATERAL (
    SELECT t1.VehicleId, t1.TripId, p.Geom |=| t1.Trip AS Distance
    FROM Trips t1
    ORDER BY Distance LIMIT 3 ) AS t2 )
SELECT t.VehicleId, t.TripId, p.PointId, PT.Distance
FROM Trips t CROSS JOIN Points p JOIN PointTrips PT
  ON t.VehicleId = PT.VehicleId AND t.TripId = PT.TripId AND p.PointId = PT.PointId
ORDER BY t.VehicleId, t.TripId, p.PointId;
/*
 Sort (actual rows=30 loops=1)
   Sort Key: t.vehicleid, t.tripid, p.pointid
   Sort Method: quicksort  Memory: 26kB
   ->  Nested Loop (actual rows=30 loops=1)
         ->  Nested Loop (actual rows=30 loops=1)
               ->  Hash Join (actual rows=10 loops=1)
                     Hash Cond: (p.pointid = p_1.pointid)
                     ->  Seq Scan on points p (actual rows=100 loops=1)
                     ->  Hash (actual rows=10 loops=1)
                           Buckets: 1024  Batches: 1  Memory Usage: 9kB
                           ->  Subquery Scan on p_1 (actual rows=10 loops=1)
                                 ->  Limit (actual rows=10 loops=1)
                                       ->  Seq Scan on points (actual rows=10 loops=1)
               ->  Limit (actual rows=3 loops=10)
                     ->  Sort (actual rows=3 loops=10)
                           Sort Key: ((p_1.geom |=| t1.trip))
                           Sort Method: top-N heapsort  Memory: 25kB
                           ->  Seq Scan on trips t1 (actual rows=18910 loops=10)
         ->  Memoize (actual rows=1 loops=30)
               Cache Key: t1.vehicleid, t1.tripid
               Cache Mode: logical
               Hits: 0  Misses: 30  Evictions: 0  Overflows: 0  Memory Usage: 4kB
               ->  Index Scan using trips_pkey on trips t (actual rows=1 loops=30)
                     Index Cond: (tripid = t1.tripid)
                     Filter: (vehicleid = t1.vehicleid)
 Planning Time: 1.579 ms
 Execution Time: 90804.815 ms
(27 rows)

Time: 90807.383 ms (01:30.807) */

-- Q16
-- For each trip from Trips, list the vehicles having the vehicle of the trip among the three nearest neighbors.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
WITH TripDistances AS (
  SELECT t1.VehicleId AS VehicleId1, t1.TripId AS TripId1, T3.VehicleId AS VehicleId2, 
    T3.TripId AS TripId2, T3.Distance
  FROM Trips t1 CROSS JOIN LATERAL (
    SELECT t2.VehicleId, t2.TripId, minValue(t1.Trip <-> t2.Trip) AS Distance
    FROM Trips1 t2
    WHERE t1.VehicleId < t2.VehicleId AND timeSpan(t1.Trip) && timeSpan(t2.Trip)
    ORDER BY Distance LIMIT 3 ) AS T3 )
SELECT t1.VehicleId, t1.TripId, t2.VehicleId, t2.TripId, TD.Distance
FROM Trips t1 JOIN Trips1 t2 ON t1.VehicleId < t2.VehicleId
  JOIN TripDistances TD ON t1.VehicleId = TD.VehicleId1 AND t1.TripId = TD.TripId1 AND
  t2.VehicleId = TD.VehicleId2 AND t2.TripId = TD.TripId2
ORDER BY t1.VehicleId, t1.TripId, t2.VehicleId, t2.TripId;
/*
 Sort (actual rows=2557 loops=1)
   Sort Key: t1.vehicleid, t1.tripid, trips.vehicleid, trips.tripid
   Sort Method: quicksort  Memory: 256kB
   ->  Nested Loop (actual rows=2557 loops=1)
         Join Filter: (t1.vehicleid < trips.vehicleid)
         ->  Hash Join (actual rows=18910 loops=1)
               Hash Cond: ((t1.vehicleid = t1_1.vehicleid) AND (t1.tripid = t1_1.tripid))
               ->  Seq Scan on trips t1 (actual rows=18910 loops=1)
               ->  Hash (actual rows=18910 loops=1)
                     Buckets: 32768  Batches: 1  Memory Usage: 1452kB
                     ->  Seq Scan on trips t1_1 (actual rows=18910 loops=1)
         ->  Hash Join (actual rows=0 loops=18910)
               Hash Cond: ((trips.vehicleid = t3.vehicleid) AND (trips.tripid = t3.tripid))
               ->  Limit (actual rows=100 loops=2119)
                     ->  Seq Scan on trips (actual rows=100 loops=2119)
               ->  Hash (actual rows=0 loops=18910)
                     Buckets: 1024  Batches: 1  Memory Usage: 9kB
                     ->  Subquery Scan on t3 (actual rows=0 loops=18910)
                           ->  Limit (actual rows=0 loops=18910)
                                 ->  Sort (actual rows=0 loops=18910)
                                       Sort Key: (minvalue((t1_1.trip <-> t2.trip)))
                                       Sort Method: quicksort  Memory: 25kB
                                       ->  Subquery Scan on t2 (actual rows=0 loops=18910)
                                             Filter: ((t1_1.vehicleid < t2.vehicleid) AND (timespan(t1_1.trip) && timespan(t2.tr
ip)))
                                             Rows Removed by Filter: 100
                                             ->  Limit (actual rows=100 loops=18910)
                                                   ->  Seq Scan on trips trips_1 (actual rows=100 loops=18910)
 Planning Time: 1.315 ms
 Execution Time: 10738.288 ms
(29 rows)

Time: 10740.546 ms (00:10.741)
*/

-- Q17
-- For each group of ten disjoint vehicles, list the point(s) from Points, having the minimum aggregated distance from the given group of ten vehicles during the given period.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
WITH Groups AS (
  SELECT ((ROW_NUMBER() OVER (ORDER BY V.VehicleId))-1)/10 + 1 AS GroupId, V.VehicleId
  FROM Vehicles V
  LIMIT 100 ),
SumDistances AS (
  SELECT G.GroupId, p.PointId,
    SUM(ST_Distance(trajectory(t.Trip), p.Geom)) AS SumDist
  FROM Groups G, Points p, Trips t
  WHERE t.VehicleId = G.VehicleId
  GROUP BY G.GroupId, p.PointId )
SELECT S1.GroupId, S1.PointId, S1.SumDist
FROM SumDistances S1
WHERE S1.SumDist <= ALL (
  SELECT SumDist
  FROM SumDistances S2
  WHERE S1.GroupId = S2.GroupId )
ORDER BY S1.GroupId, S1.PointId;
/*
 Sort (actual rows=10 loops=1)
   Sort Key: s1.groupid, s1.pointid
   Sort Method: quicksort  Memory: 25kB
   CTE sumdistances
     ->  HashAggregate (actual rows=1000 loops=1)
           Group Key: ((((row_number() OVER (?) - 1) / 10) + 1)), p.pointid
           Batches: 1  Memory Usage: 529kB
           ->  Nested Loop (actual rows=301300 loops=1)
                 ->  Nested Loop (actual rows=3013 loops=1)
                       ->  Limit (actual rows=100 loops=1)
                             ->  WindowAgg (actual rows=100 loops=1)
                                   ->  Index Only Scan using vehicles_pkey on vehicles v (actual rows=100 loops=1)
                                         Heap Fetches: 100
                       ->  Index Scan using trips_vehicleid_idx on trips t (actual rows=30 loops=100)
                             Index Cond: (vehicleid = v.vehicleid)
                 ->  Materialize (actual rows=100 loops=3013)
                       ->  Seq Scan on points p (actual rows=100 loops=1)
   ->  CTE Scan on sumdistances s1 (actual rows=10 loops=1)
         Filter: (SubPlan 2)
         Rows Removed by Filter: 990
         SubPlan 2
           ->  CTE Scan on sumdistances s2 (actual rows=5 loops=1000)
                 Filter: (s1.groupid = groupid)
                 Rows Removed by Filter: 41
 Planning Time: 0.919 ms
 Execution Time: 140878.933 ms
(26 rows)

Time: 140881.911 ms (02:20.882)
*/

------------------------------------------------------------------------------
-- Citus plans
------------------------------------------------------------------------------

---------------------------------------
-- Range queries
---------------------------------------

-- Q1
-- List the vehicles that have passed at a region from Regions.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT DISTINCT r.RegionId, t.VehicleId
FROM Trips t, Regions r
WHERE ST_Intersects(trajectory(t.Trip), r.Geom)
ORDER BY r.RegionId, t.VehicleId;
/*
 Sort (actual rows=6128 loops=1)
   Sort Key: remote_scan.regionid, remote_scan.vehicleid
   Sort Method: quicksort  Memory: 480kB
   ->  HashAggregate (actual rows=6128 loops=1)
         Group Key: remote_scan.regionid, remote_scan.vehicleid
         Batches: 1  Memory Usage: 737kB
         ->  Custom Scan (Citus Adaptive) (actual rows=6128 loops=1)
               Task Count: 32
               Tuple data received from nodes: 48 kB
               Tasks Shown: One of 32
               ->  Task
                     Tuple data received from node: 2064 bytes
                     Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
                     ->  Unique (actual rows=258 loops=1)
                           ->  Incremental Sort (actual rows=2150 loops=1)
                                 Sort Key: r.regionid, t.vehicleid
                                 Presorted Key: r.regionid
                                 Full-sort Groups: 33  Sort Method: quicksort  Average Memory: 28kB  Peak Memory: 28kB
                                 Pre-sorted Groups: 31  Sort Method: quicksort  Average Memory: 25kB  Peak Memory: 25kB
                                 ->  Nested Loop (actual rows=2150 loops=1)
                                       Join Filter: st_intersects(trajectory(t.trip), r.geom)
                                       Rows Removed by Join Filter: 77850
                                       ->  Index Scan using regions_pkey_102081 on regions_102081 r (actual rows=100 loops=1)
                                       ->  Materialize (actual rows=800 loops=100)
                                             ->  Seq Scan on trips_102011 t (actual rows=800 loops=1)
                         Planning Time: 1.966 ms
                         Execution Time: 85992.909 ms
 Planning Time: 6.701 ms
 Execution Time: 143043.302 ms
(29 rows)

Time: 143055.225 ms (02:23.055)
*/

-- Q2
-- List the vehicles that have passed at a period from Periods.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT DISTINCT p.PeriodId, t.VehicleId
FROM Trips t, Periods p
WHERE t.Trip && p.Period
ORDER BY p.PeriodId, t.VehicleId;
/*
 Sort (actual rows=50273 loops=1)
   Sort Key: remote_scan.periodid, remote_scan.vehicleid
   Sort Method: quicksort  Memory: 3722kB
   ->  HashAggregate (actual rows=50273 loops=1)
         Group Key: remote_scan.periodid, remote_scan.vehicleid
         Batches: 1  Memory Usage: 4129kB
         ->  Custom Scan (Citus Adaptive) (actual rows=50273 loops=1)
               Task Count: 32
               Tuple data received from nodes: 393 kB
               Tasks Shown: One of 32
               ->  Task
                     Tuple data received from node: 16 kB
                     Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
                     ->  HashAggregate (actual rows=2086 loops=1)
                           Group Key: p.periodid, t.vehicleid
                           Batches: 1  Memory Usage: 241kB
                           ->  Nested Loop (actual rows=5578 loops=1)
                                 Join Filter: (t.trip && p.period)
                                 Rows Removed by Join Filter: 74422
                                 ->  Seq Scan on trips_102011 t (actual rows=800 loops=1)
                                 ->  Materialize (actual rows=100 loops=800)
                                       ->  Seq Scan on periods_102077 p (actual rows=100 loops=1)
                         Planning Time: 9.146 ms
                         Execution Time: 16164.676 ms
 Planning Time: 8.585 ms
 Execution Time: 30385.270 ms
(26 rows)

Time: 30396.815 ms (00:30.397)
*/

-- Q3
-- List the vehicles that were within a region from Regions1 during a period from Periods1.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT r.RegionId, p.PeriodId, t.VehicleId
FROM Trips t, Regions1 r, Periods1 p
WHERE t.Trip && stbox(r.Geom, p.Period) AND 
  eIntersects(atTime(t.Trip, p.Period), r.Geom)
ORDER BY r.RegionId, p.PeriodId, t.VehicleId;
/*
 Sort (actual rows=50273 loops=1)
   Sort Key: remote_scan.periodid, remote_scan.vehicleid
   Sort Method: quicksort  Memory: 3722kB
   ->  HashAggregate (actual rows=50273 loops=1)
         Group Key: remote_scan.periodid, remote_scan.vehicleid
         Batches: 1  Memory Usage: 4129kB
         ->  Custom Scan (Citus Adaptive) (actual rows=50273 loops=1)
               Task Count: 32
               Tuple data received from nodes: 393 kB
               Tasks Shown: One of 32
               ->  Task
                     Tuple data received from node: 16 kB
                     Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
                     ->  HashAggregate (actual rows=2086 loops=1)
                           Group Key: p.periodid, t.vehicleid
                           Batches: 1  Memory Usage: 241kB
                           ->  Nested Loop (actual rows=5578 loops=1)
                                 Join Filter: (t.trip && p.period)
                                 Rows Removed by Join Filter: 74422
                                 ->  Seq Scan on trips_102011 t (actual rows=800 loops=1)
                                 ->  Materialize (actual rows=100 loops=800)
                                       ->  Seq Scan on periods_102077 p (actual rows=100 loops=1)
                         Planning Time: 9.146 ms
                         Execution Time: 16164.676 ms
 Planning Time: 8.585 ms
 Execution Time: 30385.270 ms
(26 rows)

Time: 30396.815 ms (00:30.397)
*/

-- Q4
-- List the pairs of vehicles that were both located within a region from Regions during a period from Periods.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT DISTINCT t1.VehicleId AS VehicleId1, t2.VehicleId AS VehicleId2, r.RegionId, p.PeriodId
FROM Trips t1, Trips1 t2, Regions1 r, Periods1 p
WHERE t1.VehicleId < t2.VehicleId AND t1.Trip && stbox(r.Geom, p.Period) AND
  t2.Trip && stbox(r.Geom, p.Period) AND 
  eIntersects(atTime(t1.Trip, p.Period), r.Geom) AND
  eIntersects(atTime(t2.Trip, p.Period), r.Geom)
ORDER BY t1.VehicleId, t2.VehicleId, r.RegionId, p.PeriodId;
/*
 Sort (actual rows=94 loops=1)
   Sort Key: remote_scan.vehicleid1, remote_scan.vehicleid2, remote_scan.regionid, remote_scan.periodid
   Sort Method: quicksort  Memory: 30kB
   ->  HashAggregate (actual rows=94 loops=1)
         Group Key: remote_scan.vehicleid1, remote_scan.vehicleid2, remote_scan.regionid, remote_scan.periodid
         Batches: 1  Memory Usage: 40kB
         ->  Custom Scan (Citus Adaptive) (actual rows=94 loops=1)
               ->  Distributed Subplan 3_1
                     Intermediate Data Size: 3546 kB
                     Result destination: Send to 1 nodes
                     ->  Seq Scan on trips1 t2 (actual rows=100 loops=1)
                     Planning Time: 0.000 ms
                     Execution Time: 0.110 ms
               Task Count: 32
               Tuple data received from nodes: 1504 bytes
               Tasks Shown: One of 32
               ->  Task
                     Tuple data received from node: 0 bytes
                     Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
                     ->  Unique (actual rows=0 loops=1)
                           ->  Sort (actual rows=0 loops=1)
                                 Sort Key: t1.vehicleid, intermediate_result.vehicleid, r.regionid, p.periodid
                                 Sort Method: quicksort  Memory: 25kB
                                 ->  Nested Loop (actual rows=0 loops=1)
                                       Join Filter: ((t1.vehicleid < intermediate_result.vehicleid) AND (intermediate_result.tri
p && stbox(r.geom, p.period)) AND eintersects(attime(intermediate_result.trip, p.period), r.geom))
                                       Rows Removed by Join Filter: 18900
                                       ->  Function Scan on read_intermediate_result intermediate_result (actual rows=10
0 loops=1)
                                       ->  Materialize (actual rows=189 loops=100)
                                             ->  Nested Loop (actual rows=189 loops=1)
                                                   Join Filter: ((t1.trip && stbox(r.geom, p.period)) AND eintersects(at
time(t1.trip, p.period), r.geom))
                                                   Rows Removed by Join Filter: 79811
                                                   ->  Nested Loop (actual rows=8000 loops=1)
                                                         ->  Seq Scan on periods1_102078 p (actual rows=10 loops=1)
                                                         ->  Materialize (actual rows=800 loops=10)
                                                               ->  Seq Scan on trips_102011 t1 (actual rows=800 loops=1)
                                                   ->  Materialize (actual rows=10 loops=8000)
                                                         ->  Seq Scan on regions1_102082 r (actual rows=10 loops=1)
                         Planning Time: 24.789 ms
                         Execution Time: 19715.865 ms
 Planning Time: 10.167 ms
 Execution Time: 35874.663 ms
(41 rows)

Time: 35891.383 ms (00:35.891)
*/

-- Q5
-- List the first time at which a vehicle visited a point in Points.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t.VehicleId, p.PointId, MIN(startTimestamp(atValues(t.Trip,p.Geom))) AS Instant
FROM Trips t, Points p
WHERE ST_Contains(trajectory(t.Trip), p.Geom)
GROUP BY t.VehicleId, p.PointId;
/*
 Custom Scan (Citus Adaptive) (actual rows=413 loops=1)
   Task Count: 32
   Tuple data received from nodes: 6608 bytes
   Tasks Shown: One of 32
   ->  Task
         Tuple data received from node: 224 bytes
         Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
         ->  GroupAggregate (actual rows=14 loops=1)
               Group Key: t.vehicleid, p.pointid
               ->  Incremental Sort (actual rows=56 loops=1)
                     Sort Key: t.vehicleid, p.pointid
                     Presorted Key: t.vehicleid
                     Full-sort Groups: 2  Sort Method: quicksort  Average Memory: 27kB  Peak Memory: 27kB
                     ->  Nested Loop (actual rows=56 loops=1)
                           Join Filter: st_contains(trajectory(t.trip), p.geom)
                           Rows Removed by Join Filter: 79944
                           ->  Index Scan using trips_vehicleid_startdate_seqno_key_102011 on trips_102011 t (actual rows=80
0 loops=1)
                           ->  Materialize (actual rows=100 loops=800)
                                 ->  Seq Scan on points_102079 p (actual rows=100 loops=1)
             Planning Time: 2.057 ms
             Execution Time: 88422.883 ms
 Planning Time: 7.110 ms
 Execution Time: 149079.245 ms
(23 rows)

Time: 149089.398 ms (02:29.089)
*/

---------------------------------------
-- Temporal Aggregate Queries
---------------------------------------

-- Q6 
-- Compute how many vehicles were active at each period in Periods.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT p.PeriodID, COUNT(*), tCount(atTime(t.Trip, p.Period))
FROM Trips t, Periods p
WHERE t.Trip && p.Period
GROUP BY p.PeriodID
ORDER BY p.PeriodID;
/*
 Sort (actual rows=96 loops=1)
   Sort Key: remote_scan.periodid
   Sort Method: external merge  Disk: 23136kB
   ->  HashAggregate (actual rows=96 loops=1)
         Group Key: remote_scan.periodid
         Batches: 7  Memory Usage: 43556kB  Disk Usage: 3193152kB
         ->  Custom Scan (Citus Adaptive) (actual rows=131837 loops=1)
               Task Count: 32
               Tuple data received from nodes: 4213 MB
               Tasks Shown: One of 32
               ->  Task
                     Tuple data received from node: 176 MB
                     Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
                     ->  Nested Loop (actual rows=5578 loops=1)
                           Join Filter: (t.trip && p.period)
                           Rows Removed by Join Filter: 74422
                           ->  Seq Scan on trips_102011 t (actual rows=800 loops=1)
                           ->  Materialize (actual rows=100 loops=800)
                                 ->  Seq Scan on periods_102077 p (actual rows=100 loops=1)
                         Planning Time: 18.532 ms
                         Execution Time: 18636.523 ms
 Planning Time: 1.416 ms
 Execution Time: 182572.190 ms
(23 rows)

Time: 182576.566 ms (03:02.577)
*/

-- Q7
-- Count the number of trips that were active during each hour in May 29, 2007.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
WITH TimeSplit(Period) AS (
  SELECT span(H, H + interval '1 hour')
  FROM generate_series(timestamptz '2007-05-29 00:00:00', 
    timestamptz '2007-05-29 23:00:00', interval '1 hour') AS H )
SELECT Period, COUNT(*)
FROM TimeSplit s, Trips t
WHERE s.Period && t.Trip AND atTime(Trip, Period) IS NOT NULL
GROUP BY s.Period
ORDER BY s.Period;
/*
 Sort (actual rows=0 loops=1)
   Sort Key: remote_scan.period
   Sort Method: quicksort  Memory: 25kB
   ->  HashAggregate (actual rows=0 loops=1)
         Group Key: remote_scan.period
         Batches: 1  Memory Usage: 40kB
         ->  Custom Scan (Citus Adaptive) (actual rows=0 loops=1)
               ->  Distributed Subplan 7_1
                     Intermediate Data Size: 624 bytes
                     Result destination: Send to 1 nodes
                     ->  Function Scan on generate_series h (actual rows=24 loops=1)
                     Planning Time: 0.000 ms
                     Execution Time: 0.053 ms
               Task Count: 32
               Tuple data received from nodes: 0 bytes
               Tasks Shown: One of 32
               ->  Task
                     Tuple data received from node: 0 bytes
                     Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
                     ->  HashAggregate (actual rows=0 loops=1)
                           Group Key: intermediate_result.period
                           Batches: 1  Memory Usage: 40kB
                           ->  Nested Loop (actual rows=0 loops=1)
                                 Join Filter: ((intermediate_result.period && t.trip) AND (attime(t.trip, intermediate_result.period) IS NOT NULL))
                                 Rows Removed by Join Filter: 16704
                                 ->  Function Scan on read_intermediate_result intermediate_result (actual rows=24 loops=1)
                                 ->  Materialize (actual rows=696 loops=24)
                                       ->  Seq Scan on trips_102020 t (actual rows=696 loops=1)
                         Planning Time: 9.862 ms
                         Execution Time: 3685.824 ms
 Planning Time: 4.427 ms
 Execution Time: 8270.718 ms
(32 rows)

Time: 8283.592 ms (00:08.284)
*/

---------------------------------------
-- Distance Queries
---------------------------------------

-- Q8
-- List the overall traveled distances of the vehicles during the periods from Periods.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t.VehicleId, p.PeriodId, p.Period,
  SUM(length(atTime(t.Trip, p.Period))) AS Distance
FROM Trips t, Periods p
WHERE t.Trip && p.Period
GROUP BY t.VehicleId, p.PeriodId, p.Period
ORDER BY t.VehicleId, p.PeriodId;
/*
 Sort (actual rows=50273 loops=1)
   Sort Key: remote_scan.vehicleid, remote_scan.periodid
   Sort Method: external merge  Disk: 2464kB
   ->  Custom Scan (Citus Adaptive) (actual rows=50273 loops=1)
         Task Count: 32
         Tuple data received from nodes: 1767 kB
         Tasks Shown: One of 32
         ->  Task
               Tuple data received from node: 70 kB
               Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
               ->  HashAggregate (actual rows=1996 loops=1)
                     Group Key: t.vehicleid, p.periodid
                     Batches: 1  Memory Usage: 369kB
                     ->  Nested Loop (actual rows=5166 loops=1)
                           Join Filter: (t.trip && p.period)
                           Rows Removed by Join Filter: 69434
                           ->  Seq Scan on trips_102022 t (actual rows=746 loops=1)
                           ->  Materialize (actual rows=100 loops=746)
                                 ->  Seq Scan on periods_102077 p (actual rows=100 loops=1)
                   Planning Time: 12.326 ms
                   Execution Time: 19696.024 ms
 Planning Time: 2.326 ms
 Execution Time: 36540.385 ms
(23 rows)

Time: 36545.870 ms (00:36.546)
 */

-- Q9
-- List the minimum distance ever between each vehicle and each point from Points.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t.VehicleId, p.PointId, MIN(t.Trip |=| p.Geom) AS MinDistance
FROM Trips t, Points p
WHERE t.Trip && p.Geom
GROUP BY t.VehicleId, p.PointId
ORDER BY t.VehicleId, p.PointId;
/*
 Sort (actual rows=22122 loops=1)
   Sort Key: remote_scan.vehicleid, remote_scan.pointid
   Sort Method: quicksort  Memory: 1978kB
   ->  Custom Scan (Citus Adaptive) (actual rows=22122 loops=1)
         Task Count: 32
         Tuple data received from nodes: 346 kB
         Tasks Shown: One of 32
         ->  Task
               Tuple data received from node: 13 kB
               Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
               ->  GroupAggregate (actual rows=827 loops=1)
                     Group Key: t.vehicleid, p.pointid
                     ->  Sort (actual rows=9608 loops=1)
                           Sort Key: t.vehicleid, p.pointid
                           Sort Method: quicksort  Memory: 1210kB
                           ->  Nested Loop (actual rows=9608 loops=1)
                                 Join Filter: ((t.trip)::stbox && (p.geom)::stbox)
                                 Rows Removed by Join Filter: 59992
                                 ->  Seq Scan on trips_102020 t (actual rows=696 loops=1)
                                 ->  Materialize (actual rows=100 loops=696)
                                       ->  Seq Scan on points_102079 p (actual rows=100 loops=1)
                   Planning Time: 8.144 ms
                   Execution Time: 25174.986 ms
 Planning Time: 1.456 ms
 Execution Time: 41350.882 ms
(25 rows)

Time: 41354.946 ms (00:41.355)
 */

-- Q10
-- List the minimum temporal distance between each pair of vehicles.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t1.VehicleId AS Car1Id, t2.VehicleId AS Car2Id, minValue(t1.Trip <-> t2.Trip) AS MinDistance
FROM Trips t1, Trips1 t2
WHERE t1.VehicleId < t2.VehicleId AND timeSpan(t1.Trip) && timeSpan(t2.Trip)
GROUP BY t1.VehicleId, t2.VehicleId, t1.Trip, t2.Trip
ORDER BY t1.VehicleId, t2.VehicleId;
/*
 Sort (actual rows=1758 loops=1)
   Sort Key: remote_scan.car1id, remote_scan.car2id
   Sort Method: external merge  Disk: 411496kB
   ->  Custom Scan (Citus Adaptive) (actual rows=1758 loops=1)
         ->  Distributed Subplan 10_1
               Intermediate Data Size: 3546 kB
               Result destination: Send to 1 nodes
               ->  Seq Scan on trips1 t2 (actual rows=100 loops=1)
               Planning Time: 0.000 ms
               Execution Time: 0.127 ms
         Task Count: 32
         Tuple data received from nodes: 172 MB
         Tasks Shown: One of 32
         ->  Task
               Tuple data received from node: 11 MB
               Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
               ->  Group (actual rows=136 loops=1)
                     Group Key: t1.vehicleid, intermediate_result.vehicleid, t1.trip, intermediate_result.trip
                     ->  Sort (actual rows=136 loops=1)
                           Sort Key: t1.vehicleid, intermediate_result.vehicleid, t1.trip, intermediate_result.trip
                           Sort Method: external merge  Disk: 16272kB
                           ->  Nested Loop (actual rows=136 loops=1)
                                 ->  Function Scan on read_intermediate_result intermediate_result (actual rows=100 loops=1)
                                 ->  Index Scan using trips_vehicleid_startdate_seqno_key_102016 on trips_102016 t1 (actual rows=1 loops=100)
                                       Index Cond: (vehicleid < intermediate_result.vehicleid)
                                       Filter: (timespan(trip) && timespan(intermediate_result.trip))
                                       Rows Removed by Filter: 95
                   Planning Time: 6.506 ms
                   Execution Time: 799.433 ms
 Planning Time: 2.807 ms
 Execution Time: 7422.285 ms
(31 rows)

Time: 7432.573 ms (00:07.433)
*/

-- Q11
-- List the nearest approach time, distance, and shortest line between each pair of trips.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t1.VehicleId AS Car1Id, t1.TripId AS Trip1Id, t2.VehicleId AS Car2Id, 
  t2.TripId AS Trip2Id, timeSpan(nearestApproachInstant(t1.Trip, t2.Trip)) AS Time,
  nearestApproachDistance(t1.Trip, t2.Trip) AS Distance, 
  shortestLine(t1.Trip, t2.Trip) AS Line
FROM Trips t1, Trips1 t2
WHERE t1.VehicleId < t2.VehicleId AND timeSpan(t1.Trip) && timeSpan(t2.Trip)
ORDER BY t1.VehicleId, t1.TripId, t2.VehicleId, t2.TripId;
/*
 Sort (actual rows=1758 loops=1)
   Sort Key: remote_scan.car1id, remote_scan.trip1id, remote_scan.car2id, remote_scan.trip2id
   Sort Method: quicksort  Memory: 268kB
   ->  Custom Scan (Citus Adaptive) (actual rows=1758 loops=1)
         ->  Distributed Subplan 11_1
               Intermediate Data Size: 3547 kB
               Result destination: Send to 1 nodes
               ->  Seq Scan on trips1 t2 (actual rows=100 loops=1)
               Planning Time: 0.000 ms
               Execution Time: 0.113 ms
         Task Count: 32
         Tuple data received from nodes: 153 kB
         Tasks Shown: One of 32
         ->  Task
               Tuple data received from node: 12 kB
               Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
               ->  Nested Loop (actual rows=136 loops=1)
                     ->  Function Scan on read_intermediate_result intermediate_result (actual rows=100 loops=1)
                     ->  Index Scan using trips_vehicleid_startdate_seqno_key_102016 on trips_102016 t1 (actual rows=1 loops=100)
                           Index Cond: (vehicleid < intermediate_result.vehicleid)
                           Filter: (timespan(trip) && timespan(intermediate_result.trip))
                           Rows Removed by Filter: 95
                   Planning Time: 7.505 ms
                   Execution Time: 1700.779 ms
 Planning Time: 2.784 ms
 Execution Time: 3644.611 ms
(26 rows)

Time: 3655.210 ms (00:03.655)
*/

-- Q12
-- List when and where a pairs of vehicles have been at 10 m or less from each other.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t1.VehicleId AS VehicleId1, t2.VehicleId AS VehicleId2, atTime(t1.Trip,
  -- getTime(tdwithin(t1.Trip, t2.Trip, 10.0, TRUE))) AS Position
  getTime(tDwithin(t1.Trip, t2.Trip, 10.0))) AS Position
FROM Trips t1, Trips1 t2
WHERE t1.VehicleId < t2.VehicleId AND t1.Trip && expandSpace(t2.Trip, 10) AND
  -- tDwithin(t1.Trip, t2.Trip, 10.0, TRUE) IS NOT NULL
  tDwithin(t1.Trip, t2.Trip, 10.0) #<= 10.0
ORDER BY t1.VehicleId, t2.VehicleId, Position;
/*
 Sort (actual rows=8 loops=1)
   Sort Key: remote_scan.vehicleid1, remote_scan.vehicleid2, remote_scan."position"
   Sort Method: quicksort  Memory: 45kB
   ->  Custom Scan (Citus Adaptive) (actual rows=8 loops=1)
         ->  Distributed Subplan 12_1
               Intermediate Data Size: 3546 kB
               Result destination: Send to 1 nodes
               ->  Seq Scan on trips1 t2 (actual rows=100 loops=1)
               Planning Time: 0.000 ms
               Execution Time: 0.113 ms
         Task Count: 32
         Tuple data received from nodes: 5981 bytes
         Tasks Shown: One of 32
         ->  Task
               Tuple data received from node: 0 bytes
               Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
               ->  Nested Loop (actual rows=0 loops=1)
                     ->  Function Scan on read_intermediate_result intermediate_result (actual rows=100 loops=1)
                     ->  Index Scan using trips_vehicleid_startdate_seqno_key_102016 on trips_102016 t1 (actual rows=0 loops=100)
                           Index Cond: (vehicleid < intermediate_result.vehicleid)
                           Filter: ((tdwithin(trip, intermediate_result.trip, '10'::double precision, true) IS NOT NULL) AND (trip && expandspace(intermediate_result.trip, '10'::double precision)))
                           Rows Removed by Filter: 96
                   Planning Time: 0.914 ms
                   Execution Time: 2340.810 ms
 Planning Time: 2.904 ms
 Execution Time: 4350.463 ms
(26 rows)

Time: 4358.306 ms (00:04.358)
 */
 
 ---------------------------------------
-- Nearest-Neighbor Queries
---------------------------------------

-- Q13
-- For each trip from Trips, list the three points from Points that have been closest to that vehicle.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t.VehicleId, P1.PointId, P1.Distance
FROM Trips t CROSS JOIN LATERAL (
  SELECT p.PointId, t.Trip |=| p.Geom AS Distance
  FROM Points1 p
  ORDER BY Distance LIMIT 3 ) AS P1
ORDER BY t.TripId, t.VehicleId, P1.Distance;
/*
 Sort (actual rows=56730 loops=1)
   Sort Key: remote_scan.worker_column_4, remote_scan.vehicleid, remote_scan.distance
   Sort Method: external merge  Disk: 1672kB
   ->  Custom Scan (Citus Adaptive) (actual rows=56730 loops=1)
         Task Count: 32
         Tuple data received from nodes: 1108 kB
         Tasks Shown: One of 32
         ->  Task
               Tuple data received from node: 47 kB
               Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
               ->  Nested Loop (actual rows=2400 loops=1)
                     ->  Seq Scan on trips_102011 t (actual rows=800 loops=1)
                     ->  Limit (actual rows=3 loops=800)
                           ->  Sort (actual rows=3 loops=800)
                                 Sort Key: ((t.trip |=| p.geom))
                                 Sort Method: top-N heapsort  Memory: 25kB
                                 ->  Seq Scan on points1_102080 p (actual rows=10 loops=800)
                   Planning Time: 2.249 ms
                   Execution Time: 7841.069 ms
 Planning Time: 2.216 ms
 Execution Time: 15228.086 ms
(21 rows)

Time: 15233.476 ms (00:15.233)
 */

-- Q14
-- For each trip from Trips, list the three vehicles that are closest to that vehicle

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT t1.VehicleId AS VehicleId1, C2.VehicleId AS VehicleId2, C2.Distance
FROM Trips t1 CROSS JOIN LATERAL (
  SELECT t2.VehicleId, t1.Trip |=| t2.Trip AS Distance
  FROM Trips t2
  WHERE t1.VehicleId < t2.VehicleId AND timeSpan(t1.Trip) && timeSpan(t2.Trip)
  ORDER BY Distance LIMIT 3 ) AS C2
ORDER BY t1.VehicleId, C2.VehicleId;
/*
complex joins are only supported when all distributed tables are co-located and joined on their distribution columns
 */

-- Q15
-- For each trip from Trips, list the points from Points that have that vehicle among their three nearest neighbors.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
WITH PointTrips AS (
  SELECT p.PointId, t2.VehicleId, t2.TripId, t2.Distance
  FROM Points1 p CROSS JOIN LATERAL (
    SELECT t1.VehicleId, t1.TripId, p.Geom |=| t1.Trip AS Distance
    FROM Trips t1
    ORDER BY Distance LIMIT 3 ) AS t2 )
SELECT t.VehicleId, t.TripId, p.PointId, PT.Distance
FROM Trips t CROSS JOIN Points p JOIN PointTrips PT
  ON t.VehicleId = PT.VehicleId AND t.TripId = PT.TripId AND p.PointId = PT.PointId
ORDER BY t.VehicleId, t.TripId, p.PointId;
/*
ERROR:  cannot push down this subquery
DETAIL:  Limit clause is currently unsupported when a lateral subquery references a column from a reference table (p)
 */

-- Q16
-- For each trip from Trips, list the vehicles having the vehicle of the trip among the three nearest neighbors.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
WITH TripDistances AS (
  SELECT t1.VehicleId AS VehicleId1, t1.TripId AS TripId1, T3.VehicleId AS VehicleId2, 
    T3.TripId AS TripId2, T3.Distance
  FROM Trips t1 CROSS JOIN LATERAL (
    SELECT t2.VehicleId, t2.TripId, minValue(t1.Trip <-> t2.Trip) AS Distance
    FROM Trips1 t2
    WHERE t1.VehicleId < t2.VehicleId AND timeSpan(t1.Trip) && timeSpan(t2.Trip)
    ORDER BY Distance LIMIT 3 ) AS T3 )
SELECT t1.VehicleId, t1.TripId, t2.VehicleId, t2.TripId, TD.Distance
FROM Trips t1 JOIN Trips1 t2 ON t1.VehicleId < t2.VehicleId
  JOIN TripDistances TD ON t1.VehicleId = TD.VehicleId1 AND t1.TripId = TD.TripId1 AND
  t2.VehicleId = TD.VehicleId2 AND t2.TripId = TD.TripId2
ORDER BY t1.VehicleId, t1.TripId, t2.VehicleId, t2.TripId;
/*
ERROR:  direct joins between distributed and local tables are not supported
HINT:  Use CTE's or subqueries to select from local tables and use them in joins
*/

-- Q17
-- For each group of ten disjoint vehicles, list the point(s) from Points, having the minimum aggregated distance from the given group of ten vehicles during the given period.

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
WITH Groups AS (
  SELECT ((ROW_NUMBER() OVER (ORDER BY V.VehicleId))-1)/10 + 1 AS GroupId, V.VehicleId
  FROM Vehicles V
  LIMIT 100 ),
SumDistances AS (
  SELECT G.GroupId, p.PointId,
    SUM(ST_Distance(trajectory(t.Trip), p.Geom)) AS SumDist
  FROM Groups G, Points p, Trips t
  WHERE t.VehicleId = G.VehicleId
  GROUP BY G.GroupId, p.PointId )
SELECT S1.GroupId, S1.PointId, S1.SumDist
FROM SumDistances S1
WHERE S1.SumDist <= ALL (
  SELECT SumDist
  FROM SumDistances S2
  WHERE S1.GroupId = S2.GroupId )
ORDER BY S1.GroupId, S1.PointId;
/*
 Custom Scan (Citus Adaptive) (actual rows=10 loops=1)
   ->  Distributed Subplan 20_1
         Intermediate Data Size: 33 kB
         Result destination: Write locally
         ->  HashAggregate (actual rows=1000 loops=1)
               Group Key: remote_scan.groupid, remote_scan.pointid
               Batches: 1  Memory Usage: 209kB
               ->  Custom Scan (Citus Adaptive) (actual rows=8800 loops=1)
                     ->  Distributed Subplan 21_1
                           Intermediate Data Size: 2200 bytes
                           Result destination: Send to 1 nodes
                           ->  Limit (actual rows=100 loops=1)
                                 ->  WindowAgg (actual rows=100 loops=1)
                                       ->  Sort (actual rows=100 loops=1)
                                             Sort Key: remote_scan.vehicleid
                                             Sort Method: quicksort  Memory: 25kB
                                             ->  Custom Scan (Citus Adaptive) (actual rows=632 loops=1)
                                                   Task Count: 32
                                                   Tuple data received from nodes: 2528 bytes
                                                   Tasks Shown: One of 32
                                                   ->  Task
                                                         Tuple data received from node: 88 bytes
                                                         Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
                                                         ->  Seq Scan on vehicles_102042 v (actual rows=22 loops=1)
                                                             Planning Time: 0.079 ms
                                                             Execution Time: 0.040 ms
                           Planning Time: 0.000 ms
                           Execution Time: 14.030 ms
                     Task Count: 32
                     Tuple data received from nodes: 172 kB
                     Tasks Shown: One of 32
                     ->  Task
                           Tuple data received from node: 8000 bytes
                           Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
                           ->  HashAggregate (actual rows=400 loops=1)
                                 Group Key: intermediate_result.groupid, p.pointid
                                 Batches: 1  Memory Usage: 817kB
                                 ->  Merge Join (actual rows=15800 loops=1)
                                       Merge Cond: (t.vehicleid = intermediate_result.vehicleid)
                                       ->  Nested Loop (actual rows=15801 loops=1)
                                             ->  Index Scan using trips_vehicleid_startdate_seqno_key_102039 on trips_102039 t (actual rows=159 loops=1)
                                             ->  Materialize (actual rows=99 loops=159)
                                                   ->  Seq Scan on points_102079 p (actual rows=100 loops=1)
                                       ->  Sort (actual rows=15895 loops=1)
                                             Sort Key: intermediate_result.vehicleid
                                             Sort Method: quicksort  Memory: 29kB
                                             ->  Function Scan on read_intermediate_result intermediate_result (actual rows=100 loops=1)
                               Planning Time: 0.244 ms
                               Execution Time: 12099.132 ms
         Planning Time: 0.000 ms
         Execution Time: 77678.783 ms
   Task Count: 1
   Tuple data received from nodes: 200 bytes
   Tasks Shown: All
   ->  Task
         Tuple data received from node: 200 bytes
         Node: host=localhost port=5432 dbname=brussels_citus_sf0.1
         ->  Sort (actual rows=10 loops=1)
               Sort Key: intermediate_result.groupid, intermediate_result.pointid
               Sort Method: quicksort  Memory: 25kB
               ->  Function Scan on read_intermediate_result intermediate_result (actual rows=10 loops=1)
                     Filter: (SubPlan 1)
                     Rows Removed by Filter: 990
                     SubPlan 1
                       ->  Function Scan on read_intermediate_result intermediate_result_1 (actual rows=5 loops=1000)
                             Filter: (intermediate_result.groupid = groupid)
                             Rows Removed by Filter: 49
             Planning Time: 0.139 ms
             Execution Time: 11.238 ms
 Planning Time: 14.594 ms
 Execution Time: 78657.576 ms
(71 rows)

Time: 156370.106 ms (02:36.370)
*/

------------------------------------------------------------------------------
