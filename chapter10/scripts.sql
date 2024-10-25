CREATE DATABASE AISAnalysis;

CREATE EXTENSION PostGIS;
CREATE EXTENSION MobilityDB;

--sample trace for the running example
DROP TABLE IF EXISTS routePoints;
CREATE TABLE routePoints AS
    WITH r(t, x, y) AS(
        VALUES (1,0,0), (2,0.5,1.2), (3,1,1.7), (4,1.5,2.1), (5,2,2.5), (6,5,5.9),
        (7,6,6), (8,8,3.5), (9,12,1), (10,14,2), (11,16,3.8), (12,18,6)
    )
    SELECT t, st_makepoint(x, y) AS pt FROM r;

--original linestring
DROP TABLE IF EXISTS routeLineString;
CREATE TABLE routeLineString AS
    SELECT st_makeline(pt ORDER BY t) AS l FROM routePoints;

--Simplification idea 1: keep only first and last points
DROP TABLE IF EXISTS simplifiedOD;
CREATE TABLE simplifiedOD(l) AS(
    SELECT st_makeline(pt)
    FROM (
        SELECT t,
               pt,
               row_number() OVER () AS rn,
               count(*) OVER () AS total_count
        FROM routePoints
    ) t
    WHERE rn = 1
       OR rn = total_count);


--Simplification idea 2: keep one out of two points
DROP TABLE IF EXISTS simplifiedSample;
CREATE TABLE simplifiedSample(l) AS(
    SELECT st_makeline(pt)
    FROM (
        SELECT t,
               pt,
               row_number() OVER () AS rn,
               count(*) OVER () AS total_count
        FROM routePoints
    ) t
    WHERE rn%2 = 0);

--Simplification idea 3: Douglas Peucker
DROP TABLE IF EXISTS simplified3;
CREATE TABLE simplified3(l) AS(
    SELECT st_simplify(st_makeline(pt), 0)
    FROM (
        SELECT pt
        FROM routePoints
    ) t
    );

SELECT sum(st_distance(pt, l))
FROM routePoints, simplifiedOD;
--20.96590088691635

SELECT sum(st_distance(pt, l))
FROM routePoints, simplifiedSample;
--3.73336399294522

SELECT sum(st_distance(pt, l))
FROM routePoints, simplified3;
--0



--AIS simplification (similar to CH5)
/*****************************************************************************
 * SQL script to input one day of AIS data and store it in MobilityDB
 *****************************************************************************/

  -- Set parameters for input timestamps in the CSV file
  SET TimeZone = 'UTC';
  SET DateStyle = 'ISO, DMY';

  -- Create input table to hold CSV records
  DROP TABLE IF EXISTS AISInput;
  CREATE TABLE AISInput(
    T timestamp,
    TypeOfMobile varchar(100),
    MMSI integer,
    Latitude float,
    Longitude float,
    NavigationalStatus varchar(100),
    ROT float,
    SOG float,
    COG float,
    Heading integer,
    IMO varchar(100),
    CallSign varchar(100),
    Name varchar(100),
    ShipType varchar(100),
    CargoType varchar(100),
    Width float,
    Length float,
    TypeOfPositionFixingDevice varchar(100),
    Draught float,
    Destination varchar(100),
    ETA varchar(100),
    DataSourceType varchar(100),
    SizeA float,
    SizeB float,
    SizeC float,
    SizeD float,
    Geom geometry(Point, 4326)
  );

COPY AISInput(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus,
  ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length,
  TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType,
  SizeA, SizeB, SizeC, SizeD)
FROM '/home/mahmoud/Desktop/MobilityDataScience/Book/my_notebooks/aisdk-2024-03-01.csv' DELIMITER  ',' CSV HEADER;

UPDATE AISInput
SET Latitude = NULL, Longitude = NULL
WHERE Longitude NOT BETWEEN -180 AND 180 OR Latitude NOT BETWEEN -90 AND 90;

  -- Set to NULL 'undefined' values and add geometry to the records
UPDATE AISInput SET
    NavigationalStatus = CASE NavigationalStatus WHEN 'Unknown value' THEN NULL END,
    IMO = CASE IMO WHEN 'Unknown' THEN NULL END,
    ShipType = CASE ShipType WHEN 'Undefined' THEN NULL END,
    TypeOfPositionFixingDevice = CASE TypeOfPositionFixingDevice
      WHEN 'Undefined' THEN NULL END,
    Geom = ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326);

  -- Filter out duplicate timestamps and valid but out-of-range values of
  -- latitude and longitude
DROP TABLE IF EXISTS AISInputFiltered;
CREATE TABLE AISInputFiltered AS
  SELECT DISTINCT ON (MMSI,T) *
  FROM AISInput
  WHERE Longitude BETWEEN -16.1 AND 32.88 AND Latitude BETWEEN 40.18 AND 84.17
  ORDER BY MMSI, T;

-- Create table with only the columns used for creating temporal types
DROP TABLE IF EXISTS AISInputTarget;
CREATE TABLE AISInputTarget AS
  SELECT MMSI, Length, T, SOG, COG, Geom
  FROM AISInputFiltered;

-- Create table with temporal types
DROP TABLE IF EXISTS Ships;
CREATE TABLE Ships(MMSI integer, Trip tgeompoint, SOG tfloat, COG tfloat);

-- Notice that we do not fill the Length attribute since for a single MMSI
-- some records have NULL and others have a value
INSERT INTO Ships(MMSI, Trip, SOG, COG)
      SELECT MMSI,
        tgeompointSeq(array_agg(tgeompoint(ST_Transform(Geom, 25832), T) ORDER BY T)),
        tfloatSeq(array_agg(tfloat(SOG, T) ORDER BY T) FILTER (WHERE SOG IS NOT NULL)),
        tfloatSeq(array_agg(tfloat(COG, T) ORDER BY T) FILTER (WHERE COG IS NOT NULL))
      FROM AISInputTarget
      GROUP BY MMSI;


SELECT SUM(numinstants(Trip)) AS Trip,
       SUM(numinstants(douglaspeuckersimplify(Trip, 10, FALSE))) AS DP_10m,
       SUM(numinstants(douglaspeuckersimplify(Trip, 10, TRUE))) AS SED_10m,
       SUM(numinstants(douglaspeuckersimplify(Trip, 50, FALSE))) AS DP_50m,
       SUM(numinstants(douglaspeuckersimplify(Trip, 50, TRUE))) AS SED_50m,
       SUM(numinstants(douglaspeuckersimplify(Trip, 100, FALSE))) AS DP_100m,
       SUM(numinstants(douglaspeuckersimplify(Trip, 100, TRUE))) AS SED_100m
FROM Ships
--[2024-10-14 20:18:44] 1 row retrieved starting from 1 in 27 s 627 ms (execution: 27 s 617 ms, fetching: 10 ms)


-- generate a bitmap of the recorded data: time v.s. MMSI
WITH time_intervals AS (
    -- Generate a series of 1-minute intervals for the entire day
    SELECT generate_series(
        '2024-03-01 00:00:00'::timestamp,
        '2024-03-01 23:59:59'::timestamp,
        '10 minute'::interval
    ) AS time_bin
),
observations AS (
    -- Get distinct MMSI and corresponding observation times
    SELECT MMSI, date_bin('10 minutes', t, TIMESTAMP '2024-03-01 00:00:00') AS observation_bin
    FROM AISInputTarget
    WHERE T >= '2024-03-01 00:00:00' AND T <= '2024-03-01 23:59:59'
)
-- Create the matrix where each cell represents whether an observation exists (1) or not (0)
SELECT mmsi_ids.MMSI, time_intervals.time_bin,
    COUNT(observations.observation_bin) > 0 AS has_observations
FROM
    (SELECT DISTINCT MMSI FROM AISInputTarget) AS mmsi_ids
    CROSS JOIN time_intervals
    LEFT JOIN observations ON mmsi_ids.MMSI = observations.MMSI AND time_intervals.time_bin = observations.observation_bin
GROUP BY mmsi_ids.MMSI, time_intervals.time_bin;


DROP FUNCTION build_tgeompoints;
CREATE OR REPLACE FUNCTION build_tgeompoints(geoms geometry[],
  times timestamptz[], max_distance float, max_time_interval interval)
RETURNS tgeompoint AS $$
DECLARE
  result tgeompoint;
BEGIN
  WITH gap_calculations AS (
    SELECT location, t, time_diff, distance
    FROM (
        SELECT *,
               t - lag(t) OVER time_window AS time_diff,
               location <-> lag(location) OVER time_window AS distance
        FROM (SELECT * FROM unnest(geoms, times)) AS unnested(location, t)
        WINDOW time_window AS (ORDER BY t)
    ) AS delta
  ),
  grouped_points AS (
    SELECT location, t, group_id
    FROM (
        SELECT gap_calculations.*,
               COUNT(*) FILTER (WHERE time_diff > max_time_interval OR
                 distance > max_distance OR location IS NULL)
               OVER (ORDER BY t) AS group_id
        FROM gap_calculations
    ) AS grouped_data
  ),
  sequence_creation AS (
    SELECT group_id,
      tgeompointseq(array_agg(tgeompoint(location, t)
        ORDER BY t)) AS seq
    FROM grouped_points
    WHERE location IS NOT NULL
    GROUP BY group_id
  )
  SELECT tgeompointseqset(array_agg(seq ORDER BY group_id)) INTO result
  FROM sequence_creation;

  RETURN result;
END;
$$ LANGUAGE plpgsql;


WITH raw AS(
    SELECT MMSI,
           ARRAY_AGG(ST_Transform(Geom, 25832) ORDER BY t) AS GeomArr,
           ARRAY_AGG(t ORDER BY t) AS TArr
    FROM AISInputTarget
    GROUP BY MMSI),
Sequences AS(
SELECT MMSI, build_tgeompoints(GeomArr, TArr, 1000, '30 minute'::interval) AS TripWithGaps
FROM raw)
SELECT SUM(numsequences(TripWithGaps)) AS numSequences, COUNT(MMSI) AS numShips
FROM Sequences;
--[2024-10-15 16:49:59] 1 row retrieved starting from 1 in 28 s 199 ms (execution: 28 s 189 ms, fetching: 10 ms)
--500m, 10min, 28444,3112
--1km, 30 min, 12538,3112

WITH raw AS(
    SELECT MMSI,
           ARRAY_AGG(tgeompoint(ST_Transform(Geom, 25832), t) ORDER BY t) AS TGeomArr
    FROM AISInputTarget
    GROUP BY MMSI),
Sequences AS(
SELECT MMSI, tgeompointSeqSetGaps(TGeomArr, '10 minute'::interval,  500) AS TripWithGaps
FROM raw)
SELECT SUM(numsequences(TripWithGaps)) AS numSequences, COUNT(MMSI) AS numShips
FROM Sequences;
-- [2024-10-15 16:56:03] 1 row retrieved starting from 1 in 11 s 610 ms (execution: 11 s 593 ms, fetching: 17 ms)
-- 28444,3112

CREATE SEQUENCE cnt START 1;
CREATE TABLE TripsByGaps AS
WITH raw AS(
    SELECT MMSI,
           ARRAY_AGG(ST_Transform(Geom, 25832) ORDER BY t) AS GeomArr,
           ARRAY_AGG(t ORDER BY t) AS TArr
    FROM AISInputTarget
    GROUP BY MMSI),
Sequences AS(
SELECT MMSI, build_tgeompoints(GeomArr, TArr, 1000, '30 minute'::interval) AS TripWithGaps
FROM raw)
SELECT MMSI, nextval('cnt') tripID, unnest(sequences(TripWithGaps)) AS trip
FROM Sequences;

ALTER TABLE Ships ADD COLUMN traj geometry;
UPDATE Ships SET traj= trajectory(trip);

ALTER TABLE TripsByGaps ADD COLUMN traj geometry;
UPDATE TripsByGaps SET traj= trajectory(trip);


CREATE TABLE TripsByGapsSample AS
    SELECT * FROM TripsByGaps WHERE MMSI IN (211608700, 630001042);

ALTER TABLE TripsByGapsSample ADD COLUMN traj geometry;
UPDATE TripsByGapsSample SET traj= trajectory(trip);

DROP TABLE IF EXISTS TripsByStops;
CREATE TABLE TripsByStops AS
    SELECT MMSI, nextval('cnt') tripID, unnest(sequences(attime(trip, gettime(atValues(speed(trip) #> 3, TRUE))))) trip
    FROM Ships;
-- too many stops, so we need to only consider extended stops, e.g., of 60 minutes

DROP TABLE IF EXISTS TripsByStops;
CREATE TABLE TripsByStops AS
    WITH stops AS (
        SELECT MMSI,
               nextval('cnt') tripID,
               unnest(spans(whentrue(speed(trip) #< 1))) AS stopSpan
        FROM Ships
    )
    ,longStops AS (
        SELECT MMSI, tripID, stopSpan
        FROM stops
        WHERE duration(stopSpan) >= '60 minutes'::interval
    )
    , stopIntervals AS (
        SELECT MMSI, spanset(ARRAY_AGG(stopSpan)) stopSpanSet
        FROM longStops
        GROUP BY MMSI
    )
    , withStops AS (
        SELECT s.MMSI, minustime(trip, stopSpanSet) AS trip
        FROM Ships s
                 JOIN stopIntervals i ON s.MMSI = i.MMSI
        WHERE minustime(trip, stopSpanSet) IS NOT NULL
    )
    SELECT MMSI, nextval('cnt'), unnest(sequences(trip)) AS trip
    FROM withStops;

ALTER TABLE TripsByStops ADD COLUMN traj geometry;
UPDATE TripsByStops SET traj= trajectory(trip);


DROP TABLE IF EXISTS ports;
CREATE TABLE Ports AS
        SELECT *
        FROM (VALUES ('Rodby', st_buffer(st_setsrid(ST_MakePoint(651295, 6058400), 25832), 1000)),
                     ('Puttgarden', st_buffer(st_setsrid(ST_MakePoint(644639, 6042308), 25832), 1000)))
             AS p(Portname, geom);

DROP TABLE IF EXISTS tripsByPoIs;
CREATE TABLE tripsByPoIs AS
    WITH portIntersections AS (
        SELECT MMSI, p.name, Geom
        FROM harbours P, Ships S
        WHERE eintersects(S.Trip, geom)
    ), intersectionAggregates AS (
        SELECT MMSI, st_union(geom) AS geom
        FROM portIntersections
        GROUP BY MMSI
    ), tripsWithIntersection AS (
        SELECT s.MMSI, minusgeometry(trip, geom) trip
        FROM ships s,
             intersectionAggregates h
        WHERE s.MMSI = h.MMSI
    )
    SELECT MMSI, nextval('cnt'), unnest(sequences(trip)) trip
    FROM tripsWithIntersection;

ALTER TABLE tripsByPoIs ADD COLUMN traj geometry;
UPDATE tripsByPoIs SET traj= trajectory(trip);


--=====================================================================
--Heatmaps
--=====================================================================

DROP TABLE IF EXISTS tHeatmap;
CREATE TABLE tHeatmap AS
    SELECT (grid).point AS cell_geom, (grid).time AS cell_t
    FROM(
        SELECT spaceTimeSplit(trip, 10000, interval '1 hour') AS grid
        FROM Ships) AS tmp;
--[2024-10-21 12:37:53] 86,591 rows affected in 3 s 542 ms

CREATE TABLE spatialHeatMap AS
    SELECT cell_geom, count(*)
    FROM tHeatmap
    GROUP BY cell_geom;

CREATE TABLE timeHeatMap AS
    SELECT cell_t, count(*)
    FROM tHeatmap
    GROUP BY cell_t;

DROP TABLE IF EXISTS flow;
CREATE TABLE flow AS
    SELECT a.id as harbourA, b.id as harbourB, MMSI
    FROM (harbours a JOIN harbours b ON a.id > b.id) JOIN
        ships ON (eintersects(trip, a.geom) AND eintersects(trip, b.geom))
--[2024-10-21 17:01:40] 630 rows affected in 31 m 38 s 77 ms

--=====================================================================
--Trajectory Similarity
--=====================================================================


SELECT dynTimeWarpDistance(tgeompoint '[Point(0 1)@2001-01-01, Point(4 0)@2001-01-03, Point(5 1)@2001-01-05, Point(9 1)@2001-01-07]',
tgeompoint '[Point(1 2)@2001-01-01, Point(0 4)@2001-01-03, Point(9 2)@2001-01-05]');
--11.850716732682386

SELECT frechetDistance(tgeompoint '[Point(0 1)@2001-01-01, Point(4 0)@2001-01-03, Point(5 1)@2001-01-05, Point(9 1)@2001-01-07]',
tgeompoint '[Point(1 2)@2001-01-01, Point(0 4)@2001-01-03, Point(9 2)@2001-01-05]');
--5.385164807134504