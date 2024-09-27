=================== DASHBOARD 1 ================
The query is the same for the two panels.
	
 -- Noise Heatmap
 
--Altitude of Flights Leaving Sydney

 WITH Sydney(SydneyEnv)
   AS (SELECT  ST_makeEnvelope(151.3,-33.75,150.93,-34.1,4326)),
AscSpan(Span) AS ( SELECT floatspan '[1,20]' ),
TimePeriod(Period) AS (
  SELECT tstzspan '[2020-06-01 00:00:00, 2020-06-01 23:59:00)' ),
TargetFlight(ICAO24, CallSign, RestFlight, RestGeoAlt, RestVertRate) AS (
  SELECT ICAO24, CallSign, atTime(Flight, Period), atTime(GeoAltitude, Period),
    atTime(VertRate, Period)
  FROM Flights, TimePeriod
  WHERE atTime(Flight, Period) IS NOT NULL ),
  --Ascending planes
FlightAscent(ICAO24, CallSign, RestGeoAlt, DescTrip, RestVertRate) AS (
  SELECT ICAO24, CallSign, RestGeoAlt,
    atTime(RestFlight, timeSpan(sequenceN(atValues(RestVertRate, Span), 1))),
    atTime(RestVertRate, timeSpan(sequenceN(atValues(RestVertRate, Span), 1)))
  FROM TargetFlight, AscSpan
  WHERE atValues(RestVertRate, Span) IS NOT NULL ),
FinalOutput(ICAO24, CallSign, GeoAltitude, VertRate, Lon, Lat ) AS (
  SELECT ICAO24, CallSign, getValue(unnest(instants(RestGeoAlt))),
  getValue(unnest(instants(RestVertRate))),
  ST_X(getValue(unnest(instants(DescTrip)))::geometry),
  ST_Y(getValue(unnest(instants(DescTrip)))::geometry)
  FROM FlightAscent)  
SELECT ICAO24, CallSign, GeoAltitude, VertRate, Lon, Lat
FROM FinalOutput
WHERE VertRate IS NOT NULL AND GeoAltitude IS NOT NULL AND
  GeoAltitude < 2000;

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

 WITH Time(Period) AS (
  SELECT tstzspan '[2020-06-01 08:00:00, 2020-06-01 09:00:00)'),
  
FlightTimeSlice(ICAO24, CallSign, TripTimeSlice, GeoAltitude) AS (
  SELECT ICAO24, CallSign, atTime(Flight, Period), atTime(GeoAltitude, Period) as geoaltitude
  FROM Flights TABLESAMPLE SYSTEM(5), Time
  WHERE atTime(Flight, Period) IS NOT NULL), 

FinalOutput AS (
  SELECT ICAO24, CallSign,
	getValue(unnest(instants(GeoAltitude))) AS Altitude,
  ST_X(getValue(unnest(instants(TripTimeSlice)))::geometry) AS Lon,
  ST_Y(getValue(unnest(instants(TripTimeSlice)))::geometry) AS Lat
  FROM FlightTimeSlice
  WHERE TripTimeSlice  IS NOT NULL  )
  
SELECT *
FROM FinalOutput 

------------------------

-- Cruising Flights Panel

 WITH TimeAltitude(Period, AltSpan) AS (
  SELECT tstzspan '[2020-06-01 08:00:00, 2020-06-01 09:00:00)', floatspan '[3000,8000)' ),
FlightTimeSlice(ICAO24, CallSign, TripTimeSlice, AltTimeSlice) AS (
  SELECT ICAO24, CallSign, atTime(Flight, Period), atTime(GeoAltitude, Period)
  FROM Flights, TimeAltitude
  WHERE atTime(Flight, Period) IS NOT NULL ),
FlightTimeSliceCruising(ICAO24, CallSign, CruisingTrip, CruisingAltitude) AS (
  SELECT ICAO24, CallSign,
    atTime(TripTimeSlice, getTime(atValues(AltTimeSlice, AltSpan))),
    atValues(AltTimeSlice, AltSpan)
  FROM FlightTimeSlice, TimeAltitude
  WHERE atValues(AltTimeSlice, AltSpan) IS NOT NULL AND
    atTime(TripTimeSlice, getTime(atValues(AltTimeSlice, AltSpan))) IS NOT NULL ),
Instants(ICAO24, T) AS (
  SELECT ICAO24, unnest(set(timestamps(CruisingTrip)) +
    set(timestamps(CruisingAltitude)))
  FROM FlightTimeSliceCruising
  GROUP BY ICAO24, CruisingTrip, CruisingAltitude)
SELECT f.ICAO24, f.CallSign, getValue(atTime(CruisingAltitude, T)) AS Alt,
 ST_X(getValue(atTime(CruisingTrip, T))::geometry) AS Lon,
 ST_Y(getValue(atTime(CruisingTrip, T))::geometry) AS Lat, T 
FROM FlightTimeSliceCruising f, Instants i
WHERE f.ICAO24 = i.ICAO24 AND
  getValue(atTime(CruisingAltitude, T)) IS NOT NULL AND
  getValue(atTime(CruisingTrip, T)) IS NOT NULL
ORDER BY T;

--------------------------

-- FLIGHTS TAKING-OFF PANEL

   -- FLIGHTS TAKING-OFF PANEL

  -- Span for determining Ascending planes
WITH AscSpan(Span) AS ( SELECT floatspan '[1,20]' ),
-- Time period we are interested in
TimePeriod(Period) AS (
  SELECT tstzspan '[2020-06-01 08:00:00, 2020-06-01 10:00:00)' ),
-- Planes in the given time period
TargetFlight(ICAO24, CallSign, RestFlight, RestGeoAlt, RestVertRate) AS (
  SELECT ICAO24, CallSign, atTime(Flight, Period), atTime(GeoAltitude, Period),
    atTime(VertRate, Period)
  FROM Flights, TimePeriod
  WHERE atTime(Flight, Period) IS NOT NULL ),
-- Ascending planes
FlightsAscent(ICAO24, CallSign, RestGeoAlt, AscFlight, RestVertRate) AS (
  SELECT ICAO24, CallSign,
    atTime(RestGeoAlt, getTime(atValues(RestVertRate, Span))),
    atTime(RestFlight, getTime(atValues(RestVertRate, Span))),
    atValues(RestVertRate, Span)
  FROM TargetFlight, AscSpan
  WHERE atValues(RestVertRate, Span) IS NOT NULL AND
    atTime(RestGeoAlt, getTime(atValues(RestVertRate, Span))) IS NOT NULL AND
    atTime(RestFlight, getTime(atValues(RestVertRate, Span))) IS NOT NULL ),
Instants(ICAO24, T) AS (
  SELECT ICAO24, unnest(set(timestamps(RestGeoAlt)) +
    set(timestamps(AscFlight)) + set(timestamps(RestVertRate)))
  FROM  FlightsAscent
  GROUP BY ICAO24, RestGeoAlt, AscFlight, RestVertRate )
SELECT f.ICAO24, f.CallSign, getValue(atTime(RestGeoAlt, T)) AS GeoAltitude,
  getValue(atTime(RestVertRate, T)) AS VertRate,
  ST_X(getValue(atTime(AscFlight, T))::geometry) AS Lon,
  ST_Y(getValue(atTime(AscFlight, T))::geometry) AS Lat, T
FROM FlightsAscent f, Instants i
WHERE f.ICAO24 = i.ICAO24 AND
  getValue(atTime(AscFlight, T)) IS NOT NULL AND
  getValue(atTime(RestGeoAlt, T)) IS NOT NULL AND
  getValue(atTime(RestVertRate, T)) IS NOT NULL AND
  getValue(atTime(RestGeoAlt, T)) < 1000
ORDER BY T;

--------------------------

-- LANDING FLIGHTS PANEL


 -- Span for determining descending planes
WITH DescSpan(Span) AS ( SELECT floatspan '[-20,0]' ),
-- Time period we are interested in
TimePeriod(Period) AS (
  SELECT tstzspan '[2020-06-01 08:00:00, 2020-06-01 10:00:00)' ),
-- Planes in the given time period
TargetFlight(ICAO24, CallSign, RestFlight, RestGeoAlt, RestVertRate) AS (
  SELECT ICAO24, CallSign, atTime(Flight, Period), atTime(GeoAltitude, Period),
    atTime(VertRate, Period)
  FROM Flights, TimePeriod
  WHERE atTime(Flight, Period) IS NOT NULL ),
-- Descending planes
FlightDescent(ICAO24, CallSign, RestGeoAlt, DescFlight, RestVertRate) AS (
  SELECT ICAO24, CallSign,
    atTime(RestGeoAlt, getTime(atValues(RestVertRate, Span))),
    atTime(RestFlight, getTime(atValues(RestVertRate, Span))),
    atValues(RestVertRate, Span)
  FROM TargetFlight, DescSpan
  WHERE atValues(RestVertRate, Span) IS NOT NULL AND
    atTime(RestGeoAlt, getTime(atValues(RestVertRate, Span))) IS NOT NULL AND
    atTime(RestFlight, getTime(atValues(RestVertRate, Span))) IS NOT NULL ),
Instants(ICAO24, T) AS (
  SELECT ICAO24, unnest(set(timestamps(RestGeoAlt)) +
    set(timestamps(DescFlight)) + set(timestamps(RestVertRate)))
  FROM FlightDescent
  GROUP BY ICAO24, RestGeoAlt, DescFlight, RestVertRate )
SELECT f.ICAO24, f.CallSign, getValue(atTime(RestGeoAlt, T)) AS GeoAltitude,
  getValue(atTime(RestVertRate, T)) AS VertRate,
  ST_X(getValue(atTime(DescFlight, T))::geometry) AS Lon,
  ST_Y(getValue(atTime(DescFlight, T))::geometry) AS Lat, T
FROM FlightDescent f, Instants i
WHERE f.ICAO24 = i.ICAO24 AND
  getValue(atTime(DescFlight, T)) IS NOT NULL AND
  getValue(atTime(RestGeoAlt, T)) IS NOT NULL AND
  getValue(atTime(RestVertRate, T)) IS NOT NULL AND
  getValue(atTime(RestGeoAlt, T)) < 1000
ORDER BY T;

 
