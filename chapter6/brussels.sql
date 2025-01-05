-------------------------------------------------------------------------------

ALTER TABLE Municipalities ADD COLUMN MunicipalityName_FR text;
UPDATE Municipalities SET MunicipalityName_FR = 
  regexp_replace(MunicipalityName, '(.*) - (.*)' , '\1');
  
SELECT TripId
FROM Trips 
ORDER BY area(trajectory::stbox) DESC
LIMIT 1;
-- 12079

DROP TABLE IF EXISTS Largest;
CREATE TABLE Largest AS
SELECT *, stbox(trajectory)::geometry AS box
FROM Trips 
WHERE TripId = 12079;

DROP TABLE IF EXISTS Largest10boxes;
CREATE TABLE Largest10boxes(Geom) AS
SELECT unnest(splitNStboxes(trip, 10))::geometry
FROM Trips
WHERE TripId = 12079;

DROP TABLE IF EXISTS LargestEach500boxes;
CREATE TABLE LargestEach500boxes(Geom) AS
SELECT unnest(splitEachNStboxes(trip, 500))::geometry
FROM Trips
WHERE TripId = 12079;

DROP TABLE IF EXISTS LargestBoxes5km;
CREATE TABLE LargestBoxes5km(Geom) AS
SELECT stbox((rec).tpoint)::geometry
FROM (SELECT spaceSplit(Trip, 5000) AS rec FROM Trips WHERE TripId = 12079) t;

DROP TABLE IF EXISTS Grid5km;
CREATE TABLE Grid5km(TileId, Geom) AS
WITH Extent(Box) AS (
  SELECT extent(Trip)
  FROM Trips )
SELECT (rec).index, ((rec).tile)::geometry
FROM (SELECT spaceTiles(Box, 5000) AS rec FROM Extent) t;

-------------------------------------------------------------------------------
