-----------------------------------------

CREATE TABLE MAD_BRU_RIX(Madrid, Brussels, Riga) AS
SELECT geography 'Point(-3.703333 40.416944)', geography 'Point(4.35 50.85)',
  geography 'Point(24.106389 56.948889)';

DROP TABLE IF EXISTS MAD_RIX_GEOM;
CREATE TABLE MAD_RIX_GEOM(Flight) AS
SELECT tgeompoint 'SRID=4326;[Point(-3.703333 40.416944)@2000-01-01 08:00, Point(24.106389 56.948889)@2000-01-01 14:30]';
ALTER TABLE MAD_RIX_GEOM ADD COLUMN Traj geometry;
UPDATE MAD_RIX_GEOM SET Traj = trajectory(Flight);

DROP TABLE IF EXISTS MAD_RIX_GEOM_Pts;
CREATE TABLE MAD_RIX_GEOM_Pts(Point) AS
SELECT ST_LineInterpolatePoints(Traj, 0.02, true)
FROM MAD_RIX_GEOM;

CREATE TABLE Shortest_GEOM(Line) AS
SELECT ST_ShortestLine(Traj, Brussels)
FROM MAD_RIX_GEOM, MAD_BRU_RIX;

-----------------------------------------

CREATE TABLE MAD_RIX(Flight) AS
SELECT tgeogpoint '[Point(-3.703333 40.416944)@2000-01-01 08:00, Point(24.106389 56.948889)@2000-01-01 14:30]';
ALTER TABLE MAD_RIX ADD COLUMN Traj geography;
UPDATE MAD_RIX SET Traj = trajectory(Flight);

CREATE TABLE MAD_RIX_Pts(Point) AS
SELECT ST_LineInterpolatePoints(Traj, 0.02, true)
FROM MAD_RIX;

CREATE TABLE Shortest(Line) AS
SELECT ST_ShortestLine(Traj, Brussels)
FROM MAD_RIX, MAD_BRU_RIX;

-----------------------------------------
