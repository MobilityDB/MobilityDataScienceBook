DROP TABLE IF EXISTS Grid;
CREATE TABLE Grid(Id, Row, Col, Cell, Boundary) AS
WITH Hist(Arr) AS (
  SELECT (stanumbers1::text::numeric[])[1:11]
  FROM pg_class c, pg_attribute a, pg_statistic s
  WHERE c.oid = s.starelid AND c.oid = a.attrelid AND a.attnum = s.staattnum AND
    c.relname = 'ships' AND a.attname = 'trip' ),
HistValues(Xbins, Ybins, Xmin, Ymin, Xmax, Ymax) AS (
  SELECT Arr[2], Arr[3], Arr[6], Arr[7], Arr[10], Arr[11]
  FROM Hist ),
Xcoords(Xarr) AS (
  SELECT array_agg(X)
  FROM HistValues, generate_series(Xmin, Xmax, (Xmax - Xmin) / Xbins) AS X ),
Ycoords(Yarr) AS (
  SELECT array_agg(Y)
  FROM HistValues, generate_series(Ymin, Ymax, (Ymax - Ymin) / Ybins) AS Y ),
Grid(Id, Row, Col, Cell) AS (
  -- 0-based numbering of the cells to match PostGIS numbering
  SELECT '(' || I - 1 || ',' || J - 1 || ')', I - 1, J - 1,
    stboxX(Xarr[I], Yarr[J], Xarr[I + 1], Yarr[J + 1], 25832)::geometry
  FROM Xcoords, Ycoords,
    generate_series(1, array_length(xarr, 1) - 1) AS I,
    generate_series(1, array_length(yarr, 1) - 1) AS J )
SELECT Id, Row, Col, ST_Boundary(Cell) AS Boundary
FROM Grid;

DROP TABLE IF EXISTS AlertBelt;
CREATE TABLE AlertBelt(Belt) AS
SELECT ST_MakeEnvelope(640730, 6058230, 654100, 6042487, 25832);

DROP TABLE IF EXISTS AlertBelt_Buffer;
CREATE TABLE AlertBelt_Buffer(Belt) AS
SELECT ST_MakeEnvelope(640730 - 40000, 6058230 - 40000, 654100 + 40000, 6042487 + 40000, 25832);