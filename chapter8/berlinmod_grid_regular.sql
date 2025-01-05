DROP TABLE IF EXISTS Grid;
CREATE TABLE Grid(TileId, Tile, Geom) AS
WITH Hist(arr) AS (
  SELECT (stanumbers1::text::numeric[])[1:11]
  FROM pg_class c, pg_attribute a, pg_statistic s
  WHERE c.oid = s.starelid AND c.oid = a.attrelid AND a.attnum = s.staattnum AND
    c.relname = 'trips' AND a.attname = 'trip' ),
Extent(box, xsize, ysize, origin) AS (
  SELECT stboxX(arr[6], arr[7], arr[10], arr[11], 3857),
    (arr[10] - arr[6]) / 4, (arr[11] - arr[7]) / 3, ST_Point(arr[6], arr[7], 3857)
  FROM Hist )
SELECT (rec).index, (rec).tile, ((rec).tile)::geometry
FROM (
  SELECT spaceTiles(box, xsize, ysize, origin) AS rec
  FROM Extent ) AS t;

CREATE TABLE IxellesBox AS
SELECT (geom::stbox)::geometry
FROM Communes
WHERE name LIKE '%Ixelles%';

