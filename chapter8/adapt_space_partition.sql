/*****************************************************************************
 * SQL script to partition a column Trip in a table Trips using space-only
 * multidimensional tiling.
 * Currently the script does not take into account the Z dimension
 *****************************************************************************/

  SELECT NULL AS "Creating table AllPoints ...";
  DROP TABLE IF EXISTS AllPoints;
  CREATE TABLE AllPoints(Point) AS
  WITH Instants(Inst) AS (
    SELECT unnest(instants(Trip))
    FROM Trips )
  SELECT getValue(Inst)
  FROM Instants;

  SELECT NULL AS "Creating table XTiles ...";
  DROP TABLE IF EXISTS XTiles;
  CREATE TABLE XTiles(XTileId, XSpan) AS
  WITH Tiles1(TileId, X) AS (
    SELECT NTILE(4) OVER(ORDER BY ST_X(Point)), ST_X(Point)
    FROM AllPoints ),
  Tiles2(TileId, X, RowNo) AS (
    SELECT TileId, X,
      ROW_NUMBER() OVER (PARTITION BY TileId ORDER BY X)
    FROM Tiles1 )
  SELECT TileId, span(X, COALESCE(LEAD(X, 1) OVER (ORDER BY X),
    (SELECT MAX(ST_X(Point)) FROM AllPoints)))
  FROM Tiles2
  WHERE RowNo = 1;

  SELECT NULL AS "Creating table YTiles ...";
  DROP TABLE IF EXISTS YTiles;
  CREATE TABLE YTiles(YTileId, YSpan) AS
  WITH Tiles1(TileId, Y) AS (
    SELECT NTILE(3) OVER(ORDER BY ST_Y(Point)), ST_Y(Point)
    FROM AllPoints ),
  Tiles2(TileId, Y, RowNo) AS (
    SELECT TileId, Y,
      ROW_NUMBER() OVER (PARTITION BY TileId ORDER BY Y)
    FROM Tiles1 )
  SELECT TileId, span(Y, COALESCE(LEAD(Y, 1) OVER (ORDER BY Y),
    (SELECT MAX(ST_Y(Point)) FROM AllPoints)))
  FROM Tiles2
  WHERE RowNo = 1;

  SELECT NULL AS "Creating table AdaptiveGrid ...";
  DROP TABLE IF EXISTS AdaptiveGrid;
  CREATE TABLE AdaptiveGrid(TileId, RowNo, ColNo, Tile, Geom) AS
  WITH TableSRID(SRID) AS (
    SELECT ST_SRID(Point)
    FROM AllPoints
    LIMIT 1 ),
  Tiles(TileId, RowNo, ColNo, Tile) AS (
    SELECT ROW_NUMBER() OVER(), YTileId, XTileId,
      stboxX(lower(XSpan), lower(YSpan), upper(XSpan), upper(YSpan), SRID)
    FROM YTiles y, XTiles x, TableSRID s
    ORDER BY YTileId, XTileId )
  SELECT TileId, RowNo, ColNo, Tile, Tile::geometry
  FROM Tiles;
