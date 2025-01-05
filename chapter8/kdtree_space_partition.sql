/*****************************************************************************
 * SQL script to partition one day of AIS data using space-only
 * multidimensional partitioning on the trip. Therefore, we need to find
 * pivot values of length that enable a balanced partitioning.
 * Currently the function does not take into account the Z dimension
 *****************************************************************************/

/*****************************************************************************
 * Find the pivot values to partition a table with respect to a given spatial
 * dimension (X or Y) where the number of instants in the partitions is
 * balanced up to an epsilon value which is by default set to 10%. 
 * The maximum number of iterations to find a pivot value is by default 30.
 * Currently the function does not take into account the Z dimension
 *****************************************************************************/

DROP FUNCTION IF EXISTS find_kdpivot_space;
CREATE FUNCTION find_kdpivot_space(dim int, box stbox, noTiles int)
RETURNS float AS $$
DECLARE
  debug bool = false;
  SRID int;
  xMin float;
  yMin float;
  xMax float;
  yMax float;
  minValue float;
  maxValue float;
  otherMinValue float;
  otherMaxValue float;
  res float;
BEGIN
  IF debug THEN
    -- Show the arguments received
    RAISE INFO '---------- Arguments received ----------';
    RAISE INFO 'dim := %, noTiles := %', dim, noTiles;
    RAISE INFO 'box := %', box;
    RAISE INFO '----------------------------------------';
  END IF;

  --------------------
  -- Initialization --
  --------------------

  -- Compute the minimum/maximum values for dimensions X/Y and the SRID
  SELECT xMin(box), yMin(box), xMax(box), yMax(box), SRID(box)
  INTO xMin, yMin, xMax, yMax, SRID;
  -- Set the variables according to the dimension for which the pivot is computed
  IF dim = 1 THEN
    minValue = xMin;
    maxValue = xMax;
    otherMinValue = yMin;
    otherMaxValue = yMax;
  ELSE
    minValue = yMin;
    maxValue = yMax;
    otherMinValue = xMin;
    otherMaxValue = xMax;
  END IF;
  
  ---------------------
  -- Get pivot value --
  ---------------------

  IF dim = 1 THEN
    WITH Instants(Inst) AS (
      SELECT unnest(instants(atStbox(Trip, box)))
      FROM Trips ),
    PointsBox(Point) AS (
      SELECT getValue(Inst)
      FROM Instants ),
    Tiles1(TileId, X) AS (
      SELECT NTILE(noTiles) OVER(ORDER BY ST_X(Point)), ST_X(Point)
      FROM PointsBox ),
    Tiles2(TileId, X, RowNo) AS (
      SELECT TileId, X,
        ROW_NUMBER() OVER (PARTITION BY TileId ORDER BY X)
      FROM Tiles1 )
    SELECT X INTO res
    FROM Tiles2
    WHERE RowNo = 1 AND TileId = 2;
  ELSE
    WITH Instants(Inst) AS (
      SELECT unnest(instants(atStbox(Trip, box)))
      FROM Trips ),
    PointsBox(Point) AS (
      SELECT getValue(Inst)
      FROM Instants ),
    Tiles1(TileId, Y) AS (
      SELECT NTILE(noTiles) OVER(ORDER BY ST_Y(Point)), ST_Y(Point)
      FROM PointsBox ),
    Tiles2(TileId, Y, RowNo) AS (
      SELECT TileId, Y,
        ROW_NUMBER() OVER (PARTITION BY TileId ORDER BY Y)
      FROM Tiles1 )
    SELECT Y INTO res
    FROM Tiles2
    WHERE RowNo = 1 AND TileId = 2;
  END IF;

  -- The END
  RETURN res;
END;
$$ LANGUAGE 'plpgsql';

DROP FUNCTION IF EXISTS kdtree_space_partition;
CREATE FUNCTION kdtree_space_partition(noLevels int DEFAULT 1)
RETURNS text AS $$
DECLARE
  xpivot float;
  yPivot float;
  i int;
  k int;
  SRID int;
  numPart int;
  totalBox stbox;
  box stbox;
  tiles stbox[];
  xMin float;
  yMin float;
  xMax float;
  yMax float;
  xMinBound float;
  yMinBound float;
  targetFactor float;
  tableName text;
BEGIN
  -- Compute the extent, the minimum/maximum of the other dimension and the
  -- SRID of the dataset
  SELECT getSpace(extent(stbox(trip))) INTO totalBox
  FROM Trips;
  SELECT xMin(totalBox), yMin(totalBox), xMax(totalBox), yMax(totalBox),
    SRID(totalBox) INTO xMin, yMin, xMax, yMax, SRID;

  xMinBound = xMin;
  yMinBound = yMin;
  -- One additional partition added here to be removed in the loop
  numPart = 2 * noLevels + 1;
  k = 1;
  -- Loop for the number of levels
  FOR i IN 1..noLevels LOOP
    RAISE INFO '============================';
    RAISE INFO ' Level %', i;
    RAISE INFO '----------';
    numPart = numPart - 1;
    -- Compute the bounding box and the pivot for the X dimension
    box = stboxX(xMinBound, yMinBound, xMax, yMax, SRID);
    SELECT find_kdpivot_space(1, box, numPart) INTO xPivot;
    RAISE INFO 'Pivot X: %', xPivot;
    tiles[k] = stboxX(xMinBound, yMinBound, xPivot, yMax, SRID);
    k := k + 1;
    xMinBound = xPivot;
    -- Do not partition the Y dimension for the last Level
    IF i = noLevels THEN
      EXIT;
    END IF;
    -- Compute the pivot and the bounding box for the Y dimension
    box = stboxX(xMinBound, yMinBound, xMax, yMax, SRID);
    numPart = numPart - 1;
    SELECT find_kdpivot_space(2, box, numPart) INTO yPivot;
    RAISE INFO 'Pivot Y: %', yPivot;
    tiles[k] = stboxX(xMinBound, yMinBound, xMax, yPivot, SRID);
    k := k + 1;
    yMinBound = yPivot;
  END LOOP;
  -- Compute the last tile
  tiles[k] = stboxX(xMinBound, yMinBound, xMax, yMax, SRID);
  -- Create the tiles table
  RAISE INFO '============================';
  RAISE INFO 'Create the tiles table ...';
  tableName = 'KdTiles_' || noLevels;
  EXECUTE 'DROP TABLE IF EXISTS ' || tableName || ';';
  EXECUTE 'CREATE TABLE ' || tableName || 
    '(TileId int, Tile stbox, geom geometry);';
  FOR i IN 1..array_length(tiles, 1) LOOP
    EXECUTE 'INSERT INTO ' || tableName || 
      ' VALUES (i, tiles[i], ST_Boundary(tiles[i]::geometry));';
  END LOOP;
  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql' STRICT;
