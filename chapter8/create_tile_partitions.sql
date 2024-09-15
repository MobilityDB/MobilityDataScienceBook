DROP FUNCTION IF EXISTS create_tile_partitions;
CREATE OR REPLACE FUNCTION create_tile_partitions(table_name text,
  grid_table text) 
RETURNS text AS $$
DECLARE
  i integer;
  j integer;
  noTiles integer;
  tile_name text;
  sql_str text;
BEGIN
  sql_str = 'SELECT COUNT(*)  FROM ' || grid_table || ';';
  EXECUTE sql_str INTO noTiles;
  -- Drop the partitioned table if exists
  sql_str = 'DROP TABLE IF EXISTS ' || table_name || '_tile;';
  EXECUTE sql_str;
  -- Create the partitioned table
  sql_str = 'CREATE TABLE ' || table_name || '_tile (
      tripId int, vehId int, day date, tileId int,
      seqNo int, sourceNode bigint, targetNode bigint, 
      trip tgeompoint, trajectory geometry,
      UNIQUE (vehId, day, tileId, seqNo)
    ) PARTITION BY LIST(tileId);';
  EXECUTE sql_str;
  -- Loop for each tile
  FOR i IN 1..noTiles
  LOOP
    tile_name = table_name || '_tile_t' || i;
    -- Create the table for the partition
    RAISE INFO 'Creating table %', tile_name;
    sql_str = 'CREATE TABLE ' || table_name || '_tile_t' || i ||
      ' PARTITION OF ' || table_name || '_tile FOR VALUES IN (' || i || ');';
    EXECUTE sql_str;
    -- Fill the table for the partition
    sql_str = 'INSERT INTO ' || table_name || '_tile_t' || i || ' ' ||
      '(tripId, vehId, day, tileId, seqNo, sourceNode, targetNode, trip)' 
      'SELECT tripId, vehId, day, tileId, seqNo, sourceNode, targetNode,
        atStbox(trip, tile)
       FROM ' || table_name || ', ' || grid_table || ' 
       WHERE tileId = ' || i || ' AND atStbox(trip, tile) IS NOT NULL;';
    EXECUTE sql_str;
    -- Add the trajectory to the tile tables
    sql_str = 'UPDATE ' || table_name || '_tile_t' || i ||
      ' SET trajectory = trajectory(trip);';
    EXECUTE sql_str;
  END LOOP;
  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';
