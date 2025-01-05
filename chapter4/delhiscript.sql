DO $$
DECLARE
  -- Variables for loading the CSV files
  filePath text= '/home/esteban/src/MobilityDataScienceBook/chapter4/delhi/';
  fileName text;
  fileNames text[];
  -- Variables for creating the trips
  device CURSOR FOR 
    SELECT DISTINCT deviceID FROM delhiInput;
  devicetraj CURSOR(devId text) FOR 
    SELECT t, lat, lon, pm1_0, pm2_5, pm10
    FROM delhiInput 
    WHERE deviceID = devId
    ORDER BY t;
  devId text;
  myid int;
  rec record;
  init timestamp = null;
  cur timestamp;
  prev timestamp;
  threshold interval = interval '30 minutes';
BEGIN
  CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

  /* Load the CSV tiles */
  DROP TABLE IF EXISTS delhiInput;
  CREATE TABLE delhiInput(
    id int NOT NULL,
    uid text PRIMARY KEY,
    t timestamp NOT NULL,
    deviceId text NOT NULL,
    lat double precision NOT NULL,
    lon double precision NOT NULL,
    pm1_0 double precision NOT NULL,
    pm2_5 double precision NOT NULL,
    pm10 double precision NOT NULL,
    geom geometry,
    UNIQUE(deviceId, t)
  );
  WITH files(f) AS (
    SELECT pg_ls_dir(filePath) )
  SELECT array_agg(f ORDER BY f ASC) INTO fileNames FROM files;
  FOREACH fileName IN ARRAY fileNames
  LOOP
      EXECUTE format(
        'COPY delhiInput(id, uid, t, deviceId, lat, lon, pm1_0, pm2_5, pm10) '
      'FROM %L WITH DELIMITER '','' CSV HEADER', filePath || fileName);
    COMMIT;
    RAISE NOTICE 'inserting %', fileName;
  END LOOP;
  -- This is needed for visualization purposes
  RAISE NOTICE 'Updating the Geom attribute in table delhiInput';
  UPDATE delhiInput SET geom = ST_Transform(ST_Point(lon, lat, 4326), 7760);

  /* Create the trips */
  DROP TABLE IF EXISTS delhiTrips;
  CREATE TABLE delhiTrips(
    Id integer,
    deviceId text,
    trip tgeompoint,
    pm1_0 tfloat,
    pm2_5 tfloat,
    pm10 tfloat,
    trajectory geometry,
    PRIMARY KEY(deviceId, Id)
  );


  DROP TABLE IF EXISTS Trips5min;
  CREATE TABLE Trips5min(deviceId, trip, trajectory) AS
    WITH Trips(deviceId, trip) AS (
    SELECT deviceId, tgeompointSeqSetGaps(array_agg(tgeompoint(Geom, T) ORDER BY T),
      maxt := interval '5 minutes')
    FROM DelhiInput
    GROUP BY deviceId )
  SELECT deviceId, trip, trajectory(trip)
  FROM Trips;

  DROP TABLE IF EXISTS Trips10min;
  CREATE TABLE Trips10min(deviceId, trip, trajectory) AS
    WITH Trips(deviceId, trip) AS (
    SELECT deviceId, tgeompointSeqSetGaps(array_agg(tgeompoint(Geom, T) ORDER BY T),
      interval '10 minutes')
    FROM DelhiInput
    GROUP BY deviceId )
  SELECT deviceId, trip, trajectory(trip)
  FROM Trips;
  
  OPEN device;
  LOOP
    FETCH device INTO devId;	
    EXIT WHEN NOT FOUND;
    RAISE NOTICE 'processing %', devId;
    myid = 1;
    init = null;
	  prev = null;
    OPEN devicetraj(devId);
    LOOP
      FETCH devicetraj into rec;
      EXIT WHEN NOT FOUND;
      IF (init IS NULL) THEN
        init = rec.t;
        prev = rec.t;
      END IF;
      cur = rec.t;
      IF (cur - prev > threshold) THEN
        INSERT INTO delhiTrips  
        SELECT myid, deviceId, 
          tgeompointSeq(array_agg(tgeompoint(geom, t) ORDER BY t)),
          tfloatSeq(array_agg(tfloat(pm1_0, t) ORDER BY t)),
          tfloatSeq(array_agg(tfloat(pm2_5, t) ORDER BY t)),
          tfloatSeq(array_agg(tfloat(pm10, t) ORDER BY t))
        FROM delhiInput
        WHERE deviceId = devId AND t >= init AND
		      t < cur  -- cur not included
        GROUP BY deviceId;

        myid = myid + 1;
        init = cur;
      END IF;
	    prev = cur;
	  END LOOP;
	   
    -- the last buffer 
	  IF (prev IS NOT NULL) THEN
	    INSERT INTO delhiTrips   
      SELECT myid, deviceId, 
        tgeompointSeq(array_agg(tgeompoint(
          ST_Transform(ST_Point(lon, lat, 4326), 7760), t) ORDER BY t)),
        tfloatSeq(array_agg(tfloat(pm1_0, t) ORDER BY t)),
        tfloatSeq(array_agg(tfloat(pm2_5, t) ORDER BY t)),
        tfloatSeq(array_agg(tfloat(pm10, t) ORDER BY t))
      FROM delhiInput 
      WHERE deviceId = devId AND t >= init AND
		     t <= cur  -- cur include 
      GROUP BY deviceId;
    END IF;
    CLOSE deviceTraj;
  END LOOP;
  CLOSE device;
  COMMIT;
  RAISE NOTICE 'finished';

  DELETE FROM delhiTrips WHERE duration(trip) < '10 minute';
  UPDATE delhiTrips SET trajectory = trajectory(trip);

  DROP INDEX IF EXISTS delhiTrips_trip_Idx;
  CREATE INDEX delhiTrips_trip_Idx ON delhiTrips USING GIST(Trip);
  DROP INDEX IF EXISTS delhiTrips_pm1_0_Idx;
  CREATE INDEX delhiTrips_pm1_0_Idx ON delhiTrips USING GIST(PM1_0);
  DROP INDEX IF EXISTS delhiTrips_pm10_Idx;
  CREATE INDEX delhiTrips_pm10_Idx ON delhiTrips USING GIST(PM10);
  DROP INDEX IF EXISTS delhiTrips_pm2_5_Idx;
  CREATE INDEX delhiTrips_pm2_5_Idx ON delhiTrips USING GIST(PM2_5);
END;
$$ LANGUAGE plpgsql;


