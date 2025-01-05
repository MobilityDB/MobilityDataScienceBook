DO $$
DECLARE
  filePath text = '/home/esteban/src/delhi/';
  fileName text;
  fileNames text[];
BEGIN
  CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

  DROP TABLE IF EXISTS delhiInput;
  CREATE TABLE delhiInput (
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

  SELECT array_agg(f ORDER BY f ASC) INTO fileNames 
  FROM pg_ls_dir(filePath) AS f;
  FOREACH fileName IN ARRAY fileNames
  LOOP
    EXECUTE format(
      'COPY delhiInput(id, uid, t, deviceId, lat, lon, pm1_0, pm2_5, pm10) '
      'FROM %L WITH DELIMITER '','' CSV HEADER', filePath || fileName);
    COMMIT;
    RAISE NOTICE 'Inserting %', fileName;
  END LOOP;

  UPDATE delhiInput 
  SET geom = ST_Transform(ST_Point(lon, lat, 4326), 7760);

  DROP TABLE IF EXISTS delhiTrips;
  CREATE TABLE delhiTrips(
    tripId integer PRIMARY KEY,
    deviceId text,
    trip tgeompoint,
    trajectory geometry,
    pm1_0 tfloat,
    pm2_5 tfloat,
    pm10 tfloat
  );

  INSERT INTO DelhiTrips(tripId, DeviceId, Trip, Trajectory, PM1_0, PM2_5, PM10)
  WITH DelhiStart AS (
    SELECT d.*,  
      CASE WHEN lag(t) OVER w IS NULL OR t - lag(t) OVER w > '3 minutes' OR 
        ST_Distance(geom, lag(geom) OVER w) > 1000 THEN 1
      END AS StartInst
    FROM DelhiInput d
    WINDOW w AS (PARTITION BY DeviceId ORDER BY T) ),
  DelhiGroup AS (
    SELECT d.*, COUNT(StartInst) OVER (ORDER BY DeviceId, T) AS tripId
    FROM DelhiStart d ),
  Trips(tripId, DeviceId, Trip, PM1_0, PM2_5, PM10) AS (
    SELECT tripId, DeviceId,
      tgeompointSeq(array_agg(tgeompoint(d.Geom, d.t) ORDER BY d.t)),
      tfloatSeq(array_agg(tfloat(d.PM1_0, d.t) ORDER BY d.t)),
      tfloatSeq(array_agg(tfloat(d.PM2_5, d.t) ORDER BY d.t)),
      tfloatSeq(array_agg(tfloat(d.PM10, d.t) ORDER BY d.t))
    FROM DelhiGroup d
    GROUP BY tripId, DeviceId )
  SELECT tripId, DeviceId, Trip, trajectory(Trip), PM1_0, PM2_5, PM10
  FROM Trips;

END $$;

