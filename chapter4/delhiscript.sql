CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

DROP TABLE IF EXISTS delhiInput;
CREATE TABLE delhiInput (
  id int NOT NULL,
  uid text NOT NULL,
  t timestamp NOT NULL,
  deviceid text NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  pm1_0 double precision NOT NULL,
  pm2_5 double precision NOT NULL,
  pm10 double precision NOT NULL,
  geom geometry(Point, 4326),
  PRIMARY KEY(uid),
  UNIQUE(deviceid, t)
);

DO $$
DECLARE
  prefixpath text = '/tmp/mobility/data/delhi';
  fileName text;
  fileNames text[];
BEGIN

  WITH dirs AS
  (
    SELECT prefixpath || '/2020/' || pg_ls_dir(prefixpath || '/2020'::text) dir
    UNION
    SELECT prefixpath || '/2021/' || pg_ls_dir(prefixpath || '/2021'::text) dir
  )
  SELECT array_agg(dir ORDER BY dir ASC) INTO fileNames FROM dirs;

  FOREACH fileName IN ARRAY fileNames
  LOOP
    EXECUTE format(
      'COPY delhiInput(id, uid, t, deviceid, lat, lon, pm1_0, pm2_5, pm10) '
      'FROM %L WITH DELIMITER '','' CSV HEADER', fileName);
    COMMIT;
    RAISE NOTICE 'Inserting %', fileName;
    END LOOP;
  END $$;

  UPDATE delhiInput SET geom = ST_Point(lon, lat, 4326);

  ALTER TABLE delhiInput ADD COLUMN tripInst tgeompoint;
  ALTER TABLE delhiInput ADD COLUMN pm1_0_inst tfloat;
  ALTER TABLE delhiInput ADD COLUMN pm2_5_inst tfloat;
  ALTER TABLE delhiInput ADD COLUMN pm10_inst tfloat;

  UPDATE delhiInput SET tripInst = tgeompoint(st_transform(geom,7760), t), 
    pm1_0_inst = tfloat(pm1_0, t), 
    pm2_5_inst = tfloat(pm2_5, t), pm10_inst = tfloat(pm10, t);

  DROP TABLE IF EXISTS delhiTrips;
  CREATE TABLE delhiTrips (
    Id integer,
    deviceid text,
    trip tgeompoint,
    pm1_0 tfloat,
    pm2_5 tfloat,
    pm10 tfloat,
    trajectory geometry,
    PRIMARY KEY(deviceid, Id)
  );

  DO $$
  DECLARE  
    device CURSOR FOR SELECT DISTINCT deviceID FROM delhiInput;
    devicetraj CURSOR (devId text) FOR 
      SELECT t, pm1_0_inst, pm2_5_inst, pm10_inst, tripInst
      FROM delhiinput 
      WHERE deviceID = devId
      ORDER BY t;

    devId text;
    id int;
    rec record;
    init timestamp = NULL;
    cur timestamp;
    prev timestamp;
    threshold interval = interval '30 minutes';
  BEGIN   
  OPEN device;
  LOOP
    FETCH device INTO devId;	
    EXIT WHEN NOT FOUND;
    RAISE NOTICE 'processing %', devId;
    id = 1;
    init = NULL;
    prev = NULL;
    OPEN devicetraj(devId);
    LOOP
      FETCH devicetraj INTO rec;
      EXIT WHEN NOT FOUND;
      IF (init IS NULL) THEN
        init = rec.t;
        prev = rec.t;
      END IF;
      cur = rec.t;
      IF (cur - prev > threshold) THEN
        INSERT INTO delhiTrips  
        SELECT id, deviceId, 
          tgeompointseq(array_agg(tripInst ORDER BY tripInst)),
          tfloatseq(array_agg(pm1_0_inst ORDER BY pm1_0_inst)),
          tfloatseq(array_agg(pm2_5_inst ORDER BY pm2_5_inst)),
          tfloatseq(array_agg(pm10_inst ORDER BY pm10_inst))
        FROM delhiInput
        WHERE deviceId= mydeviceid AND t >= init AND 
          t < cur  -- current not included
        GROUP BY deviceId;
        id = id + 1;
        init = cur;
      END IF;
      prev = cur;
    END LOOP;
       
    -- the last buffer 
    IF (prev IS NOT NULL) THEN
      INSERT INTO delhiTrips   
      SELECT id, deviceId, 
        tgeompointseq(array_agg(tripInst ORDER BY tripInst)),
        tfloatseq(array_agg(pm1_0_inst ORDER BY pm1_0_inst)),
        tfloatseq(array_agg(pm2_5_inst ORDER BY pm2_5_inst)),
        tfloatseq(array_agg(pm10_inst ORDER BY pm10_inst))
      FROM delhiInput 
      WHERE deviceId= mydeviceid AND t >= init AND 
        t <= cur  -- current include 
      GROUP BY deviceId;
    END IF;
    CLOSE deviceTraj;
  END LOOP;
  CLOSE device;
  COMMIT;
  RAISE NOTICE 'finished';
END;
$$ LANGUAGE plpgsql;

DELETE FROM delhiTrips WHERE duration(trip) < '10 minute';
UPDATE delhiTrips SET trajectory = trajectory(trip);

CREATE INDEX delhitrips_trip_Idx ON delhiTrips USING SPGist(Trip);
CREATE INDEX delhitrips_pm1_0_Idx ON delhiTrips USING Gist(PM1_0);
CREATE INDEX delhitrips_pm10_Idx ON delhiTrips USING Gist(PM10);
CREATE INDEX delhitrips_pm2_5_Idx ON delhiTrips USING Gist(PM2_5);


