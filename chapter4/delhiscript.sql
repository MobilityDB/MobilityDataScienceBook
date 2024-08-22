CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

DROP TABLE IF EXISTS delhiInput;
CREATE TABLE delhiInput
(
        id int not null,
        uid text not null,
        t timestamp not null,
        deviceid text not null,
        lat double precision not null,
        lon double precision not null,
        pm1_0 double precision not null,
        pm2_5 double precision not null,
        pm10 double precision not null,
        primary key(uid),
        unique(deviceid, t)
);

DO  $$
DECLARE
prefixpath text= '/tmp/mobility/data';
path text;
fileNames text[];
BEGIN

with dirs as
(
  SELECT prefixpath || '/2020/' || pg_ls_dir(prefixpath || '/2020'::text) dir
 UNION
 SELECT prefixpath || '/2021/' || pg_ls_dir(prefixpath || '/2021'::text) dir
)
SELECT array_agg(dir order by dir asc) INTO fileNames from dirs ;

 FOREACH path IN ARRAY fileNames
 LOOP
        EXECUTE format('COPY delhiInput(id, uid, t, deviceid, lat, lon, pm1_0, pm2_5, pm10) FROM %L WITH DELIMITER '','' CSV HEADER', path);
        COMMIT;
        Raise Notice 'inserting %', path;
 END LOOP;
END $$;


-- to mobilityDB
ALTER TABLE delhiInput ADD COLUMN trip_inst tgeompoint;
ALTER TABLE delhiInput ADD COLUMN pm1_0_inst tfloat;
ALTER TABLE delhiInput ADD COLUMN pm2_5_inst tfloat;
ALTER TABLE delhiInput ADD COLUMN pm10_inst tfloat;

UPDATE delhiInput SET trip_inst = tgeompoint(ST_MakePoint(lon, lat), t),  pm1_0_inst = tfloat(pm1_0, t), pm2_5_inst = tfloat(pm2_5, t), pm10_inst = tfloat(pm10, t);



DROP TABLE IF EXISTS delhiTrips;
CREATE TABLE delhiTrips (
  Id integer,
  deviceid text,
  trip tgeompoint,
  pm1_0 tfloat,
  pm2_5 tfloat,
  pm10 tfloat,
trajectory geometry,
 primary key(deviceid, Id)
);



DO  $$
DECLARE  
device CURSOR FOR SELECT  DISTINCT deviceID FROM delhiInput;
devicetraj CURSOR (key text) FOR 
SELECT t, pm1_0_inst, pm2_5_inst, pm10_inst, trip_inst
FROM delhiinput 
WHERE deviceID = key
ORDER BY t;

myDeviceId text;
myid int;
myrec record;
initial timestamp= null;
current timestamp;
prev timestamp;
threshold interval= interval '30' minute;
BEGIN   
OPEN device;
LOOP
       FETCH device INTO myDeviceId;	
      EXIT WHEN NOT FOUND;
      RAISE NOTICE 'processing %', myDeviceId;
       myid= 1;
      initial = null;
	  prev= null;
       OPEN devicetraj ( myDeviceId);
       LOOP
       FETCH devicetraj into myrec;
       EXIT WHEN NOT FOUND;
       IF (initial IS NULL) THEN
             initial= myrec.t;
             prev= myrec.t;
       END IF;
       Current = myrec.t;
       IF (current-prev > threshold) THEN
           INSERT INTO delhiTrips  
          SELECT myid, deviceId, 
		  tgeompointseq(array_agg(trip_inst ORDER BY trip_inst)),
          tfloatseq(array_agg(pm1_0_inst ORDER BY pm1_0_inst)),
          tfloatseq(array_agg(pm2_5_inst ORDER BY pm2_5_inst)),
          tfloatseq(array_agg(pm10_inst ORDER BY pm10_inst))
          FROM delhiInput
          WHERE deviceId= mydeviceid AND t >= initial 
		         AND t < current  -- current not included
           GROUP BY deviceId;

          myid= myid+1;
		  initial= current;
       END IF;
	   prev= current;

	   END LOOP;
	   
	   -- the last buffer 
	   IF (prev IS NOT NULL) THEN
	                INSERT INTO delhiTrips   
          SELECT myid, deviceId, 
		  tgeompointseq(array_agg(trip_inst ORDER BY trip_inst)),
          tfloatseq(array_agg(pm1_0_inst ORDER BY pm1_0_inst)),
          tfloatseq(array_agg(pm2_5_inst ORDER BY pm2_5_inst)),
          tfloatseq(array_agg(pm10_inst ORDER BY pm10_inst))
          FROM delhiInput
          WHERE deviceId= mydeviceid AND t >= initial 
		         AND t <= current  -- current include 
           GROUP BY deviceId;

	   END IF;
       CLOSE deviceTraj;
END LOOP;
CLOSE device;
COMMIT;
Update delhiTrips  set trip= setSRID( trip, 4326);
update delhiTrips set trajectory= trajectory(trip);
delete from delhiTrips where duration(trip) < '10 minute';

COMMIT;
RAISE NOTICE 'finished';
END;
$$ LANGUAGE plpgsql;



CREATE INDEX delhitrips_trip_Idx ON delhiTrips USING SPGist(Trip);
CREATE INDEX delhitrips_pm1_0_Idx ON delhiTrips USING Gist(PM1_0);
CREATE INDEX delhitrips_pm10_Idx ON delhiTrips USING Gist(PM10);
CREATE INDEX delhitrips_pm2_5_Idx ON delhiTrips USING Gist(PM2_5);


