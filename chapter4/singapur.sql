 select srid(trip) from trips limit 10
 
SELECT row_number() over() AS cid, traj 
FROM(
WITH Cities(Sydney, Auckland, Melbourne) AS 
(SELECT ST_Transform(ST_makeEnvelope(103.9,1.29, 104.02, 1.29,4326),3414),  ST_Transform(ST_makeEnvelope (174.0, -37.67,176.17, -36.12, 4326),4326),  ST_Transform(ST_makeEnvelope (143.947935, -38.869629, 146.320982, -37.089844, 4326),4326)), 

WITH Airport(geomairp) AS (	
SELECT  ST_Transform(ST_makeEnvelope(43448.255460 , 32100.164699, 50057.878379, 38324.866882,3414),4326)),
	
Downtown(geomdown) AS (SELECT  ST_Transform(ST_makeEnvelope(25766.253602 ,26853.438763,30962.684184 ,30800.601927,3414),4326))

WITH Places AS (
SELECT  ST_makeEnvelope(43448.255460 , 32100.164699, 50057.878379, 38324.866882,3414)  as airport,
 ST_makeEnvelope(25766.253602 ,26853.438763,30962.684184 ,30800.601927,3414) as downtown
)	
SELECT trj_id,traj
FROM Trips t, Places p
WHERE ST_Intersects(t.traj,p.airport) AND ST_Intersects(t.traj,p.downtown)

	
WITH Places AS (
SELECT  ST_makeEnvelope(43448.255460 , 32100.164699, 50057.878379, 38324.866882,3414)  as airport,
 ST_makeEnvelope(25766.253602 ,26853.438763,30962.684184 ,30800.601927,3414) as downtown
)
SELECT trj_id,astext(trip), traj, endValue(trip) 
FROM Trips t, Places p
WHERE ST_Intersects(t.traj,p.airport) AND ST_Intersects(t.traj,p.downtown)	
and ST_geometrytype(endValue(trip) )='ST_Point'
	
SELECT stops(tfloat '[1@2000-01-01, 1@2000-01-02, 2@2000-01-03]',0.0001,'1 second');
	
SELECT asText(stops(tgeompoint '[Point(1 1 1)@2000-01-01, Point(1 1 1)@2000-01-02,
Point(2 2 2)@2000-01-03, Point(2 2 2)@2000-01-04]', 1.75));	
	
	
WITH Places AS (
SELECT  ST_makeEnvelope(43448.255460 , 32100.164699, 50057.878379, 38324.866882,3414)  as airport,
 ST_makeEnvelope(25766.253602 ,26853.438763,30962.684184 ,30800.601927,3414) as downtown
)
SELECT trj_id,  traj, endValue(trip), startValue(trip) 
FROM Trips t, Places p
WHERE ST_Intersects(startValue(trip),p.downtown) AND ST_Intersects(t.traj,p.airport)	
and ST_geometrytype(endValue(trip) )='ST_Point'
	
	
WITH Places AS (
SELECT  ST_makeEnvelope(43448.255460 , 32100.164699, 50057.878379, 38324.866882,3414)  as airport,
 ST_makeEnvelope(25766.253602 ,26853.438763,30962.684184 ,30800.601927,3414) as downtown
)
SELECT trj_id,  traj, endValue(trip), startValue(trip) 
FROM Trips t, Places p
WHERE ST_Intersects(startValue(trip),p.airport) AND ST_Intersects(endValue(trip),p.downtown)	
and ST_geometrytype(endValue(trip) )='ST_Point'

WITH Places AS (
SELECT  ST_makeEnvelope(43448.255460 , 32100.164699, 50057.878379, 38324.866882,3414)  as airport,
 ST_makeEnvelope(25766.253602 ,26853.438763,30962.684184 ,30800.601927,3414) as downtown
),
Speed AS ( 
select trj_id,   speed(trip) as speed 
from trips),
Speeds as (SELECT trj_id, twavg(speed) as weightedAvgSpeed
FROM Speed),
Averages AS (SELECT  avg(weightedAvgSpeed) as prom, max(weightedAvgSpeed) as maxim, min(weightedAvgSpeed) as minim
from Speeds ),
Veloc AS(SELECT t.*, A.*
FROM Speeds t, Averages A )
SELECT t.trj_id,  t.traj, endValue(t.trip), startValue(t.trip), s.weightedAvgSpeed
FROM Trips t, Places p, Speeds s
WHERE ST_Intersects(startValue(trip),p.airport) AND ST_Intersects(endValue(trip),p.downtown)	
and ST_geometrytype(endValue(trip) )='ST_Point'
and s.trj_id = t.trj_id
