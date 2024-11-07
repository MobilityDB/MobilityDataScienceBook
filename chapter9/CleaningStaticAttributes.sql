-- Section 9.2 AIS Data Cleaning
-- Loading
CREATE EXTENSION MobilityDB CASCADE;

--loading data (similar to CH5)
DROP TABLE AISInput;
CREATE TABLE AISInput(
T timestamp,
TypeOfMobile varchar(50),
MMSI integer,
Latitude float,
Longitude float,
NavigationalStatus varchar(60),
ROT float,
SOG float,
COG float,
Heading integer,
IMO varchar(50),
CallSign varchar(50),
Name varchar(100),
ShipType varchar(50),
CargoType varchar(100),
Width float, Length float,
TypeOfPositionFixingDevice varchar(50),
Draught float,
Destination varchar(50),
ETA varchar(50),
DataSourceType varchar(50),
SizeA float,
SizeB float,
SizeC float,
SizeD float,
Geom geometry(Point, 4326)
);

SET TimeZone = 'UTC';
SET DateStyle = 'ISO, DMY';

COPY AISInput(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus,
  ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length,
  TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType,
  SizeA, SizeB, SizeC, SizeD)
FROM '/home/mahmoud/Desktop/MobilityDataScience/Book/my_notebooks/aisdk-2024-03-01.csv' DELIMITER  ',' CSV HEADER;

-- Initial filtering and transformation:
UPDATE AISInput
SET Latitude= NULL, Longitude= NULL
WHERE Longitude not between -180 and 180 OR Latitude not between -90 and 90;
-- 192,330 rows affected in 4 s 302 ms

UPDATE AISInput SET
   Geom = ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326);
-- 15,512,927 rows affected in 58 s 703 ms


ALTER TABLE AISInput
  ADD COLUMN GeomProj geometry(Point, 25832);

UPDATE AISInput SET
  GeomProj = ST_Transform(Geom, 25832);
-- 15,512,927 rows affected in 1 m 5 s 145 ms

-- Take a sample: For big datasets, one would need to take a manageable sample for exploring errors and building the
-- cleaning pipleline. In this example, we shall focus on the morning hours between 9h00 and 10h59.
DROP TABLE IF EXISTS AISInputSample;
CREATE TABLE AISInputSample AS
    SELECT *
    FROM AISInput
    WHERE EXTRACT(HOUR FROM T) BETWEEN 9 AND 10;
-- 1,334,271 rows affected in 9 s 495 ms


-- Missing data
---- Explore
---- NULL should be called NULL

SELECT
  COUNT(*) AS total_rows,
  COUNT(T) AS T_non_null,
  COUNT(TypeOfMobile) AS TypeOfMobile_non_null,
  COUNT(MMSI) AS MMSI_non_null,
  COUNT(Latitude) AS Latitude_non_null,
  COUNT(Longitude) AS Longitude_non_null,
  COUNT(NavigationalStatus) AS NavigationalStatus_non_null,
  COUNT(ROT) AS ROT_non_null,
  COUNT(SOG) AS SOG_non_null,
  COUNT(COG) AS COG_non_null,
  COUNT(Heading) AS Heading_non_null,
  COUNT(IMO) AS IMO_non_null,
  COUNT(CallSign) AS CallSign_non_null,
  COUNT(Name) AS Name_non_null,
  COUNT(ShipType) AS ShipType_non_null,
  COUNT(CargoType) AS CargoType_non_null,
  COUNT(Width) AS Width_non_null,
  COUNT(Length) AS Length_non_null,
  COUNT(TypeOfPositionFixingDevice) AS TypeOfPositionFixingDevice_non_null,
  COUNT(Draught) AS Draught_non_null,
  COUNT(Destination) AS Destination_non_null,
  COUNT(ETA) AS ETA_non_null,
  COUNT(DataSourceType) AS DataSourceType_non_null,
  COUNT(SizeA) AS SizeA_non_null,
  COUNT(SizeB) AS SizeB_non_null,
  COUNT(SizeC) AS SizeC_non_null,
  COUNT(SizeD) AS SizeD_non_null
FROM AISInputSample;

--total_rows,t_non_null,typeofmobile_non_null,mmsi_non_null,latitude_non_null,longitude_non_null,navigationalstatus_non_null,rot_non_null,sog_non_null,cog_non_null,heading_non_null,imo_non_null,callsign_non_null,name_non_null,shiptype_non_null,cargotype_non_null,width_non_null,length_non_null,typeofpositionfixingdevice_non_null,draught_non_null,destination_non_null,eta_non_null,datasourcetype_non_null,sizea_non_null,sizeb_non_null,sizec_non_null,sized_non_null
--1334271,1334271,1334271,1334271,1319025,1319025,1334271,885998,1216567,1145613,1035586,1334271,1334271,1225256,1334271,199278,1199709,1199751,1334271,993618,1334271,852560,1334271,1196868,1186757,1186085,1187354

-- The queries above assumed that NULL values are encoded as NULL in the data. Sometimes this is not the case. Other
-- placeholders can be used in the data to convey NULL. To identify what placeholders (like "unknown" or "unknown_value")
-- are used in place of NULL values across different columns in your database, you generally have a few strategies you
-- can employ. This requires an exploratory approach, involving you as human in the loop, to inspect the data and
-- determine the nature of these placeholders.
-- 1. SQL Queries to Detect Common Placeholders
-- You can run a set of SQL queries to quickly identify common placeholders such as "unknown", "n/a", "none", or
-- "not available", as follows:

SELECT DISTINCT Destination AS UniqueValues
FROM AISInputSample
WHERE Destination ILIKE '%none%' OR Destination ILIKE '%n/a%' OR
  Destination ILIKE '%not available%' OR Destination ILIKE '%unknown%';
--N/A
--THYBOROEN/AGGER
--Unknown
--UNKNOWN



-- 2. Automated Script to Detect NULL Placeholders
-- When dealing with a large number of columns and a potential variety of unknown placeholders, you might write a more
-- automated SQL script that aggregates the most frequent values of the columns:
WITH ColsVals(ColumnName, ColumnValue) AS (
  SELECT 'TypeOfMobile', TypeOfMobile FROM AISInputSample UNION ALL
  SELECT 'NavigationalStatus', NavigationalStatus FROM AISInputSample UNION ALL
  SELECT 'IMO', IMO FROM AISInputSample UNION ALL
  SELECT 'CallSign', CallSign FROM AISInputSample UNION ALL
  SELECT 'Name', Name FROM AISInputSample UNION ALL
  SELECT 'ShipType', ShipType FROM AISInputSample UNION ALL
  SELECT 'CargoType', CargoType FROM AISInputSample UNION ALL
  SELECT 'TypeOfPositionFixingDevice', TypeOfPositionFixingDevice
  FROM AISInputSample UNION ALL
  SELECT 'Destination', Destination FROM AISInputSample UNION ALL
  SELECT 'ETA', ETA FROM AISInputSample UNION ALL
  SELECT 'DataSourceType', DataSourceType FROM AISInputSample )
SELECT ColumnName, ColumnValue, COUNT(*) AS Frequency
FROM ColsVals
WHERE ColumnValue IS NOT NULL
GROUP BY ColumnName, ColumnValue
ORDER BY Frequency DESC;

SELECT
  column_name,
  value,
  COUNT(*) AS frequency
FROM
  (SELECT 'TypeOfMobile' AS column_name, TypeOfMobile AS value FROM AISInputSample
   UNION ALL
   SELECT 'NavigationalStatus', NavigationalStatus FROM AISInputSample
   UNION ALL
   SELECT 'IMO', IMO FROM AISInputSample
   UNION ALL
   SELECT 'CallSign', CallSign FROM AISInputSample
   UNION ALL
   SELECT 'Name', Name FROM AISInputSample
   UNION ALL
   SELECT 'ShipType', ShipType FROM AISInputSample
   UNION ALL
   SELECT 'CargoType', CargoType FROM AISInputSample
   UNION ALL
   SELECT 'TypeOfPositionFixingDevice', TypeOfPositionFixingDevice FROM AISInputSample
   UNION ALL
   SELECT 'Destination', Destination FROM AISInputSample
   UNION ALL
   SELECT 'ETA', ETA FROM AISInputSample
   UNION ALL
   SELECT 'DataSourceType', DataSourceType FROM AISInputSample
  ) sub
WHERE value IS NOT NULL
GROUP BY column_name, value
ORDER BY frequency DESC
LIMIT 100;

-- 100 rows retrieved starting from 1 in 1 m 46 s 853 ms (execution: 1 m 46 s 836 ms, fetching: 17 ms)
-- This query helps you systematically uncover potential data quality issues by identifying frequent entries in your
-- dataset. The assumption here is that commonly recurring values, especially in textual columns, are often used as
-- placeholders for missing data. Clearly this is not always the case. For instance, your data might have columns that
-- have a small domain, e.g., the gear of a car is an enumeration 1-6 and the reverse gear. In such cases, normal values
-- may have higher frequencies over the null values, and the null values might be missed by this query.

-- You might have noticed that the query contains only the textual attributes. This includes all varchar and text columns.
-- This is because the UNION ALL statement requires the types of the unioined attribures to be the same. Numeric and
-- geometry columns are typically less likely to have such string-based placeholders but can be checked for non-standard
-- values if necessary. Therefore you may need to repeat the query again for the group of numerical attributes, and
-- possibly other gorups of compatible data types.

-- By scanning the results fo this query, we identify the following placeholders:
| column\_name | value | frequency |
| :--- | :--- | :--- |
| IMO | Unknown | 555622 |
| Destination | Unknown | 344362 |
| navigationalStatus | Unknown value | 211223 |
| ShipType | Undefined | 136437 |

-- Depending on the dataset, you need to ensure that any identified placeholders like 'unknown' are indeed meant to be
-- interpreted as NULL. Sometimes, terms like 'unknown' might be legitimate data points depending on the context. In the
-- results of the above query, there were other less clear situations, that can raise argument whether or not they
-- should be replaced by NULL. For this example, we leave them unchanged.

| ShipType | Other | 62958 |
| ETA | 01/01/2025 00:00:00 | 23285 |
| CargoType | Reserved for future use | 21517 |
| navigationalStatus | Reserved for future amendment \[HSC\] | 15530 |

-- Based on the identified non-standard placeholders for NULL in your database from the provided table, you can write
-- SQL UPDATE statements to replace these placeholders with actual NULL values in each specified column of your AISInput
-- table, as follows:

UPDATE AISInputSample
SET
  IMO = CASE WHEN IMO = 'Unknown' THEN NULL ELSE IMO END,
  Destination = CASE WHEN Destination = 'Unknown' THEN NULL ELSE
    Destination END,
  NavigationalStatus = CASE WHEN NavigationalStatus = 'Unknown value' THEN
    NULL ELSE NavigationalStatus END,
  ShipType = CASE WHEN ShipType = 'Undefined' THEN NULL ELSE ShipType END,
  CargoType = CASE WHEN CargoType = 'No additional information' THEN NULL
    ELSE CargoType END,
  CallSign = CASE WHEN CallSign = 'Unknown' THEN NULL ELSE CallSign END
WHERE IMO = 'Unknown' OR Destination = 'Unknown' OR
  NavigationalStatus = 'Unknown value' OR ShipType = 'Undefined' OR
  CargoType = 'No additional information' OR CallSign = 'Unknown';
-- 735,725 rows affected in 2 s 276 ms

-- Now recalculate the NULL statistics as in the beginning of this section, we clearly get bigger number of NULL values,
-- which more accurately reflect the status of the data.

--total_rows,t_non_null,typeofmobile_non_null,mmsi_non_null,latitude_non_null,longitude_non_null,navigationalstatus_non_null,rot_non_null,sog_non_null,cog_non_null,heading_non_null,imo_non_null,callsign_non_null,name_non_null,shiptype_non_null,cargotype_non_null,width_non_null,length_non_null,typeofpositionfixingdevice_non_null,draught_non_null,destination_non_null,eta_non_null,datasourcetype_non_null,sizea_non_null,sizeb_non_null,sizec_non_null,sized_non_null
--1334271,1334271,1334271,1334271,1319025,1319025,1123048,885998,1216567,1145613,1035586,778649,1212419,1225256,1197834,65865,1199709,1199751,1334271,993618,989909,852560,1334271,1196868,1186757,1186085,1187354


-- Consistency of ship data
-- This dataset has two keys that identify the ship:  MMSI (Maritime Mobile Service Identity) and IMO (International
-- Maritime Organization number). We therefore need to verify a one-to-one MMSI to IMO correspondence i.e., each MMSI
-- should map to exactly one IMO and vice versa. Here’s an SQL script to validate this relationship:

SELECT MMSI, COUNT(DISTINCT IMO) AS unique_imo_count
FROM AISInputSample
--WHERE IMO IS NOT NULL AND IMO NOT IN ('Unknown', 'Undefined')  -- Exclude known placeholders
GROUP BY MMSI
HAVING COUNT(DISTINCT IMO) > 1;
-- The result of this query is empty, which is good. Next we check for duplicates in the IMO numbers:

SELECT IMO, COUNT(DISTINCT MMSI) AS unique_mmsi_count
FROM AISInputSample
--WHERE IMO IS NOT NULL AND IMO NOT IN ('Unknown', 'Undefined')  -- Exclude known placeholders
GROUP BY IMO
HAVING COUNT(DISTINCT MMSI) <> 1;

--| imo | unique\_mmsi\_count |
--| :--- | :--- |
--| NULL | 1370 |

-- This query has identified that a NULL IMO has appeared in combination with 1370 different MMSI numbers.
-- We thus want to have a closer look at them to understand how to impute. To address this, we'll start by examining
-- records that do successfully pair these MMSI numbers with an IMO. If we can identify consistent pairings in the data,
-- we can leverage these associations to fill in the missing IMO values for records with the same MMSI. By implementing
-- this strategy, we aim to enhance the completeness of our dataset.

WITH IMOMapping AS (
    SELECT MMSI,
           MODE() WITHIN GROUP (ORDER BY IMO) AS MostFrequentImo -- Use statistical mode to find the most common IMO per MMSI
    FROM AISInputSample
    WHERE MMSI IN (SELECT DISTINCT MMSI FROM AISInputSample WHERE IMO IS NULL)
    GROUP BY MMSI
    --HAVING MODE() WITHIN GROUP (ORDER BY IMO) IS NOT NULL
)
UPDATE AISInputSample
SET
    IMO = MostFrequentImo
FROM
    ImoMapping
WHERE
    AISInputSample.MMSI = ImoMapping.MMSI
    AND AISInputSample.IMO IS NULL
    AND MostFrequentImo IS NOT NULL;
-- 1,620 rows affected in 903 ms


-- The Common Table Expression CTE IMOMapping establishes a mapping of the most frequently occurring IMO for each MMSI.
-- This CTE uses PostgreSQL's `MODE()` function in the `WITHIN GROUP (ORDER BY IMO)` clause, which computes the
-- statistical mode of IMO values for each group of records with the same MMSI. You are encouraged to try playing with
-- this CTE, since it can be useful in other different cituations. Also try uncommenting the HAVING clause. You will see
-- that the query finds only 35 such associations among the 1370 missing IMOs.
--
-- The second step of the query uses this CTE to update the `AISInputSample` table, setting the `IMO` field to the most
-- frequently occurring IMO wherever it is currently null, but only for those 35 records with MostFrequentImo is not NULL.
-- This approach ensures that all missing IMO values are systematically filled with the most plausible data, based on
-- observed patterns within the dataset. In this case, we could only impute a small number of missing IMOs, i.e., 35.
-- The remaining missing IMOs cannot be imputed using only the information that we have. Perhaps, if needed, they can be
-- queried in other external sources such as marinetraffic.com or other aggregators of maritime data.

-- The same cleaning of IMO can be repeated for other ship-related attributes such as the SizeA...SizeD. Since this data
-- is always the same for the same ship, their corresspondance with the MMSI key should be one-to-one.

-- Notice that one-to-one correspondance is one example of functional dependancy between attributes. Functional
-- dependency in tabular data refers to a relationship where the value of one set of columns determines the value of
-- another column, which can help data cleaning as we saw in the case of imputing missing IMO numbers. Automated tools
-- such as OpenRefine and Microsoft SQL Server Data Quality Services DQS can discover these dependencies and use them
-- for data cleaning tasks. These tools can automatically detect patterns or dependencies in the data, allow users to
-- define rules based on these dependencies, and perform cleansing operations such as normalizing data, correcting
-- anomalies, and filling missing values according to the observed functional dependencies.

SELECT MMSI, IMO
FROM AISInputSample
WHERE MMSI = 211291170;


-- Cleaning Voyage-Related Data

WITH DestinationMapping AS (
  SELECT MMSI,
    MODE() WITHIN GROUP (ORDER BY Destination) AS MostFrequentDestination
  FROM AISInputSample
  WHERE MMSI IN (
    SELECT DISTINCT MMSI
    FROM AISInputSample
    WHERE Destination IS NULL )
  GROUP BY MMSI )
UPDATE AISInputSample
SET Destination = MostFrequentDestination
FROM DestinationMapping
WHERE AISInputSample.MMSI = DestinationMapping.MMSI AND
  AISInputSample.Destination IS NULL AND MostFrequentDestination IS NOT NULL;

