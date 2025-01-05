CREATE TABLE Heatmap_csv(id bigint PRIMARY KEY, geom geometry, nodeliveries int);
COPY Heatmap_csv(id, geom, nodeliveries)
FROM '/home/esteban/src/MobilityDataScienceBook/chapter7/heatmap.csv' DELIMITER ',' CSV HEADER;