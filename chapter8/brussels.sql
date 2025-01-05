-------------------------------------------------------------------------------

CREATE TABLE IxellesMBR(Geo) AS
SELECT stbox(MunicipalityGeo)::geometry
FROM Municipalities
WHERE MunicipalityName LIKE 'Ixelles%';

-------------------------------------------------------------------------------
