CREATE OR REPLACE TABLE 
  `seventh-jet-424513-h5.OLAP.Dim_SalesReason` AS
SELECT salesreasonid,
  name as salesreasonname,
  reasontype
FROM `seventh-jet-424513-h5.STAGING.SalesReason` 

