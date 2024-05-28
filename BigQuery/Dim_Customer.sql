CREATE OR REPLACE TABLE 
  `seventh-jet-424513-h5.OLAP.Dim_Customer` AS
SELECT 
c.customerid,
c.accountnumber
FROM `seventh-jet-424513-h5.STAGING.Customer` AS c
