--  Tạo bảng Dim_Date
CREATE OR REPLACE TABLE `seventh-jet-424513-h5.OLAP.Dim_Date` AS
SELECT
  date AS Date,
  EXTRACT(YEAR FROM date) AS Year,
  EXTRACT(MONTH FROM date) AS Month,
  EXTRACT(DAY FROM date) AS Day,
  FORMAT_DATE('%A', date) AS DayName,
  EXTRACT(QUARTER FROM date) AS Quarter
FROM UNNEST(GENERATE_DATE_ARRAY(
  (SELECT MIN(CAST(orderdate AS DATE)) FROM `seventh-jet-424513-h5.STAGING.SalesOrderHeader`),  -- Cast OrderDate to DATE
  (SELECT MAX(CAST(orderdate AS DATE)) FROM `seventh-jet-424513-h5.STAGING.SalesOrderHeader`),  -- Cast OrderDate to DATE
  INTERVAL 1 DAY
)) AS date;

ALTER TABLE `OLAP.Dim_Date`
ADD COLUMN IsWeekend BOOL;
UPDATE `OLAP.Dim_Date`
SET IsWeekend = 
  CASE WHEN EXTRACT(DAYOFWEEK FROM Date) IN (1, 7) THEN TRUE ELSE FALSE END
WHERE true;
