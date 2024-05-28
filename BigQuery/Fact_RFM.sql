ALTER TABLE `seventh-jet-424513-h5.OLAP.Fact`
ADD COLUMN IF NOT EXISTS recency_score INT64,
ADD COLUMN IF NOT EXISTS frequency_score INT64,
ADD COLUMN IF NOT EXISTS monetary_score INT64,
ADD COLUMN IF NOT EXISTS RFM_segment STRING;

CREATE OR REPLACE TABLE `seventh-jet-424513-h5.OLAP.LastOrderDate` AS
SELECT
  MAX(orderdate) AS last_order_date
FROM
  `seventh-jet-424513-h5.OLAP.Fact`;

CREATE OR REPLACE TABLE `seventh-jet-424513-h5.OLAP.Fact_RFM` AS
WITH last_date AS (
  SELECT last_order_date FROM `seventh-jet-424513-h5.OLAP.LastOrderDate`
),
rfm_values AS (
  SELECT
    customerid,
    MAX(orderdate) AS last_order_date,
    COUNT(customerid) AS frequency,
    SUM(totalordertoUSD) AS monetary
  FROM
    `seventh-jet-424513-h5.OLAP.Fact`
  GROUP BY
    customerid
),
rfm_scores AS (
  SELECT
    customerid,
    DATE_DIFF((SELECT last_order_date FROM last_date), last_order_date, DAY) AS recency,
    frequency,
    monetary
  FROM
    rfm_values
),
rfm_with_scores AS (
  SELECT
    customerid,
    recency,
    frequency,
    monetary,
    NTILE(4) OVER (ORDER BY recency) AS recency_score,
    NTILE(4) OVER (ORDER BY frequency DESC) AS frequency_score,
    NTILE(4) OVER (ORDER BY monetary DESC) AS monetary_score
  FROM
    rfm_scores
),
rfm_segmented AS (
  SELECT
    customerid,
    recency_score,
    frequency_score,
    monetary_score,
    CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)) AS RFM_score,
    CASE
      WHEN CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)) = '444' THEN 'Best Customer'
      WHEN CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)) = '111' THEN 'Churn Customer'
      WHEN RIGHT(CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)), 1) = '4' THEN 'Highest Paying Customers'
      WHEN SUBSTR(CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)), 2, 1) = '4' THEN 'Loyal Customer'
      WHEN LEFT(CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)), 2) = '41' THEN 'Newest Customers'
      WHEN LEFT(CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)), 2) = '11' THEN 'Once Loyal, Now Gone'
      ELSE 'Normal'
    END AS RFM_segment
  FROM
    rfm_with_scores
)
SELECT
  customerid,
  recency_score,
  frequency_score,
  monetary_score,
  RFM_segment
FROM
  rfm_segmented;

MERGE `seventh-jet-424513-h5.OLAP.Fact` T
USING `seventh-jet-424513-h5.OLAP.Fact_RFM` S
ON T.customerid = S.customerid
WHEN MATCHED THEN
  UPDATE SET
    T.recency_score = S.recency_score,
    T.frequency_score = S.frequency_score,
    T.monetary_score = S.monetary_score,
    T.RFM_segment = S.RFM_segment;
