CREATE OR REPLACE TABLE 
  `seventh-jet-424513-h5.OLAP.Fact` AS
SELECT 
  soh.salesorderid,
  soh.orderdate,
  soh.customerid,
  soh.shiptoaddressid,
  soh.currencyrateid,
  soh.subtotal,
  soh.taxamt,
  soh.freight,
  soh.totaldue,
  IFNULL(soh.totaldue / dc.averagerate, soh.totaldue) as totalordertoUSD,
  sohsr.salesreasonid
FROM 
  `seventh-jet-424513-h5.STAGING.SalesOrderHeader` as soh
LEFT JOIN 
  `seventh-jet-424513-h5.STAGING.SalesOrderHeaderSalesReason` as sohsr
  ON soh.salesorderid = sohsr.salesorderid
LEFT JOIN 
  `seventh-jet-424513-h5.OLAP.Dim_Currency` as dc
  ON soh.currencyrateid = dc.currencyrateid;