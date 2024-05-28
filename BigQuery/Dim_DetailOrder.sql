CREATE OR REPLACE TABLE 
  `seventh-jet-424513-h5.OLAP.Dim_DetailOrder` AS
SELECT
soh.salesorderid,
sod.salesorderdetailid,
soh.onlineorderflag,
sod.productid,
p.name as productname,
p.standardcost,
sod.orderqty,
sod.unitprice,
sod.unitpricediscount,
sod.linetotal,
p.productsubcategoryid,
ps.name as productsubcategoryname,
pc.productcategoryid,
pc.name as producategoryname

FROM `seventh-jet-424513-h5.STAGING.SalesOrderHeader` as soh
left join `seventh-jet-424513-h5.STAGING.SalesOrderDetail` as sod
on soh.salesorderid = sod.salesorderid
left join `seventh-jet-424513-h5.STAGING.Product` as p 
on p.productid = sod.productid
left join `seventh-jet-424513-h5.STAGING.ProductSubcategory` as ps
on p.productsubcategoryid = ps.productsubcategoryid
left join `seventh-jet-424513-h5.STAGING.ProductCategory` as pc
on ps.productcategoryid = pc.productcategoryid