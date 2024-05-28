CREATE OR REPLACE TABLE 
  `seventh-jet-424513-h5.OLAP.Dim_DeliveryAddress` AS
SELECT 
a.addressid,
a.addressline1,
a.addressline2,
a.city,
a.stateprovinceid,
sp.stateprovincecode,
sp.name as stateprovincename,
sp.countryregioncode,
cr.name as countryregionname,

FROM  `seventh-jet-424513-h5.STAGING.Address` as a
LEFT join `seventh-jet-424513-h5.STAGING.StateProvince` as sp
on a.stateprovinceid = sp.stateprovinceid
LEFT join `seventh-jet-424513-h5.STAGING.CountryRegion` as cr
on sp.countryregioncode = cr.countryregioncode