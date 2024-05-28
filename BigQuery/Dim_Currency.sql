CREATE OR REPLACE TABLE 
  `seventh-jet-424513-h5.OLAP.Dim_Currency` AS
SELECT 
cr.currencyrateid,
cr.currencyratedate,
cr.fromcurrencycode,
c1.name as fromcurrencyname,
cr.tocurrencycode,
c2.name as tocurrencyname,
cr.averagerate
FROM `seventh-jet-424513-h5.STAGING.CurrencyRate` as cr
left join `seventh-jet-424513-h5.STAGING.Currency` as c1
on c1.currencycode = cr.fromcurrencycode
left join `seventh-jet-424513-h5.STAGING.Currency` as c2
on c2.currencycode = cr.tocurrencycode
