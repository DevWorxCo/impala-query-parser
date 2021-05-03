

DROP TABLE IF EXISTS stock_prices;

-- Some stock Prices
--
CREATE TABLE stock_prices
(
    internal_id                 STRING COMMENT 'The stock internal id',
    price                       DOUBLE,
    observation                 TIMESTAMP
)
    STORED AS PARQUET tblproperties ("parquet.compression"="SNAPPY");

compute incremental stats stock_reference_data;

refresh stock_reference_data
;

