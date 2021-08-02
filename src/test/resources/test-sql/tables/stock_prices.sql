

DROP TABLE IF EXISTS ${DATABASE}.stock_prices;

-- Some stock Prices
--
CREATE TABLE ${DATABASE}.stock_prices
(
    internal_id                 STRING COMMENT 'The stock internal id',
    price                       DOUBLE,
    observation                 TIMESTAMP
)
    STORED AS PARQUET tblproperties ("parquet.compression"="SNAPPY");

compute incremental stats ${DATABASE}.stock_reference_data;

refresh ${DATABASE}.stock_reference_data
;

