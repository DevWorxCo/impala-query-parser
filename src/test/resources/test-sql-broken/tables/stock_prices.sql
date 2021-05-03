

DROP TABLE IF EXISTS stock_prices;

CREATE TABLE stock_prices
(
    internal_id                 STRING,
    price                       DOUBLE,
    observation                 TIMESTAMP
)
    STORED AS PARQUET tblpro perties ("parquet.compression"="SNAPPY");

compute incremental stats stock_reference_data;

refresh stock_reference_data
;

