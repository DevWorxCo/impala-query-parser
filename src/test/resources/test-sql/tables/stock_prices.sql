
-- But there could be some apostrophe here - doesn't the make it harder to parse !!

DROP TABLE IF EXISTS ${DATABASE}.stock_prices;

-- Some stock Prices for this firm's data
-- items
CREATE TABLE ${DATABASE}.stock_prices
(
    internal_id                 STRING -- the internal id's comments; and other items.
        COMMENT 'The stock internal id',
    price                       DOUBLE,
    observation                 TIMESTAMP
)
    STORED AS PARQUET tblproperties ("parquet.compression"="SNAPPY");

compute incremental stats ${DATABASE}.stock_reference_data;

refresh ${DATABASE}.stock_reference_data
;

