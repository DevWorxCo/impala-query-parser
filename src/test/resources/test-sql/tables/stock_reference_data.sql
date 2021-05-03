
DROP TABLE IF EXISTS stock_reference_data;

CREATE TABLE stock_reference_data
(
    internal_id                 STRING,
    ticker                      STRING,
    name                        STRING,
    description                 STRING,
    included                    BOOLEAN,
    creation_date               TIMESTAMP
)
STORED AS PARQUET tblproperties ("parquet.compression"="SNAPPY");

compute incremental stats stock_reference_data;

refresh stock_reference_data
;

