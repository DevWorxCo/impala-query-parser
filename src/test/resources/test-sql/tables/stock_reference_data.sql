
DROP TABLE IF EXISTS ${DATABASE}.stock_reference_data;

CREATE TABLE ${DATABASE}.stock_reference_data
(
    internal_id                 STRING,
    ticker                      STRING,
    name                        STRING,
    description                 STRING,
    included                    BOOLEAN,
    creation_date               TIMESTAMP
)
STORED AS PARQUET tblproperties ("parquet.compression"="SNAPPY");

compute incremental stats ${DATABASE}.stock_reference_data;

refresh ${DATABASE}.stock_reference_data
;

