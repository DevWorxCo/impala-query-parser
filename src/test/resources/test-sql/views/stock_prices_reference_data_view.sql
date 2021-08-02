

DROP VIEW IF EXISTS ${DATABASE}.stock_prices_reference_data_view;

CREATE VIEW ${DATABASE}.stock_prices_reference_data_view AS

    SELECT  rf.internal_id,
            rf.ticker,
            rf.name,
            rf.description,
            rf.included,
            rf.creation_date,

            sp.price,
            rank() over(order by sp.price desc) as price_rank,
            avg(sp.price) as avg_price,
            sp.observation

    FROM ${DATABASE}.stock_prices sp
    INNER JOIN ${DATABASE}.stock_reference_data rf
    ON sp.internal_id = rf.internal_id
;

refresh ${DATABASE}.stock_prices_reference_data_view;


