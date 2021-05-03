

DROP VIEW IF EXISTS stock_prices_reference_data_view;

CREATE VIEW stock_prices_reference_data_view AS

    SELECT  rf.internal_id,
            rf.ticker,
            rf.name,
            rf.description,
            rf.included,
            rf.creation_date,

            sp.price,
            sp.observation

    FROM stock_prices sp
    INNER JOIN stock_reference_data rf
    ON sp.internal_id = rf.internal_id
;

refresh stock_prices_reference_data_view;


