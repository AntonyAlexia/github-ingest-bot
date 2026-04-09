CREATE OR REPLACE VIEW `market_raw.market_intelligence_gold` AS
SELECT 
    symbol, 
    date, 
    close,
    -- Calculate Daily Return % (Volatility)
    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / 
    LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS daily_return,
    -- Calculate 7-Day Moving Average
    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d
FROM `market_raw.daily_stock_data`;
