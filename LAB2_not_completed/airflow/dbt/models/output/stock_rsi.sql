WITH price_changes AS (
    SELECT 
        symbol,
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS previous_close
    FROM analytics.data_cleaned
),
gains_losses AS (
    SELECT 
        symbol,
        date,
        close,
        previous_close,
        close - previous_close AS price_change,
        CASE 
            WHEN close - previous_close > 0 THEN close - previous_close 
            ELSE 0 
        END AS gain,
        CASE 
            WHEN close - previous_close < 0 THEN ABS(close - previous_close) 
            ELSE 0 
        END AS loss
    FROM price_changes
    WHERE previous_close IS NOT NULL
),
average_gain_loss AS (
    SELECT
        symbol,
        date,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_losses
),
relative_strength AS (
    SELECT
        symbol,
        date,
        avg_gain,
        avg_loss,
        CASE 
            WHEN avg_loss = 0 THEN NULL
            ELSE avg_gain / avg_loss 
        END AS rs
    FROM average_gain_loss
),
rsi_values AS (
    SELECT
        symbol,
        date,
        CASE
            WHEN rs IS NULL THEN NULL
            ELSE 100 - (100 / (1 + rs))
        END AS rsi
    FROM relative_strength
)

SELECT symbol, date, rsi
FROM rsi_values
WHERE rsi IS NOT NULL
ORDER BY symbol, date DESC