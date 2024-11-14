SELECT
	DISTINCT date,
	close,
	symbol
FROM {{ source('raw_data', 'stock_price') }}
ORDER BY symbol, date DESC 