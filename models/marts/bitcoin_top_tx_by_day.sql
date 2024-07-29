with data as (
    SELECT * 
    FROM {{ ref('bitcoin_transactions_details') }} 
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01') FROM {{ this }})
    {% endif %}
)
SELECT 
    DATE,
    TX_HASH_2,
    TX_OUTPUT_VALUE 
FROM data
ORDER BY TX_OUTPUT_VALUE DESC LIMIT 10
