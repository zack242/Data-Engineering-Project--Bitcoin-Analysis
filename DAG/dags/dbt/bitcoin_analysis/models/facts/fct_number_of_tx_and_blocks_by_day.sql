SELECT 
    DATE,
    COUNT(*) AS number_of_blocks,
    SUM(block_transaction_count) AS number_of_transactions,
    INGESTION_TIMESTAMP
FROM 
    {{ ref('stg_blocks') }} 
{% if is_incremental() %}
WHERE 
    INGESTION_TIMESTAMP > (
        SELECT MAX(INGESTION_TIMESTAMP) 
        FROM {{ this }}
    )
{% endif %} 
GROUP BY 
    DATE,INGESTION_TIMESTAMP
