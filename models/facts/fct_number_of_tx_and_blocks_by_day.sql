WITH data AS (
    SELECT * 
    FROM {{ ref('stg_blocks') }} 
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (
        SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01') 
        FROM {{ this }}
    )
    {% endif %}
)

SELECT 
    DATE,
    COUNT(*) AS "NUMBER_OF_BLOCKS",
    SUM(BLOCK_TRANSACTION_COUNT) AS "NUMBER_OF_TRANSACTIONS" 
FROM 
    data 
GROUP BY 
    DATE
