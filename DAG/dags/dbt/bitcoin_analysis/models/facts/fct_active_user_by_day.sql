SELECT 
    DATE,
    COUNT(DISTINCT ADDRESS_INPUT) AS "ACTIVE_USER_COUNT",
    INGESTION_TIMESTAMP
FROM 
    {{ ref('stg_transactions_details') }}
{% if is_incremental() %}
WHERE 
    INGESTION_TIMESTAMP > (
        SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '1900-01-01') 
        FROM {{ this }}
    )
{% endif %}
GROUP BY 
    DATE,INGESTION_TIMESTAMP
