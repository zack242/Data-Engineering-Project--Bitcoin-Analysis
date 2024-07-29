SELECT 
    DATE,
    SUM(TX_FEE) AS "FEES"
FROM 
    {{ ref('stg_transactions_details') }}
{% if is_incremental() %}
WHERE 
    ingestion_timestamp > (
        SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01') 
        FROM {{ this }}
    )
{% endif %}
GROUP BY 
    DATE
