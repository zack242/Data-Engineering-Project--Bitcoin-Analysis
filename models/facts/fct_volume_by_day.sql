SELECT 
    DATE,
    SUM(TX_INPUT_VALUE) AS "VOLUME_IN",
    SUM(TX_OUTPUT_VALUE) AS "VOLUME_OUT"
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
