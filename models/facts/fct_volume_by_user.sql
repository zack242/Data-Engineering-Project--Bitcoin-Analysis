SELECT 
    DATE,
    ADDRESS_INPUT AS "ADDRESS",
    SUM(TX_INPUT_VALUE) AS "VOLUME_BY_USER"
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
    DATE,
    ADDRESS_INPUT
