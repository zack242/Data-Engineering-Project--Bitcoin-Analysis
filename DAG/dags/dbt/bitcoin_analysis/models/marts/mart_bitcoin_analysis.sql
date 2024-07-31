WITH active_users AS (
    SELECT 
        date,
        ACTIVE_USER_COUNT,
        ingestion_timestamp
    FROM 
        {{ ref('fct_active_user_by_day') }}
    {% if is_incremental() %}
    WHERE 
        ingestion_timestamp > (
            SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01') 
            FROM {{ this }}
        )
    {% endif %}
),

fees AS (
    SELECT 
        date,
        FEES
    FROM 
        {{ ref('fct_fees_by_day') }}
    {% if is_incremental() %}
    WHERE 
        ingestion_timestamp > (
            SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01') 
            FROM {{ this }}
        )
    {% endif %}
),

transactions_and_blocks AS (
    SELECT 
        date,
        NUMBER_OF_BLOCKS,
        NUMBER_OF_TRANSACTIONS
    FROM 
        {{ ref('fct_number_of_tx_and_blocks_by_day') }}
    {% if is_incremental() %}
    WHERE 
        ingestion_timestamp > (
            SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01') 
            FROM {{ this }}
        )
    {% endif %}
),

volume_by_day AS (
    SELECT 
        date,
        VOLUME_IN,
        VOLUME_OUT
    FROM 
        {{ ref('fct_volume_by_day') }}
    {% if is_incremental() %}
    WHERE 
        ingestion_timestamp > (
            SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01') 
            FROM {{ this }}
        )
    {% endif %}
)

SELECT 
    a.date,
    a.ingestion_timestamp,
    a.ACTIVE_USER_COUNT,
    f.FEES,
    t.NUMBER_OF_TRANSACTIONS,
    t.NUMBER_OF_BLOCKS,
    v.VOLUME_IN,
    v.VOLUME_OUT

FROM 
    active_users a
LEFT JOIN 
    fees f ON a.date = f.date
LEFT JOIN 
    transactions_and_blocks t ON a.date = t.date
LEFT JOIN 
    volume_by_day v ON a.date = v.date
