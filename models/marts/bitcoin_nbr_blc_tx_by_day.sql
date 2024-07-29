with data as (
    SELECT * 
    FROM {{ ref('bitcoin_blocks') }} 
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT COALESCE(MAX(ingestion_timestamp), '1900-01-01') FROM {{ this }})
    {% endif %}
)
select DATE,
count(*) as "nombre_of_blocks",
sum(BLOCK_TRANSACTION_COUNT) as "nombre_of_transactions" 
from data group by DATE