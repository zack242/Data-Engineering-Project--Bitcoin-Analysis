select 
    DATE,
    sum(TX_INPUT_VALUE) as "volume_in",
    sum(TX_OUTPUT_VALUE) as "volume_out"
from {{ ref('bitcoin_transactions_details') }}
{% if is_incremental() %}
where ingestion_timestamp > (select coalesce(max(ingestion_timestamp), '1900-01-01') from {{ this }})
{% endif %}
group by DATE
