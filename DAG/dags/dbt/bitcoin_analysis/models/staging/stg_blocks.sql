select 
    date,
    timestamp as block_timestamp,
    hash as block_hash,
    size as block_size,
    transaction_count as block_transaction_count,
    ingestion_timestamp,
    '{{invocation_id}}' as batch_id
from {{source('bitcoin_src','blocks')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where ingestion_timestamp > (select coalesce(max(ingestion_timestamp),'1900-01-01') from {{ this }} )

{% endif %}