select 
    date,
    hash as tx_hash, 
    block_hash,
    block_timestamp,
    input_value as tx_input_value,
    output_value as tx_output_value,
    fee as tx_fee,
    is_coinbase as tx_is_coinbase,
    inputs as tx_inputs_data,
    outputs as tx_outputs_data,
    ingestion_timestamp,
    '{{invocation_id}}' as batch_id
from {{source('bitcoin_src','transactions')}}
where date = '{{ var('cut_off_date' ) }}'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
and ingestion_timestamp > (select coalesce(max(ingestion_timestamp),'1900-01-01') from {{ this }} )

{% endif %}