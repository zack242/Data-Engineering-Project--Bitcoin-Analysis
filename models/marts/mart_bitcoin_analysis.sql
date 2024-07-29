with active_users as (
    select 
        date,
        ACTIVE_USER_COUNT
    from 
        {{ ref('fct_active_user_by_day') }}
),

fees as (
    select 
        date,
        FEES
    from 
        {{ ref('fct_fees_by_day') }}
),

transactions_and_blocks as (
    select 
        date,
        NUMBER_OF_BLOCKS,
        NUMBER_OF_TRANSACTIONS
    from 
        {{ ref('fct_number_of_tx_and_blocks_by_day') }}
),

volume_by_day as (
    select 
        date,
        VOLUME_IN,
        VOLUME_OUT
    from 
        {{ ref('fct_volume_by_day') }}
)

select 
    a.date,
    a.ACTIVE_USER_COUNT,
    f.FEES,
    t.NUMBER_OF_TRANSACTIONS,
    t.NUMBER_OF_BLOCKS,
    v.VOLUME_IN,
    v.VOLUME_OUT
from 
    active_users a
left join 
    fees f on a.date = f.date
left join 
    transactions_and_blocks t on a.date = t.date
left join 
    volume_by_day v on a.date = v.date
