{{
  config(
    materialized = 'view',
    tags         = ['finance', 'staging']
  )
}}

with source as (

    select * from {{ ref('raw_transactions') }}

),

renamed as (

    select
        -- Keys
        transaction_id::varchar             as transaction_id,
        order_id::varchar                   as order_id,

        -- Dates
        try_cast(transaction_date as date)  as transaction_date,

        -- Amounts
        amount_usd::number(12,4)            as amount_usd,
        upper(currency_code)                as currency_code,
        fx_rate::number(10,6)               as fx_rate,

        -- Compute local currency amount
        round(amount_usd * fx_rate, 2)      as amount_local,

        -- Attributes
        lower(payment_method)               as payment_method,
        lower(transaction_type)             as transaction_type,
        gl_account,
        is_reconciled::boolean              as is_reconciled,

        -- Metadata
        current_timestamp()                 as _dbt_loaded_at

    from source
    where transaction_id is not null

)

select * from renamed
