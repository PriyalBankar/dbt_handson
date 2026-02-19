{{
  config(
    materialized = 'view',
    tags         = ['commerce', 'staging']
  )
}}

with source as (

    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        -- Keys
        order_id::varchar              as order_id,
        customer_id::varchar           as customer_id,
        product_id::varchar            as product_id,

        -- Dates
        try_cast(order_date as date)   as order_date,

        -- Amounts
        order_amount_usd::number(12,2) as order_amount_usd,
        upper(currency_code)           as currency_code,

        -- Attributes
        lower(status)                  as order_status,
        lower(channel)                 as order_channel,
        upper(region)                  as region,

        -- Metadata
        current_timestamp()            as _dbt_loaded_at

    from source
    where order_id is not null

)

select * from renamed
