{{
  config(
    materialized = 'view',
    tags         = ['commerce', 'staging']
  )
}}

with source as (

    select * from {{ ref('raw_products') }}

),

renamed as (

    select
        -- Keys
        product_id::varchar                 as product_id,
        supplier_id::varchar                as supplier_id,

        -- Attributes
        product_name,
        initcap(category)                   as category,
        initcap(subcategory)                as subcategory,

        -- Financials
        unit_cost_usd::number(12,2)         as unit_cost_usd,
        list_price_usd::number(12,2)        as list_price_usd,

        -- Derived margin
        round(
            (list_price_usd - unit_cost_usd) / nullif(list_price_usd, 0) * 100,
            2
        )                                   as gross_margin_pct,

        -- Flags
        is_active::boolean                  as is_active,
        try_cast(launch_date as date)       as launch_date,

        -- Metadata
        current_timestamp()                 as _dbt_loaded_at

    from source
    where product_id is not null

)

select * from renamed
