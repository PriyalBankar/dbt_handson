{{
  config(
    materialized = 'table',
    tags         = ['commerce', 'edw']
  )
}}

/*
  dim_customer
  ────────────
  Customer dimension table with SCD Type 1 (overwrite).
  Grain: one row per customer (current state).
*/

with customer_360 as (

    select * from {{ ref('int_customer_360') }}

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}  as customer_sk,

        -- Natural key
        customer_id,

        -- Profile (non-PII attributes only in EDW)
        customer_segment,
        country_code,
        city,
        signup_date,
        days_since_signup,
        is_active,

        -- Behavioural segments
        rfm_status,
        clv_band,

        -- Order metrics
        total_orders,
        completed_orders,
        returned_orders,
        cancelled_orders,
        lifetime_revenue_usd,
        lifetime_gross_profit_usd,
        avg_order_value_usd,
        first_order_date,
        last_order_date,
        days_since_last_order,
        preferred_channel,
        primary_region,

        -- Return rate
        round(
            returned_orders::float / nullif(total_orders, 0) * 100,
            2
        )                       as return_rate_pct,

        -- Metadata
        current_timestamp()     as dw_updated_at,
        true                    as is_current

    from customer_360

)

select * from final
