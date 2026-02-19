{{
  config(
    materialized = 'incremental',
    unique_key   = 'customer_id',
    on_schema_change = 'append_new_columns',
    tags         = ['commerce', 'intermediate']
  )
}}

/*
  int_customer_360
  ─────────────────
  Aggregates all order activity per customer.
  Provides the 360-degree customer view used by the EDW dim_customer.
*/

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    -- Use enriched orders so we get recognized_revenue and margin
    select * from {{ ref('int_orders_enriched') }}

),

order_aggs as (

    select
        customer_id,

        -- Volume
        count(order_id)                                     as total_orders,
        count(case when is_completed then order_id end)     as completed_orders,
        count(case when is_returned  then order_id end)     as returned_orders,
        count(case when is_cancelled then order_id end)     as cancelled_orders,

        -- Revenue
        sum(recognized_revenue_usd)                         as lifetime_revenue_usd,
        sum(gross_profit_usd)                               as lifetime_gross_profit_usd,
        avg(case when is_completed then order_amount_usd end) as avg_order_value_usd,

        -- Recency
        min(order_date)                                     as first_order_date,
        max(order_date)                                     as last_order_date,
        datediff('day', max(order_date), current_date())    as days_since_last_order,

        -- Behaviour
        max(order_channel)                                  as preferred_channel,
        max(region)                                         as primary_region

    from orders
    group by 1

),

final as (

    select
        c.customer_id,
        c.customer_segment,
        c.country_code,
        c.city,
        c.signup_date,
        c.days_since_signup,
        c.is_active,

        -- Order stats
        coalesce(a.total_orders, 0)             as total_orders,
        coalesce(a.completed_orders, 0)         as completed_orders,
        coalesce(a.returned_orders, 0)          as returned_orders,
        coalesce(a.cancelled_orders, 0)         as cancelled_orders,
        coalesce(a.lifetime_revenue_usd, 0)     as lifetime_revenue_usd,
        coalesce(a.lifetime_gross_profit_usd,0) as lifetime_gross_profit_usd,
        a.avg_order_value_usd,
        a.first_order_date,
        a.last_order_date,
        a.days_since_last_order,
        a.preferred_channel,
        a.primary_region,

        -- RFM scoring (simplified)
        case
            when a.days_since_last_order <= 30  then 'active'
            when a.days_since_last_order <= 90  then 'at_risk'
            when a.days_since_last_order <= 180 then 'lapsed'
            else 'churned'
        end                                     as rfm_status,

        -- CLV band
        case
            when coalesce(a.lifetime_revenue_usd, 0) >= 1000 then 'high'
            when coalesce(a.lifetime_revenue_usd, 0) >= 300  then 'medium'
            else 'low'
        end                                     as clv_band,

        current_timestamp()                     as _dbt_updated_at

    from customers c
    left join order_aggs a on c.customer_id = a.customer_id

)

select * from final

{% if is_incremental() %}
    where customer_id in (
        select distinct customer_id from {{ ref('int_orders_enriched') }}
        where _dbt_updated_at >= (select max(_dbt_updated_at) from {{ this }})
    )
{% endif %}
