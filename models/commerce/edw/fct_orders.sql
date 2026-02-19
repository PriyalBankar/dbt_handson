{{
  config(
    materialized = 'table',
    tags         = ['commerce', 'edw']
  )
}}

/*
  fct_orders
  ──────────
  The primary fact table for order analytics.
  Grain: one row per order.

  Selector relevance:
    - fct_orders_full_chain: +fct_orders (entire upstream DAG incl. finance domain)
    - fct_orders_hotfix:    1+fct_orders (immediate parents only)
    - fct_orders_downstream: fct_orders+ (semantic layer and any downstream)
    - commerce_edw:          intersection(tag:commerce, tag:edw)
*/

with orders as (

    select * from {{ ref('int_orders_enriched') }}

),

-- Surrogate keys using dbt_utils
final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_id']) }}   as order_sk,

        -- Natural keys
        order_id,
        customer_id,
        product_id,

        -- Dates (for joining to a date dimension if present)
        order_date,
        extract(year  from order_date)   as order_year,
        extract(month from order_date)   as order_month,
        extract(quarter from order_date) as order_quarter,

        -- Dimension attributes (degenerate dims)
        order_status,
        order_channel,
        region,
        currency_code,
        category,
        subcategory,
        customer_segment,
        customer_country,

        -- Measures
        order_amount_usd,
        order_amount_local,
        fx_rate,
        unit_cost_usd,
        list_price_usd,
        gross_margin_pct,
        recognized_revenue_usd,
        gross_profit_usd,

        -- Flags
        is_completed,
        is_returned,
        is_cancelled,

        -- Metadata
        _dbt_updated_at

    from orders

)

select * from final
