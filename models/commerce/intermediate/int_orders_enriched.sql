{{
  config(
    materialized  = 'incremental',
    unique_key    = 'order_id',
    on_schema_change = 'append_new_columns',
    tags          = ['commerce', 'intermediate', 'cross_domain']
  )
}}

/*
  int_orders_enriched
  ────────────────────
  Enriches commerce orders with:
    1. Customer details (same domain)
    2. Product details  (same domain)
    3. FX rates from the FINANCE domain  ← cross-domain ref!

  Selector relevance:
    - Included in: orders_enriched_with_parents, cross_domain_models, finance_daily_with_children
    - When int_finance_daily changes → this model must rebuild → use: finance_daily_with_children
*/

with orders as (

    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        -- Only process new/updated orders on incremental runs
        where order_date >= (select max(order_date) from {{ this }})
    {% endif %}

),

customers as (

    -- Same-domain reference: commerce → commerce
    select * from {{ ref('stg_customers') }}

),

products as (

    -- Same-domain reference: commerce → commerce
    select * from {{ ref('stg_products') }}

),

finance_rates as (

    -- CROSS-DOMAIN reference: commerce → finance
    -- This creates a DAG dependency across domain boundaries!
    select
        rate_date,
        currency_code,
        fx_rate
    from {{ ref('int_finance_daily') }}

),

enriched as (

    select
        -- Order keys
        o.order_id,
        o.customer_id,
        o.product_id,
        o.order_date,

        -- Order details
        o.order_status,
        o.order_channel,
        o.region,

        -- Original amounts
        o.order_amount_usd,
        o.currency_code,

        -- FX enrichment from finance domain
        coalesce(f.fx_rate, 1.0)                        as fx_rate,
        round(o.order_amount_usd * coalesce(f.fx_rate, 1.0), 2) as order_amount_local,

        -- Customer context
        c.customer_segment,
        c.country_code                                  as customer_country,
        c.days_since_signup,

        -- Product context
        p.product_name,
        p.category,
        p.subcategory,
        p.unit_cost_usd,
        p.list_price_usd,
        p.gross_margin_pct,

        -- Derived flags
        case when o.order_status = 'completed'  then true  else false end as is_completed,
        case when o.order_status = 'returned'   then true  else false end as is_returned,
        case when o.order_status = 'cancelled'  then true  else false end as is_cancelled,

        -- Revenue recognition (only count completed)
        case when o.order_status = 'completed'
             then o.order_amount_usd else 0 end          as recognized_revenue_usd,

        -- Margin (only on completed)
        case when o.order_status = 'completed'
             then round(o.order_amount_usd - p.unit_cost_usd, 2) else 0 end as gross_profit_usd,

        -- Metadata
        current_timestamp()                             as _dbt_updated_at

    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products  p on o.product_id  = p.product_id
    left join finance_rates f
        on o.order_date    = f.rate_date
        and o.currency_code = f.currency_code

)

select * from enriched
