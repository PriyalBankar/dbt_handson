{{
  config(
    materialized = 'view',
    tags         = ['commerce', 'semantic']
  )
}}

/*
  sem_customer_kpis
  ──────────────────
  Customer-level KPI view for BI dashboards.
  Exposes customer 360 data in business-friendly format.
  Grain: one row per customer.
*/

with customers as (

    select * from {{ ref('dim_customer') }}

),

final as (

    select
        -- Identity
        customer_id                             as "Customer ID",
        customer_segment                        as "Segment",
        country_code                            as "Country",
        city                                    as "City",
        signup_date                             as "Signup Date",
        is_active                               as "Is Active",

        -- Behavioural Segments
        rfm_status                              as "RFM Status",
        clv_band                                as "CLV Band",

        -- Order KPIs
        total_orders                            as "Total Orders",
        completed_orders                        as "Completed Orders",
        returned_orders                         as "Returned Orders",
        cancelled_orders                        as "Cancelled Orders",
        return_rate_pct                         as "Return Rate %",

        -- Revenue KPIs
        lifetime_revenue_usd                    as "Lifetime Revenue (USD)",
        lifetime_gross_profit_usd               as "Lifetime Gross Profit (USD)",
        avg_order_value_usd                     as "Avg Order Value (USD)",

        -- Recency
        first_order_date                        as "First Order Date",
        last_order_date                         as "Last Order Date",
        days_since_last_order                   as "Days Since Last Order",
        days_since_signup                       as "Days Since Signup",

        -- Channel
        preferred_channel                       as "Preferred Channel",
        primary_region                          as "Primary Region"

    from customers

)

select * from final
