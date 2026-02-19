{{
  config(
    materialized = 'view',
    tags         = ['commerce', 'semantic']
  )
}}

/*
  sem_revenue_metrics
  ────────────────────
  Semantic layer view for revenue analytics.
  Business-friendly column names ready for BI tools (Tableau, Looker, Power BI).
  Grain: one row per order.

  This is the top of the commerce DAG — fct_orders+ will include this model.
*/

with orders as (

    select * from {{ ref('fct_orders') }}

),

final as (

    select
        -- Dimensions (for slicing in BI)
        order_id                                as "Order ID",
        order_date                              as "Order Date",
        order_year                              as "Year",
        order_month                             as "Month",
        order_quarter                           as "Quarter",
        order_channel                           as "Sales Channel",
        region                                  as "Region",
        currency_code                           as "Currency",
        category                                as "Product Category",
        subcategory                             as "Product Subcategory",
        customer_segment                        as "Customer Segment",
        customer_country                        as "Customer Country",
        order_status                            as "Order Status",

        -- Revenue Metrics
        order_amount_usd                        as "Gross Revenue (USD)",
        recognized_revenue_usd                  as "Net Revenue (USD)",
        gross_profit_usd                        as "Gross Profit (USD)",
        gross_margin_pct                        as "Gross Margin %",
        order_amount_local                      as "Revenue (Local Currency)",
        fx_rate                                 as "FX Rate",

        -- Flags (for boolean filters in BI)
        is_completed                            as "Is Completed",
        is_returned                             as "Is Returned",
        is_cancelled                            as "Is Cancelled"

    from orders

)

select * from final
