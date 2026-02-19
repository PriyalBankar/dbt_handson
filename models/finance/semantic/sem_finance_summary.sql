{{
  config(
    materialized = 'view',
    tags         = ['finance', 'semantic']
  )
}}

/*
  sem_finance_summary
  ────────────────────
  Finance semantic view for CFO/finance dashboards.
  Grain: one row per transaction.
  Business-friendly column names.
*/

with transactions as (

    select * from {{ ref('fct_transactions') }}

),

final as (

    select
        -- Dimensions
        transaction_id                          as "Transaction ID",
        order_id                                as "Order ID",
        transaction_date                        as "Transaction Date",
        transaction_year                        as "Year",
        transaction_month                       as "Month",
        transaction_quarter                     as "Quarter",
        payment_method                          as "Payment Method",
        transaction_type                        as "Transaction Type",
        gl_account                              as "GL Account",
        currency_code                           as "Currency",
        is_reconciled                           as "Is Reconciled",

        -- Measures
        amount_usd                              as "Amount (USD)",
        fx_rate                                 as "FX Rate",
        amount_local                            as "Amount (Local Currency)",
        sale_amount_usd                         as "Sale Amount (USD)",
        refund_amount_usd                       as "Refund Amount (USD)",

        -- Daily context
        daily_total_sales_usd                   as "Daily Total Sales (USD)",
        daily_total_refunds_usd                 as "Daily Total Refunds (USD)",
        daily_net_revenue_usd                   as "Daily Net Revenue (USD)",
        daily_transaction_count                 as "Daily Transaction Count"

    from transactions

)

select * from final
