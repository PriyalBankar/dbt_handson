{{
  config(
    materialized = 'table',
    tags         = ['finance', 'intermediate']
  )
}}

/*
  int_finance_daily
  ─────────────────
  Aggregates daily FX rates and transaction totals per currency.
  This model is referenced by the COMMERCE domain's int_orders_enriched,
  making it a cross-domain dependency anchor.

  Cross-domain consumers:
    - commerce.intermediate.int_orders_enriched  (reads fx_rate, rate_date)
*/

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

daily_summary as (

    select
        transaction_date                    as rate_date,
        currency_code,

        -- FX rate: average of the day's transactions for that currency
        avg(fx_rate)                        as fx_rate,

        -- Volume metrics
        count(transaction_id)               as transaction_count,
        sum(case when transaction_type = 'sale'   then amount_usd else 0 end)  as total_sales_usd,
        sum(case when transaction_type = 'refund' then amount_usd else 0 end)  as total_refunds_usd,
        sum(amount_usd)                     as net_revenue_usd,

        -- Reconciliation
        sum(case when is_reconciled then 1 else 0 end) as reconciled_count,
        count(transaction_id) - sum(case when is_reconciled then 1 else 0 end)
                                            as unreconciled_count

    from transactions
    group by 1, 2

)

select * from daily_summary
