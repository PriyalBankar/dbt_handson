{{
  config(
    materialized = 'table',
    tags         = ['finance', 'edw']
  )
}}

/*
  fct_transactions
  ─────────────────
  Financial transactions fact table.
  Grain: one row per transaction.
  Used for GL reconciliation and financial reporting.
*/

with transactions as (

    select *,'col' as dummy from {{ ref('stg_transactions') }}

),

daily_finance as (

    select * from {{ ref('int_finance_daily') }}

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as transaction_sk,

        -- Natural keys
        t.transaction_id,
        t.order_id,

        -- Dates
        t.transaction_date,
        extract(year  from t.transaction_date) as transaction_year,
        extract(month from t.transaction_date) as transaction_month,
        extract(quarter from t.transaction_date) as transaction_quarter,

        -- Attributes
        t.payment_method,
        t.transaction_type,
        t.gl_account,
        t.currency_code,
        t.is_reconciled,

        -- Amounts
        t.amount_usd,
        t.fx_rate,
        t.amount_local,

        -- Daily context from intermediate
        d.total_sales_usd      as daily_total_sales_usd,
        d.total_refunds_usd    as daily_total_refunds_usd,
        d.net_revenue_usd      as daily_net_revenue_usd,
        d.transaction_count    as daily_transaction_count,

        -- Sale/refund split flags
        case when t.transaction_type = 'sale'   then t.amount_usd else 0 end as sale_amount_usd,
        case when t.transaction_type = 'refund' then t.amount_usd else 0 end as refund_amount_usd,

        current_timestamp() as dw_updated_at

    from transactions t
    left join daily_finance d
        on t.transaction_date = d.rate_date
        and t.currency_code   = d.currency_code

)

select * from final
