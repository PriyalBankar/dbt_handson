{{
    config(
        materialized='table'
    )
}}
select * from {{ ref('fct_transactions') }}
-- {{ ref('int_orders_enriched') }}