{{
  config(
    materialized = 'view',
    tags         = ['commerce', 'staging', 'pii']
  )
}}

with source as (

    select * from {{ ref('raw_customers') }}

),

renamed as (

    select
        -- Keys
        customer_id::varchar            as customer_id,

        -- PII fields — tagged with pii
        initcap(first_name)             as first_name,
        initcap(last_name)              as last_name,
        lower(email)                    as email,
        phone                           as phone_number,

        -- Attributes
        upper(country)                  as country_code,
        initcap(city)                   as city,
        try_cast(signup_date as date)   as signup_date,
        lower(customer_segment)         as customer_segment,
        is_active::boolean              as is_active,

        -- Derived
        datediff('day', try_cast(signup_date as date), current_date()) as days_since_signup,

        -- Metadata
        current_timestamp()             as _dbt_loaded_at

    from source
    where customer_id is not null

)

select * from renamed
