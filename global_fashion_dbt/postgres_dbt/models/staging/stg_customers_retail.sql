{{ 
  config(
    materialized='view'
  ) 
}}

with customers as (

    select *,
        row_number() over (
            partition by Customer, lower(trim(Email))
            order by Customer
        ) as rn
    from {{ source('customers_global', 'customers_retail') }}
    where Customer is not null
      and Email is not null

)

select
    -- Surrogate unique key
    {{ dbt_utils.generate_surrogate_key(['Customer', 'Email']) }} as customer_key,

    -- Customer identity
    Customer as customer_id,
    initcap(trim(Name)) as full_name,
    lower(trim(Email)) as email,
    regexp_replace(trim(Telephone), '[^0-9]', '', 'g') as phone_number,

    -- Location
    initcap(City) as city,
    initcap(Country) as country,

    -- Personal information
    upper(trim(Gender)) as gender,
    {{ dbt.safe_cast('"Date Of Birth"', api.Column.translate_type("date")) }} as birth_date,
    date_part('year', age(current_date, {{ dbt.safe_cast('"Date Of Birth"', api.Column.translate_type("date")) }})) as age,
    initcap("Job Title") as job_title

from customers
where rn = 1
