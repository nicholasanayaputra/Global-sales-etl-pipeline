{{ config(
    materialized='table'
) }}

with transactions_retail as (
    select * from {{ ref('stg_transactions_retail') }}
),

customers_retail as (
    select * from {{ ref('stg_customers_retail') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_discounts as (
    select * from {{ ref('dim_discounts') }}
),

dim_employees as (
    select * from {{ ref('dim_employees') }}
),

dim_stores as (
    select * from {{ ref('dim_stores') }}
),

joined as (
    select
        t.unique_row_id,
        t.invoice_id,
        t.line,
        t.transaction_timestamp,
        t.customer_id,

        c.full_name,
        c.email,
        c.city as customer_city,
        c.country as customer_country,
        c.gender,
        c.age,
        c.job_title,

        t.product_id,
        p.category,
        p.sub_category,
        p.description_en,
        p.production_cost,
        p.color,
        p.size,
        t.unit_price,
        t.quantity,
        t.line_total,
        t.discount,
        t.calculated_line_total,

        d.description as discount_description,

        t.store_id,
        s.store_name,
        s.city as store_city,
        s.country as store_country,

        t.employee_id,
        e.full_name as employee_name,
        e.job_title as employee_position,

        t.currency,
        t.currency_symbol,
        t.transaction_type,
        t.payment_method,
        t.invoice_total

    from transactions_retail t
    left join customers_retail c on t.customer_id = c.customer_id::integer
    left join dim_products p on t.product_id::text = p.product_id
    left join dim_discounts d 
    on p.category = d.category 
    and p.sub_category = d.sub_category
    and t.transaction_timestamp between d.start and d.end
    left join dim_employees e on t.employee_id = e.employee_id
    left join dim_stores s on t.store_id = s.store_id
)
select * from joined