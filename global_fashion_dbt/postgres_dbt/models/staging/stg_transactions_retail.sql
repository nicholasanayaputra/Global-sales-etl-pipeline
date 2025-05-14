{{ 
  config(
    materialized='view'
  ) 
}}

with source as (

    select
        unique_row_id,
        "Invoice ID" as invoice_id,
        "Line" as line,
        "Customer ID" as customer_id,
        "Product ID" as product_id,
        size,
        color,
        "Unit Price" as unit_price,
        quantity,
        cast(date as timestamp) as transaction_timestamp,
        discount,
        "Line Total" as line_total,
        "Store ID" as store_id,
        "Employee ID" as employee_id,
        currency,
        "Currency Symbol" as currency_symbol,
        sku,
        "Transaction Type" as transaction_type,
        "Payment Method" as payment_method,
        "Invoice Total" as invoice_total

    from {{ source('transactions_global', 'transactions_retail') }}

),

renamed as (

    select
        unique_row_id,
        invoice_id,
        line,
        customer_id,
        product_id,
        size,
        color,
        unit_price,
        quantity,
        transaction_timestamp,
        discount,
        line_total,
        store_id,
        employee_id,
        currency,
        currency_symbol,
        sku,
        lower(transaction_type) as transaction_type,
        lower(payment_method) as payment_method,
        invoice_total,
        
        -- Optional derived field
        unit_price * quantity * (1 - discount) as calculated_line_total

    from source

)

select * from renamed
