{{ 
    config(
        materialized='incremental',
        unique_key = 'id',
        incremental_strategy = 'merge'
    )
}}


select
    o.id                            as id,
    o.order_id                      as order_id, 
    o.created_at                    as order_created_at, 
    o.subtotal                      as order_subtotal, 
    o.total                         as order_total, 
    o.line_id                       as order_line_id, 
    o.product_id                    as product_id, 
    o.variant_id                    as variant_id,
    o.price                         as line_item_price,
    o.quantity                      as line_item_quantity,
    (o.price)*(o.quantity)          as line_item_revenue,
    o.line_total_discount           as line_total_discount,
    sum(o.quantity) over (partition by o.order_id) as units_in_order,
    ((o.price)*(o.quantity) - o.line_total_discount ) as line_item_gross_revenue,
    p.category                      as product_category,
    p.title                         as product_title, 
    p.variant_sku                   as product_variant_sku,
    p.variant_title                 as product_variant_title, 
    p.variant_option1               as product_variant_style, 
    p.variant_option2               as product_variant_size,
    o.etl_loaded_at                 as previous_sync,
    current_timestamp()             as _timestamp -- timestamp of when record was loaded into this table. 

from {{ ref('stg_orders')}} as o
left join {{ref('stg_product')}} p 
on o.product_id = p.product_id 
and o.variant_id = p.variant_id


-- only process the rows which were loaded in staging tables ( stg_orders ) after the last load into this table. 
{% if is_incremental() %}
where o.etl_loaded_at >= (select max(_timestamp) from {{this}} )
{% endif %}




