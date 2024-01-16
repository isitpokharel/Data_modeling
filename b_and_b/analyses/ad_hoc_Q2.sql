-- Which product SKU generated the most gross revenue?

with
product_table as (
    select * from {{ ref('stg_product')}}
),
order_table as (
    select * from {{ ref('stg_orders')}}
), 

-- perform required joins for question 2
table_join as (
select 
    --    o._id,
    --    o.product_id,
    --    o.variant_id,
       p.variant_sku,
       sum((o.quantity * o.price) - (o.line_total_discount)) as line_item_gross_revenue
       from order_table o
       left join product_table p
       on o.product_id = p.product_id and o.variant_id = p.variant_id
       group by p.variant_sku
       order by line_item_gross_revenue DESC
)

-- 2.	Which product SKU generated the most gross revenue?

select * from table_join
limit 1