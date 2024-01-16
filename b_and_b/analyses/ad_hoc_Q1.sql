-- What proportion of orders contained a product with category “Sheet Sets” and size “King”?

with
product_table as (
    select * from {{ ref('stg_product')}}
),
order_table as (
    select * from {{ ref('stg_orders')}}
),

-- perform required joins for question 1 
table_join as (
select 
       o.order_id,
       o.product_id,
       p.category,
       p.variant_option2,
       CASE WHEN COUNTIF(lower(p.category) = 'sheet sets' and lower(p.variant_option2) = 'king') OVER(PARTITION BY o.order_id) >= 1
       THEN 1 ELSE 0 
       END as with_category_and_size
from order_table o
left join product_table p
on o.product_id = p.product_id and o.variant_id = p.variant_id
)


-- 1.	What proportion of orders contained a product with category “Sheet Sets” and size “King”?
select
  -- orders contained a product with category “Sheet Sets” and size “King”
  (select count(distinct order_id) from table_join where with_category_and_size = 1) as reqiuired_order_id,
  -- total distinct order ids 
  count(distinct order_id) as total_unique_order_id,
  -- proportion of orders contained a product with category “Sheet Sets” and size “King”
  ((select count(distinct order_id) from table_join where with_category_and_size = 1)/count(distinct order_id)) as proportion
from table_join
