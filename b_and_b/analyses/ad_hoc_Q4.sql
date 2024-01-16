-- What was the conversion rate of web sessions where the user added a “Plush Bath Towel Set” product to their cart?


with
product as (
    select * from {{ ref('stg_product')}}
),

-- get product id of product with title "Plush Bath Towel Set"
required_product_id as (
select product_id from product
where title = 'Plush Bath Towel Set'
),

-- add a binary field to web_sessions table to indicate if an event qualifies for condition "where the user added a “Plush Bath Towel Set” product to their cart?"
web_sessions as (
select *,
CASE WHEN replace(CAST(JSON_QUERY(event_properties, '$.product_id') as string),'"','') = CONCAT("",(select distinct(product_id) from required_product_id LIMIT 1),"") THEN 1 ELSE 0 
END as is_plush_bath_towel_set
 from {{ ref('web_session_final')}}
),

-- get all the session_ids from web_sessions table table that have is_plush_bath_towel_set = 1
required_web_sessions as 
(
  select
        session_id
  from web_sessions
  where is_plush_bath_towel_set = 1
)

-- Q4 What was the conversion rate of web sessions where the user added a “Plush Bath Towel Set” product to their cart?
select COUNT( distinct session_id) as total_web_sessions,
       (select count(distinct session_id) from web_sessions where is_order_completed_order = 1 and is_plush_bath_towel_set = 1) as sessions_with_order_completed,
       (select count(distinct session_id) from web_sessions where is_order_completed_order = 1 and is_plush_bath_towel_set = 1)/COUNT( distinct session_id) as conversion_rate
from 
web_sessions
where session_id in (select session_id from required_web_sessions)










