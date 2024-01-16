-- What are the top five non-null session sources which garnered the most gross revenue?



-- for question 7, I am assuming that top "five non-null session sources " mean "Session source - first `utm_source` of a session" defined in the word document in Challenge 2

with web_sessions as (
select session_id, event_id, event_properties, session_first_utm_source ,event_name,
       replace(CAST(JSON_QUERY(event_properties, '$.order_id') as string),'"','') as order_id
 from {{ ref('web_session_final')}}
 where session_first_utm_source is not null
),

orders as (
select *
 from from {{ ref('stg_orders')}}
-- only select events that are page vi
), 

gross_revenue_calc as (
  select w.session_first_utm_source,
        ((o.quantity * o.price) - (o.line_total_discount)) as line_item_gross_revenue
  from 
  web_sessions w 
  left join orders o 
  on w.order_id = o.order_id
  where w.order_id is not null
)

-- Question 7. 	What are the top five non-null session sources which garnered the most gross revenue?
select session_first_utm_source, sum(gross_revenue_calc.line_item_gross_revenue)  as gross_revenue_by_session_source
from gross_revenue_calc
group by session_first_utm_source
order by sum(gross_revenue_calc.line_item_gross_revenue)  desc
limit 5