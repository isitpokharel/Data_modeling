
-- Which date had the highest average order units?

with order_table as (
    select * 
    from {{ ref('stg_orders')}}
),

-- perform required joins for question 3
table_join as (
select 
    date(o.created_at) as created_at_date,
    count(distinct order_id) as total_distinct_orders,
    count(distinct line_id) as total_distinct_line_id,
    count(distinct line_id)/count(distinct order_id) as average_order_units
    from order_table o
    group by date(o.created_at)
)

-- Q3 Which date had the highest average order units?
select * from table_join
order by table_join.average_order_units desc
limit 1
