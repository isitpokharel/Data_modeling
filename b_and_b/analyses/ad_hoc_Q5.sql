
-- Among `page` events only, what are the top five most common page URLs that immediately preceded a user’s navigation to the “checkout.bollandbranch.com” domain during their session?

with web_sessions as (
select *,
-- new column that indicates whether given event_url is a checkout page or not. 
      CASE WHEN event_url like '%checkout.bollandbranch.com%' then 1 else 0 
       end as is_check_out_page
 from {{ ref('web_session_final')}}
-- only select events that are page view events
where is_page_view_event = 1
),

table_calc as (
select session_id, event_id, event_url,is_check_out_page,event_created_at,
       -- window function to identify event url that immeditaely precedded the check out page url
       LAG(is_check_out_page) over(partition by session_id order by event_created_at desc) as last_events_url_before_checkout
from  web_sessions
-- and event_url like '%checkout.bollandbranch.com%'
order by session_id, event_created_at
)

-- 5.	Among `page` events only, what are the top five most common page URLs that immediately preceded a user’s navigation to the “checkout.bollandbranch.com” domain during their session?
select event_url, count(*)  from table_calc
where last_events_url_before_checkout = 1 
group by event_url
order by count(*) desc
limit 5