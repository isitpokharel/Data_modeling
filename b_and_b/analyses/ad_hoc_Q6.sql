-- What are the top five non-null session campaigns which garnered the most web users?



-- for question 6, I am assuming that top "five non-null session campaigns " mean "Session Campaign - first `utm_campaign` of a session" defined in the word document in Challenge 2
with web_sessions as (
select session_id, event_id, session_first_utm_campaign , web_user_id
 from {{ ref('web_session_final')}} 
 where session_first_utm_campaign is not null
)

select session_first_utm_campaign, count(distinct web_user_id) as distinct_web_users_for_campaign
from web_sessions
group by session_first_utm_campaign
order by count(distinct web_user_id) desc 
limit 5


