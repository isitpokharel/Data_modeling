
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ 
    config(
        materialized='incremental',
        unique_key = 'event_id',
        incremental_strategy = 'merge'
    )
}}

SELECT 
    session_id,
    event_id, 
    _loaded_at,
    cookie_id,
    customer_id,
    event_name,
    event_url,
    event_properties,
    timestamp as event_created_at,
    utm_campaign,
    utm_medium,
    utm_source,

    -- user is defined as the first known customer_id that is associated with the cookie of the web session. 
    -- If the session user has no known customer_id then default to the cookie_id.
    coalesce(FIRST_VALUE(customer_id IGNORE NULLS) over (partition by session_id order by timestamp),cookie_id) as web_user_id,
    -- boolean field to indicate if an event is a page view event. 
    CASE WHEN event_name = 'page' 
    THEN 1 ELSE 0 
        END as is_page_view_event,
    --boolean field to indicate whether a session is bounced session or not. 1 = bounced, 0= not bounced
    --for each session id, if there are less than 1 events where event_name = page, then categorize that session as bounced session.

    CASE WHEN COUNTIF(event_name = 'page') OVER(partition by session_id) <= 1
        THEN 1 ELSE 0
        END AS is_bounced_session,
    -- boolean field to indicate whether a session had at least one event with event_name = 'product_viewed'. 
    CASE WHEN COUNTIF(event_name = 'product_viewed') OVER(partition by session_id) >= 1
    THEN 1 ELSE 0
    END AS is_product_viewed_session,
    -- boolean field to indicate whether a session includes a  'product_added' event. 
    CASE WHEN COUNTIF(event_name = 'product_added') OVER(partition by session_id) >= 1
    THEN 1 ELSE 0
    END AS is_product_added_session,
    -- boolean field to indicate whether a session includes a  'checkout_step_viewed' event. 
    CASE WHEN COUNTIF(event_name = 'checkout_step_viewed') OVER(partition by session_id) >= 1
    THEN 1 ELSE 0
    END AS is_checkout_step_viewed_session,
    -- boolean field to indicate whether a session includes a  'email_sign_up' event. 
    CASE WHEN COUNTIF(event_name = 'email_sign_up') OVER(partition by session_id) >= 1
    THEN 1 ELSE 0
    END AS is_email_sign_up_session,
    -- boolean field to indicate whether a session includes a  'order_completed' event. 
    CASE WHEN COUNTIF(event_name = 'order_completed') OVER(partition by session_id) >= 1
    THEN 1 ELSE 0
    END AS is_order_completed_order,
    -- web session start timestamp
    FIRST_VALUE(timestamp IGNORE NULLS) over (partition by session_id order by timestamp) as web_session_start_timestamp,
    --first event URL of a session
    FIRST_VALUE(event_url IGNORE NULLS) over (partition by session_id order by timestamp) as session_landing_page_url,
    -- first `utm_medium` of a session
    FIRST_VALUE(utm_medium IGNORE NULLS) over (partition by session_id order by timestamp) as session_first_utm_medium,
    -- - first `utm_source` of a session
    FIRST_VALUE(utm_source IGNORE NULLS) over (partition by session_id order by timestamp) as session_first_utm_source,
    -- first `utm_campaign` of a session
    FIRST_VALUE(utm_campaign IGNORE NULLS) over (partition by session_id order by timestamp) as session_first_utm_campaign,

    etl_loaded_at as previous_sync,
    current_timestamp as etl_loaded_at

    FROM {{ref('stg_web_events_unique_session_id')}} b


{% if is_incremental() %}
where b.etl_loaded_at >= (select max(etl_loaded_at) from {{this}} )
{% endif %}


