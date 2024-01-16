
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
    select
    -- window function with sum to do cumulative sum as the id, concatentate to cookie_id to create a unique session id. 
    concat(cookie_id || '-' || cast(sum(new_event_boundary) over (partition by cookie_id order by timestamp) as string)) as session_id,
    base.* EXCEPT (new_event_boundary),
    current_timestamp() as etl_loaded_at
    from (select
            _id as event_id,
            _loaded_at,
            -- a lot of cookie ids have unnecessary \ and " characters. Using regex to remove these characeters from cookie id. 
            REGEXP_REPLACE(cookie_id, r'\\|"', '') as cookie_id,
            -- some values of customer_id are NaN - replacing these values with NULL
            NULLIF(customer_id,'NaN') as customer_id,
            event_name,
            event_url,
            event_properties,
            timestamp,
            utm_campaign,
            utm_medium,
            utm_source,
            -- window function to indicate whether an event falls into same seession as previous event for a given cookie_id. 
            -- if an event event has timestamp > 30 minutes after the timestamp of preceeding event_id for same cookie, then the new event_id is categorized as a separate session. 
            CASE WHEN (UNIX_SECONDS(timestamp) - lag(UNIX_SECONDS(timestamp)) OVER(PARTITION BY cookie_id order by timestamp)) > 30*60 
                THEN 1 ELSE 0
            END AS  new_event_boundary
        from {{source('raw','web_events')}}
    ) base 

{% if is_incremental() %}
where _loaded_at >= (select max(etl_loaded_at) from {{this}} )
{% endif %}


