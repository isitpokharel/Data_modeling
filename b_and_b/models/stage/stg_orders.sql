
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ 
    config(
        materialized='incremental',
        unique_key = 'id',
        incremental_strategy = 'merge'
    )
}}


with order_latest_record as (

    select *
    from {{source('raw','orders')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY _loaded_at DESC) = 1
)

select 
     {{ generate_id([
    'o._id',
    'o.created_at',
    'line_items.line_id',
    'line_items.product_id',
    'line_items.variant_id'
      ]) }}  as id, 

    _id                                                    as order_id, 
    _loaded_at,
    created_at,
    updated_at,
    current_timestamp()                                    as etl_loaded_at,
    subtotal,
    total,
    line_items.line_id,
    line_items.product_id,
    line_items.variant_id,
    line_items.price,
    line_items.quantity,
    line_items.line_total_discount
from order_latest_record o,
UNNEST(o.line_items) as line_items

-- in incremental load, only select rows from the source table that were loaded after
-- the last time dbt model was run to populate this table. 
{% if is_incremental() %}
where _loaded_at >= (select max(etl_loaded_at) from {{this}} )
{% endif %}
