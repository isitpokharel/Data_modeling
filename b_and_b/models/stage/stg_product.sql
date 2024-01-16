{{ 
    config(
        materialized='incremental',
        unique_key = 'id',
        incremental_strategy = 'merge'
    )
}}





with product_latest_record as (

    select *
    from {{source('raw','products')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY _loaded_at DESC) = 1
)

select 
     {{ generate_id([
    'p._id',
    'variants.variant_id',
    'variants.sku'
    ]) }}  as id,
    
    p._id                                                  as product_id, 
    p._loaded_at,
    p.category,
    p.created_at,
    p.updated_at,
    p.title,
    variants.variant_id,
    variants.sku                                            as variant_sku,
    variants.title                                          as variant_title,
    variants.created_at                                     as variant_created_at,
    variants.updated_at                                     as variant_updated_at,
    variants.option1                                        as variant_option1,
    variants.option2                                        as variant_option2,
    current_timestamp()                                     as etl_loaded_at,

from product_latest_record p,
UNNEST(p.variants) as variants

-- in incremental load, only select rows from the source table that were loaded after
-- the last time dbt model was run to populate this table. 
{% if is_incremental() %}
where _loaded_at >= (select max(etl_loaded_at) from {{this}} )
{% endif %}
