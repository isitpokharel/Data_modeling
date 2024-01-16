
### Please take a look at Proejct.pdf for overall description of the project. 


### Using the b_and_b project

Try running the following commands:
- move into b_and_b directory after cloing the repo
- dbt clean (just to be safe)
- dbt deps ( to build metric package)
- dbt run -- full-refresh

### To view documentation
- dbt docs generate
- dbt docs serve
(dbt docs will open in browser)



### INTRO TO MODEL


There are two stage models which analysts won't require - stg_orders.sql and stg_products.
Both models are set up as incremental models with merge incremental_stragey. 

#### stg_orders.sql
>  stg_orders.sql model only contains the latest record ( identified by the latest _loaded_at timestamp ) for each order_id in source orders table. To maintain uniqueness in each row (due to repeated record), i've created a new id field by concatentating order_id, created_at, line_id, product_id, and variant_id. This field ensures uniuqeness for each row in the table, and the same field is used as unique key value for incremental strategy. 

#### stg_product.sql
> stg_product.sql model only contains the latest record ( identified by the latest _loaded_at timestamp ) for each product_id in source product table. To maintain uniqueness in each row (due to repeated record), i've created a new id field by concatentating _id(product id), variant_id, and sku. This field ensures uniuqeness for each row in the table, and the same field is used as unique key value for incremental strategy. 

#### stg_web_events_unique_session_id.sql
> stg_web_events_unique_session_id.sql is also a staging model where some basic tranformations to source data table web_events is done to create unique session_ids for each event based on provided requirement 
    a ‘session’ is defined as a series of one or more web events committed by the same cookie with no more than a 30 minute gap between events. Any 30 minute gap indicates a new session.


### Presentation Tables

#### 1) product_order.sql
    obtained by performing left join stg_product on stg_orders table using product_id and variant_id. Some data transformations are done fulfill analysts's requriements described in the assignment. 

#### 2) web_session_final.sql
    This model is based on stg_web_events_unique_session_id.sql and performs few transformations to fulfill analysts's requriements described in the assignment. 

## documentation for metrics are provided using dbt metrics 
   All metrics included in part "Need the ability to report on the following metrics" 
   are included in metrics.yml file in models->final

## AD Hoc Queries (Challenge: Part 2)
    i've provided a a sql file to answer each one of the seven questions asked in Part 2 of the assessment. all sql files are present in analyses folder
    - dbt compile
    - after successful compilation all sql queries will availabe in target->compiled->b_and_b->analyses ( these queries can be copied and pasted in bigquery). Please make sure you've already built all the required tables in bigquery first. My sql queries use the final presental tables product_order, web_session_final to answer the questions in part 2. 

## Data Quality 
    I did encounter two issues while doing the other parts of the project. 
    customer_id in web_events table has 'NaN' values instead of Null which might have led to inaccurate data when calculating "Total Web Users". i've addressed the issue in stg_web_events_unique_session_id model. 
    Another issue was unnecessary ( might not be that important), but cookie_id field in web_events table had a lot of '\\' abd '"' values making the data seem like error values. I was able to address it by using REGEXP_REPLACE in stg_web_events_unique_session_id



## Ad Hoc Queries are present in analyses folder. 
