{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by int64_field_0, pickup_datetime) as rn
                        -- order by fare_amount, pulocationid, lpep_dropoff_datetime 
  from {{ source('staging','fhv_tripdata_2019') }}
  where int64_field_0 is not null 
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['int64_field_0', 'pulocationid']) }} as tripid,
    cast(int64_field_0 as integer) as int64_field_0,
    dispatching_base_num, 
    -- cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    sr_flag,
    affiliated_base_number

from tripdata
-- where rn = 1


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}