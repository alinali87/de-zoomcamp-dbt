{{ config(materialized='table') }}

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    t.dispatching_base_num,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    t.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    t.sr_flag,
    t.affiliated_base_number
from {{ source('staging','fhv_tripdata') }} as t
inner join dim_zones as pickup_zone
on t.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on t.dropoff_locationid = dropoff_zone.locationid


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}