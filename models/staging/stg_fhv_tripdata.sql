{{ config(materialized='view') }}

select
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    PUlocationID as pickup_locationid,
    DOlocationID as dropoff_locationid,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from {{ source('staging','fhv_partitoned_clustered') }}


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}