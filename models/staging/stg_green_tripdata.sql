{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by vendor_id, pickup_datetime) as rn
  from {{ source('staging','green_taxi') }}
  where vendor_id is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("rate_code", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    {{ dbt.safe_cast("trip_distance", api.Column.translate_type("numeric")) }}  as trip_distance,
    {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,

    -- payment info
    {{ dbt.safe_cast("fare_amount", api.Column.translate_type("numeric")) }}  as fare_amount,
    {{ dbt.safe_cast("extra", api.Column.translate_type("numeric")) }}as extra,
    {{ dbt.safe_cast("mta_tax", api.Column.translate_type("numeric")) }} as mta_tax,
    {{ dbt.safe_cast("tip_amount", api.Column.translate_type("numeric")) }} as tip_amount,
    {{ dbt.safe_cast("tolls_amount", api.Column.translate_type("numeric")) }} as tolls_amount,
    {{ dbt.safe_cast("ehail_fee", api.Column.translate_type("numeric")) }} as ehail_fee,
    {{ dbt.safe_cast("imp_surcharge", api.Column.translate_type("numeric")) }} as improvement_surcharge,
    {{ dbt.safe_cast("total_amount", api.Column.translate_type("numeric")) }} as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description
from tripdata
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}