select *
from {{ source('dbt', 'county_deaths_raw') }}
where county is not null
and county in (select county from covid.mart.dim_county_names)