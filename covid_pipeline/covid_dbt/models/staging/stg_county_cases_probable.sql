select *
from {{ source('dbt', 'raw_county_cases_probable') }}
where county is not null
and county in (select county from covid.mart.dim_county_names)