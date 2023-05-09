select *
from {{ source('origin', 'raw_county_deaths') }}
where county is not null
and county in (select county from covid.mart.dim_county_names)