select *
from {{ source('dbt', 'county_cases_raw') }}
where county is not null
and county in (select county from covid.metadata.county_names)