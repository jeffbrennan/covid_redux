select
county,
date,
case_type,
cases_daily
from {{ref('unpivot_county_cases_confirmed')}}
union
select
county,
date,
case_type,
cases_daily
from {{ref('unpivot_county_cases_probable')}}