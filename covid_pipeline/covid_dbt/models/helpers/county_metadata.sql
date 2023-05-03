select *
from {{ source('metadata', 'county_names')}}