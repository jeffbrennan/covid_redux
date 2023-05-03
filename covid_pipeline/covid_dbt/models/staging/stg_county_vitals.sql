with
    county_cases  as (
        select
            county,
            date,
            cases_daily
        from {{ref('unpivot_county_cases')}}
    ),
    county_deaths as (
        select
            county,
            date,
            deaths_daily
        from {{ref('unpivot_county_deaths')}}
    )
select
    c.county,
    c.date,
    c.cases_daily,
    d.deaths_daily
from county_cases c
full join county_deaths d
    on c.county = d.county
    and c.date = d.date
order by c.county, c.date
