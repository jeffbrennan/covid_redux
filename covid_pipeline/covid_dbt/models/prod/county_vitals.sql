with
    corrected_values as (
        select
            county
          , date
          , case when cases_daily < 0 then 0 else cases_daily end   as cases_daily
          , case when deaths_daily < 0 then 0 else deaths_daily end as deaths_daily
        from {{ref('stg_county_vitals')}}
    ),
    rolling_amounts  as (
        select
            county,
            date,
            cases_daily,
            sum(cases_daily) over (partition by county order by date asc)  as cases_cumsum,
            deaths_daily,
            sum(deaths_daily) over (partition by county order by date asc) as deaths_cumsum
        from corrected_values
    )
select
    county,
    date,
    cases_daily,
    cases_cumsum,
    deaths_daily,
    deaths_cumsum
from rolling_amounts

--
-- select
--     county,
--     to_date(date, 'MM/DD/YYYY') as date,
--     cases_daily                 as cases_daily_orig,
--     cases_daily::int            as cases_daily,
--     cases_daily::int            as deaths_daily
-- from covid.dbt.unpivot_county_cases
-- where county = 'Harris'
-- order by date desc
--
-- select * from covid.dbt.stg_county_cases
-- where county = 'Harris'