with
    corrected_values as (
        select
            county
          , date
          , case when cases_daily < 0 then 0 else cases_daily end   as cases_daily
          , case when deaths_cumsum < 0 then 0 else deaths_cumsum end as deaths_cumsum
        from {{ref('combined_county_vitals')}}
    ),
    rolling_amounts  as (
        select
            county,
            date,
            cases_daily,
            sum(cases_daily) over (partition by county order by date asc)  as cases_cumsum,
            deaths_cumsum - lag(deaths_cumsum, 1, 0) over (partition by county order by date asc) as deaths_daily,
            deaths_cumsum
        from corrected_values
    ),
    existing_amounts as (
        select
            county,
            date,
            cases_daily,
            cases_cumsum,
            deaths_daily,
            deaths_cumsum
        from covid.mart.fct_county_vitals
    )

select
    county,
    date,
    cases_daily,
    cases_cumsum,
    deaths_daily,
    deaths_cumsum
from rolling_amounts
where date > (select max(date) from existing_amounts)
union
select
    county,
    date,
    cases_daily,
    cases_cumsum,
    deaths_daily,
    deaths_cumsum
from existing_amounts