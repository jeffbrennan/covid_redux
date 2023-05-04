with
    corrected_values as (
        select
            county
          , date
          , case_type
          , case when cases_daily < 0 then 0 else cases_daily end as cases_daily
        from {{ref('combine_county_cases')}}
    ),
    combined_values  as (
        select
            county
          , date
          , case_type
          , cases_daily
        from corrected_values
        union
        select
            county
          , date
          , 'confirmed_plus_probable' as case_type
          , sum(cases_daily)          as cases_daily
        from corrected_values
        group by county, date

    ),
    rolling_amounts  as (
        select
            county,
            date,
            case_type,
            cases_daily,
            sum(cases_daily) over (partition by county, case_type order by date asc) as cases_cumsum
        from combined_values
    ),
    existing_amounts as (
        select
            county,
            date,
            case_type,
            cases_daily,
            cases_cumsum
        from covid.mart.fct_county_cases
    )
select
    county,
    date,
    case_type,
    cases_daily,
    cases_cumsum
from rolling_amounts
where date > (
    select
        max(date)
    from existing_amounts
)
union
select
    county,
    date,
    case_type,
    cases_daily,
    cases_cumsum
from existing_amounts