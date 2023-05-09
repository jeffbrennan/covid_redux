with
    fct_county_cases  as (
        select
            max(date)          as last_updated,
            'fct_county_cases' as tbl_name
        from {{ref('fct_county_cases')}}
    ),
    fct_rt            as (
        select
            max(date) as last_updated,
            'fct_rt'  as tbl_name
        from {{ref('fct_rt')}}
    ),
    fct_county_deaths as (
        select
            max(date)           as last_updated,
            'fct_county_deaths' as tbl_name
        from {{ref('fct_county_deaths')}}
    ),
    fct_county_vaccinations as (
        select
            max(date) as last_updated,
            'fct_county_vaccinations' as tbl_name
        from {{ref('fct_county_vaccinations')}}
    )
select
    tbl_name,
    last_updated,
    CURRENT_DATE - last_updated as update_delta_days
from (
    select *
    from fct_county_cases
    union
    select *
    from fct_county_deaths
    union
    select *
    from fct_rt
    union
    select *
    from fct_county_vaccinations
) a
order by update_delta_days desc