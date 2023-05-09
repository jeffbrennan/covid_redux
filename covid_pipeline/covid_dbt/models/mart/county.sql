with
    county_base            as (
        select
            county
        from {{ref('dim_county_names')}}
    ),
    county_cases_confirmed as (
        select
            county,
            date,
            cases_daily as confirmed_cases_daily,
            cases_cumsum as confirmed_cases_cumsum
        from {{ref('fct_county_cases')}}
        where case_type = 'confirmed'
    ),
    county_cases_probable  as (
        select
            county,
            date,
            cases_daily as confirmed_plus_probable_cases_daily,
            cases_cumsum as confirmed_plus_probable_cases_cumsum
        from {{ref('fct_county_cases')}}
        where case_type = 'confirmed_plus_probable'
    ),
    county_cases_deaths    as (
        select
            county,
            date,
            deaths_daily,
            deaths_cumsum
        from {{ref('fct_county_deaths')}}
    ),
    rt                     as (
        select
            level as county,
            date,
            cases_ma_7day
        from {{ref('fct_rt')}}
        where level_type = 'County'
    ),
    vaccinations           as (
        select
            county,
            date,
            vaccine_doses_administered,
            people_vaccinated_with_at_least_one_dose,
            people_fully_vaccinated,
            people_vaccinated_with_at_least_one_booster_dose
        from {{ref('fct_county_vaccinations')}}
    )
select
    county_base.county,
    county_cases_confirmed.date,
    county_cases_confirmed.confirmed_cases_daily,
    county_cases_confirmed.confirmed_cases_cumsum,
    county_cases_probable.confirmed_plus_probable_cases_daily,
    county_cases_probable.confirmed_plus_probable_cases_cumsum,
    county_cases_probable.confirmed_plus_probable_cases_daily - county_cases_confirmed.confirmed_cases_daily as probable_cases_daily,
    county_cases_probable.confirmed_plus_probable_cases_cumsum - county_cases_confirmed.confirmed_cases_cumsum as probable_cases_cumsum,
    county_cases_deaths.deaths_daily,
    county_cases_deaths.deaths_cumsum,
    rt.cases_ma_7day,
    vaccinations.vaccine_doses_administered,
    vaccinations.people_vaccinated_with_at_least_one_dose,
    vaccinations.people_fully_vaccinated,
    vaccinations.people_vaccinated_with_at_least_one_booster_dose
from county_base
left join county_cases_confirmed
    on county_base.county = county_cases_confirmed.county
full join county_cases_probable
    on county_base.county = county_cases_probable.county
    and county_cases_confirmed.date = county_cases_probable.date
full join county_cases_deaths
    on county_base.county = county_cases_deaths.county
    and county_cases_confirmed.date = county_cases_deaths.date
full join rt
    on county_base.county = rt.county
    and county_cases_confirmed.date = rt.date
full join vaccinations
    on county_base.county = vaccinations.county
    and county_cases_confirmed.date = vaccinations.date
