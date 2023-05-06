with
    base_df as (
        select
            a.county,
            date,
            cases_daily,
            tsa,
            phr,
            metro_area,
            population
        from {{ref('fct_county_cases')}} a
        left join {{ref('dim_county_names')}} b
        on a.county = b.county
            left join {{ref ('dim_county_populations')}} c
            on a.county = c.county
            and a.date >= c.start_date
            and (a.date <= c.end_date or c.end_date is null)
        where case_type = 'confirmed'
    )
select
    'County' as level_type,
    county   as level,
    date,
    cases_daily,
    population
from base_df
union
select
    'TSA' as level_type,
    tsa   as level,
    date,
    cases_daily,
    population
from base_df
union
select
    'PHR' as level_type,
    phr   as level,
    date,
    cases_daily,
    population
from base_df
union
select
    'Metro Area' as level_type,
    metro_area   as level,
    date,
    cases_daily,
    population
from base_df
union
select
    'State'          as level_type,
    'Texas'          as level,
    date,
    sum(cases_daily) as cases_daily,
    max(population)  as population
from base_df
group by date
