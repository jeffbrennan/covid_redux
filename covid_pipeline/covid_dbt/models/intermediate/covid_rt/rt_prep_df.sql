with
    rt_prep_df as (
        select
            level_type,
            level,
            date,
            coalesce(
                            avg(cases_daily) over
                        (
                        partition by level_type, level
                        order by date rows
                            between 7 preceding AND 1 preceding),
                            cases_daily
                ) as cases_ma_7day,
            population
        from {{ref('stacked_cases')}}
    )
select
    a.level_type,
    a.level,
    a.date,
    a.cases_ma_7day,
    a.population
from rt_prep_df a
inner join {{ref ('recent_case_avg')}} b
on a.level_type = b.level_type
    and a.level = b.level
    and b.cases_daily_avg_3wk > 5 -- TODO: make this a parameter