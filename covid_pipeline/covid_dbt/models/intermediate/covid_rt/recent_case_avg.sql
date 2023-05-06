with
    recent_case_df as (
        select
            level_type,
            level,
            date,
            cases_daily
        from {{ref('stacked_cases')}}
        where date > CURRENT_DATE - INTERVAL '3 weeks'
    ),
    avg_df         as (
        select
            level_type,
            level,
            avg(cases_daily) as cases_daily_avg_3wk
        from recent_case_df
        group by level_type, level
    )

select *
from avg_df