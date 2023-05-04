with unpivot_cases as ({{ dbt_utils.unpivot(
        relation=ref('cleaned_names_county_cases_probable'),
        cast_to='int',
        exclude=['county'],
        field_name='date',
        value_name='cases_daily'
    )
}})
select
    county,
    to_date(replace(date, 'value_', ''), 'MM_DD_YYYY') as date,
    'probable' as case_type,
    cases_daily
from unpivot_cases
