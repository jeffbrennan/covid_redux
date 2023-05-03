with unpivot_test as ({{ dbt_utils.unpivot(
        relation=ref('cleaned_names_county_cases'),
        cast_to='int',
        exclude=['county'],
        field_name='date',
        value_name='cases_daily'
    )
}})
select
    county,
    to_date(replace(date, 'value_', ''), 'MM_DD_YYYY') as date,
    cases_daily
from unpivot_test
