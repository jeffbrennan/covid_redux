with unpivot_deaths as ({{ dbt_utils.unpivot(
        relation=ref('cleaned_names_county_deaths'),
        cast_to='int',
        exclude=['county'],
        field_name='date',
        value_name='deaths_cumsum'
    )
}})
select
    county,
    to_date(replace(date, 'value_', ''), 'MM_DD_YYYY') as date,
    deaths_cumsum
from unpivot_deaths
