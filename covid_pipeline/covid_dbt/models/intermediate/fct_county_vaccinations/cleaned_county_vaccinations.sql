select
county_name as county,
to_date(date, 'YYYY-MM-DD') as date,
vaccine_doses_administered::int as vaccine_doses_administered,
people_vaccinated_with_at_least_one_dose::int as people_vaccinated_with_at_least_one_dose,
people_fully_vaccinated::int as people_fully_vaccinated,
people_vaccinated_with_at_least_one_booster_dose::int as people_vaccinated_with_at_least_one_booster_dose
from {{ref('cleaned_names_county_vaccinations')}}
where county_name is not null
and county_name in (select county from covid.mart.dim_county_names)
