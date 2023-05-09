with
    new_counts      as (
        select
            county,
            date,
            vaccine_doses_administered,
            people_vaccinated_with_at_least_one_dose,
            people_fully_vaccinated,
            people_vaccinated_with_at_least_one_booster_dose
        from {{ref('cleaned_county_vaccinations')}}

    ),
    existing_counts as (
        select
            county,
            date,
            vaccine_doses_administered,
            people_vaccinated_with_at_least_one_dose,
            people_fully_vaccinated,
            people_vaccinated_with_at_least_one_booster_dose
        from covid.mart.fct_county_vaccinations
    )
select *
from new_counts
where date > (
    select
        max(date)
    from existing_counts
)
union
select *
from existing_counts