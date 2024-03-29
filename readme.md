# COVID Redux

Modernizing the ETL of my [largest project]("https://github.com/jeffbrennan/TexasPandemics")

<img src="./images/asset_diagram.svg">

## Goals

- Transition all csv/xlsx output to database tables
- Transition manual diagnostic checking & etl to dbt+dagster
- Only clean and write updates for newest data

## Tools

- ### Local
  - Python
  - Docker
  - Postgres
  - Dagster

- ### Cloud (soon)
  - AWS 
  - PowerBI

## Next steps
- [x] Vitals data loaded and cleaned
- [x] Vaccination data loaded and cleaned
- [x] Dbt models reorganized
- [ ] Tests added to all scripts
- [ ] Weekly run set up 
- [ ] Migrate local parquet storage to aws s3
- [ ] Migrate local postgres to aws rds
- [ ] Set up config to run on ECS cluster