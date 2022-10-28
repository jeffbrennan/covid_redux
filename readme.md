# COVID Redux

Modernizing the ETL of my [largest project]("https://github.com/jeffbrennan/TexasPandemics")

## Goals

- Transition all csv/xlsx output to database tables
- Transition manual diagnostic checking & etl to airflow
- Only clean and write updates for newest data

## Tools

- ### Local
- Python
- Docker
- Airflow
- Postgres


- ### Cloud (TBD)
    - Snowflake
    - AWS 
- PowerBI

## TODOs

https://www.youtube.com/watch?v=S1eapG6gjLU


## Roadmap
- [ ] Perform initial load of final tables using existing COVID repo

- [ ] Ingest raw data into staging tables

- [ ] Split and translate existing covid-scraping.rmd script into multiple separate python files to clean raw data

- [ ] Create distinct diagnostic scripts that will validate cleaned output

- [ ] Send to statistical analysis code

- [ ] Validate analysis results
 
- [ ] Write final tables to database

- [ ] Create sample visualization in PowerBI 
