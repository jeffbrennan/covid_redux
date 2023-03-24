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
- SQLite

- ### Cloud (TBD)
    - Snowflake
    - AWS 
- PowerBI

## Next steps

1. Implement all scripts as airflow dags
2. Do sample run
3. Incorporate additional data sources (hospitalizations, vaccination demographics etc.)
4. Move db from SQLite -> Postgres
5. Move db from Postgres -> AWS