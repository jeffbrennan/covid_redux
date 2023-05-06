from setuptools import find_packages, setup

setup(
    name="covid_redux",
    packages=find_packages(),
    install_requires=[

        # dagster
        "dagster",
        "dagster-dbt",
        "dagster-postgres",

        # dbt
        "dbt-core",


        "grpcio==1.47.5",

        #  file processing

        "pandas",
        "openpyxl",
        "pyarrow",
        "fastparquet",
        "polars>=0.17.12"
        "connectorx",

        # rt estimate
        "epyestim"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
