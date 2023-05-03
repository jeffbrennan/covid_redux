from setuptools import find_packages, setup

setup(
    name="covid_redux",
    packages=find_packages(),
    install_requires=[
        "grpcio==1.47.5",
        "dagster",
        "dagster-dbt",
        "dagster-postgres",
        "pandas",
        "dbt-core",
        "openpyxl",
        "pyarrow",
        "fastparquet"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
