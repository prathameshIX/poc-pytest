import pytest
from transformation import perform_transformations
from pyspark.sql import SparkSession
from io import StringIO
import requests

@pytest.fixture
def spark():
    return SparkSession.builder.appName("pytest_example").getOrCreate()

def fetch_data_from_github(url):
    response = requests.get(url)
    if response.status_code == 200:
        return StringIO(response.text)
    else:
        raise ValueError(f"Failed to fetch data from {url}")

@pytest.fixture
def sample_data(spark):
    country_data_path = "https://raw.githubusercontent.com/prathameshIX/poc-pytest/sandbox/sample_data/countyData.csv"
    full_data_path = "https://raw.githubusercontent.com/prathameshIX/poc-pytest/sandbox/sample_data/fullData.csv"

    try:
        country_data = fetch_data_from_github(country_data_path)
        full_data = fetch_data_from_github(full_data_path)
    except ValueError as e:
        pytest.fail(f"Error fetching data from GitHub: {e}")

    yield country_data, full_data

def test_transformations_with_tables(spark, sample_data):
    country_data, full_data = sample_data
    result_df = perform_transformations(spark, country_data, full_data)
    assert result_df.count() > 0

def test_sql_transformations_with_tables(spark, sample_data):
    country_data, full_data = sample_data
    result_df = perform_transformations(spark, country_data, full_data)
    result_df.createOrReplaceTempView("test_table")
    sql_result = spark.sql("SELECT * FROM test_table WHERE total_column1 > 0")
    assert sql_result.count() > 0
