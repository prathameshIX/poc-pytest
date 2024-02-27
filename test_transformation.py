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

    country_df = spark.read.csv(country_data, header=True, inferSchema=True)
    full_df = spark.read.csv(full_data, header=True, inferSchema=True)
    country_df.createOrReplaceTempView("test_country_df")
    full_df.createOrReplaceTempView("test_full_df")

    yield country_df, full_df

def test_transformations_with_tables(spark, sample_data):
    country_df, full_df = sample_data
    result_df = perform_transformations("test_country_df", "test_full_df")
    assert result_df.count() > 0

def test_sql_transformations_with_tables(spark, sample_data):
    country_df, full_df = sample_data
    result_df = perform_transformations("test_country_df", "test_full_df")
    result_df.createOrReplaceTempView("test_table")
    sql_result = spark.sql("SELECT * FROM test_table WHERE total_column1 > 0")
    assert sql_result.count() > 0
