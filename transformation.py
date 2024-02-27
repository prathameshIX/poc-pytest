from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from io import StringIO
import requests

def perform_transformations(spark, data1, data2):
    df1 = spark.read.csv(StringIO(data1), header=True, inferSchema=True)
    df2 = spark.read.csv(StringIO(data2), header=True, inferSchema=True)

    joined_df = df1.join(df2, on='countyCode')
    grouped_df = joined_df.groupBy('countyCode').agg({'column1': 'sum', 'column2': 'avg'})
    narrowed_df = grouped_df.select('countyCode', col('sum(column1)').alias('total_column1'), col('avg(column2)').alias('average_column2'))
    
    return narrowed_df
