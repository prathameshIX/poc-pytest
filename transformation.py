from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from io import StringIO, BytesIO
import requests
import tempfile

def perform_transformations(spark, data1, data2):
    # Save the content of StringIO objects to temporary files
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv') as temp_file1:
        temp_file1.write(data1.getvalue())
        temp_path1 = temp_file1.name

    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv') as temp_file2:
        temp_file2.write(data2.getvalue())
        temp_path2 = temp_file2.name

    try:
        # Read CSV data into DataFrames
        df1 = spark.read.csv(temp_path1, header=True, inferSchema=True)
        df2 = spark.read.csv(temp_path2, header=True, inferSchema=True)

        # Perform transformations
        joined_df = df1.join(df2, on='countyCode')
        grouped_df = joined_df.groupBy('countyCode').agg({'column1': 'sum', 'column2': 'avg'})
        narrowed_df = grouped_df.select('countyCode', col('sum(column1)').alias('total_column1'), col('avg(column2)').alias('average_column2'))
    
        return narrowed_df

    finally:
        # Clean up temporary files
        if temp_path1:
            os.remove(temp_path1)
        if temp_path2:
            os.remove(temp_path2)
