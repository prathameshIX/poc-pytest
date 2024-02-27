import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from io import StringIO
import tempfile  # Added import for tempfile

def save_stringio_to_temp_file(stringio_obj):
    """Save the content of a StringIO object to a temporary file."""
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv') as temp_file:
        temp_file.write(stringio_obj.getvalue())
        return temp_file.name

def cleanup_temp_files(*file_paths):
    """Delete temporary files."""
    for file_path in file_paths:
        if file_path:
            os.remove(file_path)

def perform_transformations(spark, data1, data2):
    # Save the content of StringIO objects to temporary files
    temp_path1 = save_stringio_to_temp_file(data1)
    temp_path2 = save_stringio_to_temp_file(data2)

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
        cleanup_temp_files(temp_path1, temp_path2)
