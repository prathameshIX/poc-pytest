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

def perform_transformations(spark, data):
    # Read CSV data into a DataFrame
    df = spark.read.csv(data, header=True, inferSchema=True)

    # Perform transformations
    joined_df = df.groupBy('countyCode').agg({'PovertyEst': 'sum', 'avgAnnCount': 'avg'})
    narrowed_df = joined_df.select('countyCode', col('sum(PovertyEst)').alias('total_PovertyEst'), col('avg(avgAnnCount)').alias('average_avgAnnCount'))
    
    return narrowed_df


    finally:
        # Clean up temporary files
        cleanup_temp_files(temp_path1, temp_path2)
