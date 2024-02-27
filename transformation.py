from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def perform_transformations(file1_path, file2_path):
   spark = SparkSession.builder.appName("TransformationApp").getOrCreate()
   df1 = spark.read.csv(file1_path, header=True, inferSchema=True)
   df2 = spark.read.csv(file2_path, header=True, inferSchema=True)
   joined_df = df1.join(df2, on='countyCode')
   grouped_df = joined_df.groupBy('countyCode').agg({'column1': 'sum', 'column2': 'avg'})
   narrowed_df = grouped_df.select('countyCode', col('sum(column1)').alias('total_column1'), col('avg(column2)').alias('average_column2'))
   return narrowed_df

result_df = perform_transformations("countyData.csv", "fullData.csv")
result_df.show()
print("Dataframe Created")
