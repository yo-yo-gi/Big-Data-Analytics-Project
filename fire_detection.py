import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader
from pyspark.sql.functions import isnan, when, count, col, isnull


#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark
	
def preprocessing():
	spark = init_spark()
	df = spark.read.csv('fires.csv')
	df = df.select(['_c19','_c20','_c21','_c22','_c23','_c24','_c25','_c26','_c27','_c28','_c29','_c30','_c31','_c34','_c35'])
	df = df.na.drop()
	df = df.withColumnRenamed("_c19", "Fire_year").withColumnRenamed("_c20", "Discover_Date").withColumnRenamed("_c21", "Discover_Day").withColumnRenamed("_c22", "Discover_Time").withColumnRenamed("_c23", "Cause").withColumnRenamed("_c24", "Description").withColumnRenamed("_c25", "Containment_Date").withColumnRenamed("_c26", "Containment_Day").withColumnRenamed("_c27", "Containment_Time").withColumnRenamed("_c28", "Fire_size").withColumnRenamed("_c29", "Fire_size_class").withColumnRenamed("_c30", "Latitute").withColumnRenamed("_c31", "Longitude").withColumnRenamed("_c34", "State").withColumnRenamed("_c35", "COUNTY")
	
	
preprocessing()