import csv
import os
import sys

# Spark imports

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
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
	df = spark.read.csv('fires.csv', header='true')
	df = df.select(df.columns[19:29] + df.columns[30:32] + df.columns[34:36])
	df = df.na.drop()
	df = df.withColumn('Duration', ( df['CONT_DOY'] - df['DISCOVERY_DOY'] +1 ) )
	df = df.drop("FIRE_YEAR","DISCOVERY_DATE","DISCOVERY_DOY","DISCOVERY_TIME","CONT_DATE",'CONT_TIME','STAT_CAUSE_DESCR','CONT_DAY','CONT_DOY')
	df = df.withColumn("STAT_CAUSE_CODE", df.STAT_CAUSE_CODE.cast(IntegerType()))
	df = df.withColumn("FIRE_SIZE", df.FIRE_SIZE.cast(IntegerType()))
	df = df.withColumn("LATITUDE", df.LATITUDE.cast(FloatType()))
	df = df.withColumn("LONGITUDE", df.LONGITUDE.cast(FloatType()))
	df = df.withColumn("COUNTY", df.COUNTY.cast(IntegerType()))
	df = df.withColumn("Duration", df.Duration.cast(IntegerType()))
	categoricalColumns = ['STATE']
	stages = []
	for categoricalCol in categoricalColumns:
		stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
		encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
		stages += [stringIndexer, encoder]

	cols = df.columns
	label_stringIdx = StringIndexer(inputCol = 'FIRE_SIZE', outputCol = 'label').setHandleInvalid("keep")
	stages += [label_stringIdx]
	numericCols = ['STAT_CAUSE_CODE','LATITUDE','LONGITUDE','COUNTY','Duration']
	assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
	assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
	stages += [assembler]
	pipeline = Pipeline(stages = stages)
	df = df.na.drop()
	pipelineModel = pipeline.fit(df)
	df = pipelineModel.transform(df)
	selectedCols = ['label', 'features'] + cols
	df = df.select(selectedCols)
	return df
	
def decisionTree():
	df = preprocessing()
	train, test = df.randomSplit([0.8, 0.2], seed = 2018)
	rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')
	rfModel = rf.fit(train)
	predictions = rfModel.transform(test)
	evaluator = BinaryClassificationEvaluator()
	print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

decisionTree()
