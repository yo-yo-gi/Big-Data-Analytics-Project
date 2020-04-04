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
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# Dask imports

import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader
from pyspark.sql.functions import isnan, when, count, col, isnull

# Kmeans imports

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import seaborn as sns; sns.set()
import csv


#Initialize a spark session.

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

	
def corelationVariables(feature_correlation):

	#Getting the correlation using pearson method
	corr = feature_correlation.corr(method="pearson")
	fig = plt.figure()
	ax = fig.add_subplot(111)
	
	#Color the graphs with appropriate columns
	cax = ax.matshow(corr,cmap='coolwarm', vmin=-1, vmax=1)
	fig.colorbar(cax)
	
	#Graphs with a ticks with difference of 1
	ticks = np.arange(0,len(feature_correlation.columns),1)
	ax.set_xticks(ticks)	
	plt.xticks(rotation=90)
	ax.set_yticks(ticks)
	
	#X and Y labels containing selected columns
	ax.set_xticklabels(feature_correlation.columns)
	ax.set_yticklabels(feature_correlation.columns)
	
	#Saving the image of the correlation matrix
	plt.savefig('correlation.png')
	
def preprocessing():
	spark = init_spark()
	
	#Reading the csv file in spark dataframe
	df = spark.read.csv('fires.csv', header='true')
	
	#Taking the columns that are highly correlated between them 
	df = df.select(df.columns[19:29] + df.columns[30:32] + df.columns[34:36])
	
	#Drop the null and na values from dataset
	df = df.na.drop()
	
	#Using the Duration day and containment day calculating the Creating Duration feature
	df = df.withColumn('Duration', ( df['CONT_DOY'] - df['DISCOVERY_DOY'] +1 ) )
	df = df.drop("FIRE_YEAR","DISCOVERY_DATE","DISCOVERY_DOY","DISCOVERY_TIME","CONT_DATE",'CONT_TIME','STAT_CAUSE_DESCR','CONT_DAY','CONT_DOY')
	
	#Converting all to respective types from String DataType
	df = df.withColumn("STAT_CAUSE_CODE", df.STAT_CAUSE_CODE.cast(IntegerType()))
	df = df.withColumn("FIRE_SIZE", df.FIRE_SIZE.cast(IntegerType()))
	df = df.withColumn("LATITUDE", df.LATITUDE.cast(FloatType()))
	df = df.withColumn("LONGITUDE", df.LONGITUDE.cast(FloatType()))
	df = df.withColumn("COUNTY", df.COUNTY.cast(IntegerType()))
	df = df.withColumn("Duration", df.Duration.cast(IntegerType()))
	categorical_Columns = ['STATE']
	total_indexFeatures = []
	
	#Converting the Categororical variable from category to one hot encoding 
	for categ_col in categorical_Columns:
		catIndex = StringIndexer(inputCol = categ_col, outputCol = categ_col + 'Index')
		catEn = OneHotEncoderEstimator(inputCols=[catIndex.getOutputCol()], outputCols=[categ_col + "classVec"])
		total_indexFeatures += [catIndex, catEn]
		
	#Creating the Correlation Imgae between the features and saving	
	numeric_data = df.select('STAT_CAUSE_CODE','LATITUDE','LONGITUDE','COUNTY','Duration').toPandas()
	corelationVariables(numeric_data)

	cols = df.columns
	
	#Target variable that needs to be predicted 
	label = StringIndexer(inputCol = 'FIRE_SIZE', outputCol = 'label').setHandleInvalid("keep")
	total_indexFeatures += [label]
	
	#Extracting the continuos features in the dataset 
	continuous_features = ['STAT_CAUSE_CODE','LATITUDE','LONGITUDE','COUNTY','Duration']
	
	#combining continuos and category variables
	totalFeatures = [c + "classVec" for c in categorical_Columns] + continuous_features
	tFeatures = VectorAssembler(inputCols=totalFeatures, outputCol="features")
	total_indexFeatures += [tFeatures]
	
	#Creating the pipeling for the transform 
	pipeline = Pipeline(stages = total_indexFeatures)
	df = df.na.drop()
	pipelineModel = pipeline.fit(df)
	df = pipelineModel.transform(df)
	
	#Only selected columns are used from the dataset
	selectedCols = ['label', 'features'] + cols
	df = df.select(selectedCols)
	return df
	
def decisionTree():
	#Preprocessing the data
	df = preprocessing()
	
	#Spliting the data from training and test set with some seed
	train, test = df.randomSplit([0.8, 0.2], seed = 2018)
	
	#using the RandomForestClassifier 
	rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')
	rfModel = rf.fit(train)
	
	#Evaluation of Prediction label 
	predictions = rfModel.transform(test)
	
	#Calculating the 
	evaluator = BinaryClassificationEvaluator()
	print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

decisionTree()


def findBestK():
	spark = init_spark()
	df = spark.read.csv('fires.csv', header='true')
	df = df.select('OBJECTID', 'LATITUDE', 'LONGITUDE')
	df = df.withColumn("LATITUDE", df["LATITUDE"].cast(FloatType()))
	df = df.withColumn("LONGITUDE", df["LONGITUDE"].cast(FloatType()))
	df = df.na.drop()
	K_clusters = range(3, 10)
	kmeans = [KMeans(n_clusters=i) for i in K_clusters]
	X_axis = df.select('LATITUDE').toPandas()
	Y_axis = df.select('LONGITUDE').toPandas()
	score = [kmeans[i].fit(X_axis).score(Y_axis) for i in range(len(kmeans))]
	plt.plot(K_clusters, score)
	plt.xlabel('Number of Clusters')
	plt.ylabel('Score')
	plt.title('Elbow Curve')
	plt.show()


findBestK()


def kmeans():
	spark = init_spark()
	df = spark.read.csv('fires.csv', header='true')
	df = df.select('OBJECTID', 'LATITUDE', 'LONGITUDE')
	df = df.withColumn("LATITUDE", df["LATITUDE"].cast(FloatType()))
	df = df.withColumn("LONGITUDE", df["LONGITUDE"].cast(FloatType()))
	df = df.na.drop()
	X = df.toPandas()
	kmeans = KMeans(n_clusters=6, init='k-means++')
	kmeans.fit(X[X.columns[1:3]])
	X['cluster_label'] = kmeans.fit_predict(X[X.columns[1:3]])
	centers = kmeans.cluster_centers_
	labels = kmeans.predict(X[X.columns[1:3]])
	X.plot.scatter(x='LATITUDE', y='LONGITUDE', c=labels, s=50, cmap='viridis')
	plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.5)
	plt.show()



kmeans()