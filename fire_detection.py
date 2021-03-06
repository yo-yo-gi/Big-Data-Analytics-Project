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
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader
from pyspark.sql.functions import isnan, when, count, col, isnull
from sklearn.cluster import KMeans
import seaborn as sns; sns.set()
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col


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
	
def under_sample(df):
	window = Window.partitionBy(df['OBJECTID']).orderBy(df['STAT_CAUSE_CODE'].desc())
	df = df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 150000)
	return df

def features_conversion(categorical_Columns, continuous_features, total_indexFeatures, df):
	cols = df.columns
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

	
def preprocessing(sp_df):
	df = sp_df
	df = under_sample(df)
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

	#Target variable that needs to be predicted 
	label = StringIndexer(inputCol = 'FIRE_SIZE', outputCol = 'label').setHandleInvalid("keep")
	total_indexFeatures += [label]
	
	#Extracting the continuos features in the dataset 
	continuous_features = ['STAT_CAUSE_CODE','LATITUDE','LONGITUDE','COUNTY','Duration']
	return features_conversion(categorical_Columns, continuous_features, total_indexFeatures, df)
	
	
def preprocessing_Fire_cause(sp_df):
	df = sp_df
	df = under_sample(df)
	
	causeSeries = df.groupby(df.STAT_CAUSE_DESCR).count().orderBy('count', ascending=False)
	stat = causeSeries.collect()
	x= [i for i in range(len(stat))]
	description = [i[0] for i in stat]
	plt.bar(x, [i[1] for i in stat], alpha=0.5)
	plt.xticks(x, description)
	plt.savefig('CauseOfFire.png')
	
	
	#Taking the columns that are highly correlated between them 
	df = df.select(df.columns[19:20] + df.columns[21:22] + df.columns[26:27] + df.columns[23:24] + df.columns[29:32])
	
	#Drop the null and na values from dataset
	df = df.na.drop()
	
		
	#Converting all to respective types from String DataType
	df = df.withColumn('Duration', ( df['CONT_DOY'] - df['DISCOVERY_DOY'] +1 ) )
	df = df.withColumn("STAT_CAUSE_CODE", df.STAT_CAUSE_CODE.cast(IntegerType()))
	df = df.withColumn("FIRE_YEAR", df.FIRE_YEAR.cast(IntegerType()))
	df = df.withColumn("LATITUDE", df.LATITUDE.cast(FloatType()))
	df = df.withColumn("LONGITUDE", df.LONGITUDE.cast(FloatType()))
	df = df.withColumn("Duration", df.Duration.cast(IntegerType()))
	categorical_Columns = ['FIRE_SIZE_CLASS']
	total_indexFeatures = []
	
	#Converting the Categororical variable from category to one hot encoding 
	for categ_col in categorical_Columns:
		catIndex = StringIndexer(inputCol = categ_col, outputCol = categ_col + 'Index')
		catEn = OneHotEncoderEstimator(inputCols=[catIndex.getOutputCol()], outputCols=[categ_col + "classVec"])
		total_indexFeatures += [catIndex, catEn]
		
	#Creating the Correlation Imgae between the features and saving	
	numeric_data = df.select('LATITUDE','LONGITUDE','STAT_CAUSE_CODE', 'FIRE_YEAR','Duration').toPandas()
	corelationVariables(numeric_data)

	
	#Target variable that needs to be predicted 
	label = StringIndexer(inputCol = 'STAT_CAUSE_CODE', outputCol = 'label').setHandleInvalid("keep")
	total_indexFeatures += [label]
	
	#Extracting the continuos features in the dataset 
	continuous_features = ['LATITUDE','LONGITUDE']
	
	#combining continuos and category variables
	return features_conversion(categorical_Columns, continuous_features, total_indexFeatures, df)

	
	
def random_forest_classifier(df):
	#Spliting the data from training and test set with some seed
	train, test = df.randomSplit([0.8, 0.2], seed = 2018)
	
	#using the RandomForestClassifier 
	rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label', numTrees=20)
	rfModel = rf.fit(train)
	
	#Evaluation of Prediction label 
	predictions = rfModel.transform(test)
	
	#Calculating the 
	evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
	accuracy = evaluator.evaluate(predictions)
	print("Test Error = %g" % (1.0 - accuracy))

def fire_cause_prediction(sp_df):
	#Preprocessing the data
	df = preprocessing_Fire_cause(sp_df)
	random_forest_classifier(df)
	
def fire_size_prediction(sp_df):
	#Preprocessing the data
	df = preprocessing(sp_df)
	random_forest_classifier(df)
	

# Best K with 'LATITUDE', 'LONGITUDE'
def findBestK(sp_df):
	df = sp_df

	# Preprocessing the data
	df = df.select('OBJECTID', 'LATITUDE', 'LONGITUDE')
	df = df.withColumn("LATITUDE", df["LATITUDE"].cast(FloatType()))
	df = df.withColumn("LONGITUDE", df["LONGITUDE"].cast(FloatType()))
	df = df.na.drop()

	# Finding the number of clusters is the elbow method
	K_clusters = range(3, 10)
	kmeans = [KMeans(n_clusters=i) for i in K_clusters]
	X_axis = df.select('LATITUDE').toPandas()
	Y_axis = df.select('LONGITUDE').toPandas()
	score = [kmeans[i].fit(X_axis).score(Y_axis) for i in range(len(kmeans))]

	# Visualize k value
	plt.plot(K_clusters, score)
	plt.xlabel('Number of Clusters')
	plt.ylabel('Score')
	plt.title('Elbow Curve')
	plt.show()

#findBestK()

#Clustering using K-Means ('LATITUDE', 'LONGITUDE') and Assigning Clusters to our Data
def kmeans(sp_df):
	df = sp_df
	df = df.select('OBJECTID', 'LATITUDE', 'LONGITUDE')
	df = df.withColumn("LATITUDE", df["LATITUDE"].cast(FloatType()))
	df = df.withColumn("LONGITUDE", df["LONGITUDE"].cast(FloatType()))
	df = df.na.drop()
	X = df.toPandas()
	kmeans = KMeans(n_clusters=6, init='k-means++')
	kmeans.fit(X[X.columns[1:3]])
	X['cluster_label'] = kmeans.fit_predict(X[X.columns[1:3]])

	# Visualize the Results
	centers = kmeans.cluster_centers_
	labels = kmeans.predict(X[X.columns[1:3]])
	X.plot.scatter(x='LATITUDE', y='LONGITUDE', c=labels, s=50, cmap='viridis')
	plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.5)
	plt.show()


def data_conversion(df):
	df = df.withColumn('Duration', (df['CONT_DOY'] - df['DISCOVERY_DOY'] + 1))
	df = df.select('LATITUDE', 'LONGITUDE', "FIRE_SIZE", "Duration")
	df = df.withColumn("FIRE_SIZE", df.FIRE_SIZE.cast(IntegerType()))
	df = df.withColumn("LATITUDE", df.LATITUDE.cast(FloatType()))
	df = df.withColumn("LONGITUDE", df.LONGITUDE.cast(FloatType()))
	df = df.withColumn("Duration", df.Duration.cast(IntegerType()))
	df = df.na.drop()
	data = df.toPandas()
	return data

# Best K with 'LATITUDE', 'LONGITUDE', "FIRE_SIZE", "Duration"

def findBestK_2(sp_df):
	df = sp_df
	# Preprocessing the data
	data = data_conversion(df)

	# Fitting the data for further processing
	mms = MinMaxScaler()
	mms.fit(data)
	data_transformed = mms.transform(data)
	Sum_of_squared_distances = []
	K = range(1, 15)
	for k in K:
		km = KMeans(n_clusters=k)
		km = KMeans(n_clusters=k)
		km = km.fit(data_transformed)
		Sum_of_squared_distances.append(km.inertia_)

	# Visualize the Results
	plt.plot(K, Sum_of_squared_distances, 'bx-')
	plt.xlabel('k')
	plt.ylabel('Sum_of_squared_distances')
	plt.title('Elbow Method For Optimal k')
	plt.show()


#K-means with 'LATITUDE', 'LONGITUDE', "FIRE_SIZE", "Duration"

def kmeans_four_features(sp_df):
	df = sp_df
	# Preprocessing the data
	X = data_conversion(df)

	kmeans = KMeans(n_clusters=5, init='k-means++')
	kmeans.fit(X[X.columns[0:4]])
	X['cluster_label'] = kmeans.fit_predict(X[X.columns[0:4]])
	centers = kmeans.cluster_centers_
	labels = kmeans.predict(X[X.columns[0:4]])

	# Run PCA on the data and reduce the dimensions in pca_num_components dimensions
	reduced_data = PCA(n_components=2).fit_transform(X[X.columns[0:4]])
	results = pd.DataFrame(reduced_data,columns=['pca1','pca2'])

	#Visualize the Results
	sns.scatterplot(x="pca1", y="pca2", hue=X['cluster_label'], data=results)
	plt.title('K-means Clustering with 2 dimensions')
	plt.show()

if __name__ == "__main__":
	spark = init_spark()
	#Reading the csv file in spark dataframe
	sp_df = spark.read.csv('fires.csv', header='true')
	fire_size_prediction(sp_df)
	#fire_cause_prediction(sp_df)
	#kmeans(sp_df)
	#kmeans(sp_df)
	#findBestK_2(sp_df)
	#kmeans_four_features(sp_df)
