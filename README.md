# Big Data Analytics
# Project Proposal
- - - -
# Category - Dataset analysis
# Dataset: USA WildFires [https://www.kaggle.com/rtatman/188-million-us-wildfires]




# 1. Abstract

 In today’s world, wildfires have become one of the most serious natural calamities affecting millions of people worldwide every year. The ability to predict these disasters would not only help in curbing the impact but also give us a chance to improvise the prediction techniques. In this project, we will be aiming to analyze the data using Apache Spark and scikit-learn library on US wildfires applying different techniques on the dataset and predicting the outcome such as classifying the regions from most affected to the least affected. Secondly in determining how predictable the wildfires are concerning factors such as size and location, also visualization of the data and the factors impacting the outcome to understand the behavior of these natural occurrences. By implementing this project we hope to learn the process of using Big Data and Machine learning techniques.
 
# 2. Introduction
  ## a. Context ##
  Our project deals with the ability to predict wildfires. The data that we will be using has been provided by the US Government. The wildfire records were acquired from the reporting systems of federal, state, and local fire organizations, the core data elements of the records being discovery date, final fire size, and a point location. Data transformation has been applied so that it conforms to the data standards of the National Wildfire Coordinating Group (NWCG). Basic error-checking was performed to remove redundant records. The filtered data set  includes 1.88 million geo-referenced wildfire records, representing a total of 140 million acres burned during the 24-year period.
  ## b. Objectives ##
  * To determine if wildfires have become more or less frequent over time.
  * To identify what regions are the most and least prone to wildfires.
  * Given the size, location and date, predicting the cause of a fire wildfire.
 # 3. Materials and Methods
 ## a. The Dataset ##
  Fire occurrence database 4th edition represents the occurrence of wildfires in the United States from 1992 to 2015. This is the third update of a publication originally generated to support the national Fire Program Analysis (FPA) system. The wildfire records were acquired from the reporting systems of federal, state, and local fire organizations. The following core data elements were required for records to be included in this data publication: discovery date, final fire size, and a point location at least as precise as Public Land Survey System (PLSS) section (1-square mile grid). The data were transformed to conform, when possible, to the data standards of the National Wildfire Coordinating Group (NWCG). Basic error-checking was performed and redundant records were identified and removed, to the degree possible. The resulting product, referred to as the Fire Program Analysis fire-occurrence database (FPA FOD), includes 1.88 million geo-referenced wildfire records, representing a total of 140 million acres burned during the 24-year period.

Some of the important tables, columns and their significance is explained below:

* Fires: Table including wildfire data for the period of 1992-2015 compiled from US federal, state, and local reporting systems.
* FIRE_CODE = Code used within the interagency wildland fire community to track and compile cost information for emergency fire suppression (https://www.firecode.gov/).
* FIRE_NAME = Name of the incident, from the fire report (primary) or ICS-209 report (secondary).
* FIRE_YEAR = Calendar year in which the fire was discovered or confirmed to exist.
* DISCOVERY_DATE = Date on which the fire was discovered or confirmed to exist.
* DISCOVERY_DOY = Day of year on which the fire was discovered or confirmed to exist.
* DISCOVERY_TIME = Time of day that the fire was discovered or confirmed to exist.
* STAT_CAUSE_DESCR = Description of the (statistical) cause of the fire.
* FIRE_SIZE = Estimate of acres within the final perimeter of the fire.
* LATITUDE = Latitude (NAD83) for point location of the fire (decimal degrees).
* LONGITUDE = Longitude (NAD83) for point location of the fire (decimal degrees).
* STATE = Two-letter alphabetic code for the state in which the fire burned (or originated), based on the nominal designation in the fire report.
* COUNTY = County, or equivalent, in which the fire burned (or originated), based on nominal designation in the fire report.
* GeographicArea = Two-letter code for the geographic area in which the unit is located 
* Country = Country in which the unit is located (e.g. US = United States).
* State = Two-letter code for the state in which the unit is located (or primarily affiliated).

 The dataset is being taken from kaggle which is mostly cleaned data . The dataset may be containing some missing values that need to be preprocessed. The dataset consists of features which need  some filtering based upon the correlation of the features with the result value. We are looking for one more dataset that can be included which is from the UCI repository which has information regarding meteorological data that will help to get more insights and information.
 
  ## b. Technologies and Algorithms ##
  Spark and scikit learn will be primarily used for dataset analysis. Initially, we are planning to analyze the months, days and time for the occurrence of these fires to determine the most fire prone seasons. We will then perform visualization using matplotlib to generate histograms and heat maps of wildfire areas.

We are planning to use KNN, decision tree and random forest algorithms to process the data and make accurate predictions. Different models will be proposed and tuned, the one with the best results will be included and published in our report.

 # 4. Conclusion
In this project, we will use data visualization techniques to explore data on forest fire occurrence. We will work through identifying variables related to forest fire severity as a premise for discovering why, where and when a forest fire might occur.
 # 5. Related Work
 * Analysis of Machine Learning Methods for Wildfire Security Monitoring with an Unmanned Aerial Vehicles
 * Machine learning to predict final fire size at the time of ignition-International Journal of Wildland    Fire(https://www.publish.csiro.au/WF/WF19023)-Dmitriy Alexandrov, Elizaveta Pertseva, Ivan Berman, Igor Pantiukhin, Aleksandr Kapitonov 
 * Data-driven Forest Fire analysis, Jerry Gao, Kshama Shalini, Navit Gaur, Xuan Guan


 
   
