#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 16:30:39 2023

@author: macbookpro
"""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

#audio_features = pd.read_csv("/Users/macbookpro/Desktop/Big Data Project/Data/full_audio_features.csv")
#streams = pd.read_csv("/Users/macbookpro/Desktop/Big Data Project/Data/streams.csv")

#Specify file name directly when running on dataproc pySpark Shell

audio_features = pd.read_csv("full_audio_features.csv")
streams = pd.read_csv("streams.csv")

streams["streams"] = pd.to_numeric(streams["streams"])

# Join the two DataFrames on uri column
data = audio_features.merge(streams, on="uri")
data = data.dropna()
# Convert the column to a numeric value
data["streams"] = pd.to_numeric(data["streams"])
# Create a Spark session
spark = SparkSession.builder.appName("Regression").getOrCreate()

# Convert the Pandas DataFrame to a Spark DataFrame
data_spark = spark.createDataFrame(data)

# Assemble the selected columns into a feature vector
assembler = VectorAssembler(inputCols=["danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration_ms"], outputCol="features")
data_with_features = assembler.transform(data_spark)

# Split the data into training and testing sets
(train_data, test_data) = data_with_features.randomSplit([0.8, 0.2], seed=1234)

# Fit a random forest regression model
rf = RandomForestRegressor(featuresCol="features", labelCol="streams", numTrees=400, seed=1234)
model = rf.fit(train_data)

importances = model.featureImportances
feature_names = assembler.getInputCols()
for i in range(len(importances)):
    print("Feature %s: %.4f" % (feature_names[i], importances[i]))
    
# Evaluate the model on the testing set
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="streams", predictionCol="prediction", metricName="r2")
r_squared = evaluator.evaluate(predictions)
print("R-squared on test data = %g" % r_squared)

