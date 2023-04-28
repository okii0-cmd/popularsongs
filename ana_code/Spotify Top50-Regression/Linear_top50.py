#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder.appName("Regression").getOrCreate()

# Load audio features and weeks data
#audio_features = spark.read.csv("/Users/macbookpro/Desktop/Big Data Project/Data/cleaned_out_new_header.csv", header=True, inferSchema=True)
#weeks = spark.read.csv("/Users/macbookpro/Desktop/Big Data Project/Profiling/getSongCount/song.csv", header=True, inferSchema=True)

#Specify file name directly when running on dataproc pySpark shell
audio_features = spark.read.csv("cleaned_out_new_header.csv", header=True, inferSchema=True)
weeks = spark.read.csv("song.csv", header=True, inferSchema=True)

# Join the two DataFrames on song_name column
data = audio_features.join(weeks, "song_name")

# Assemble the selected columns into a feature vector
assembler = VectorAssembler(inputCols=["danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration"], outputCol="features")
data_with_features = assembler.transform(data)

# Split the data into training and testing sets
(trainingData, testData) = data_with_features.randomSplit([0.7, 0.3])

# Create a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="weeks_on_top_chart",fitIntercept=True, maxIter = 10000)

# Train the model
model = lr.fit(trainingData)

# Make predictions on the test data
predictions = model.transform(testData)

# Print coefficients for intepretation
coefficients = model.coefficients
print(coefficients)

# Evaluate the model
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(labelCol="weeks_on_top_chart", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print("Root Mean Squared Error (RMSE) = %g" % rmse)

summary = model.summary
pValues = summary.pValues
print("P-values:", pValues)
