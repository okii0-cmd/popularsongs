#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 16:30:39 2023

@author: macbookpro
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Create a Spark session
spark = SparkSession.builder.appName("Random Forest Regression").getOrCreate()

# Load the data into DataFrames, local 
audio_features = spark.read.csv("/Users/macbookpro/Desktop/Big Data Project/Data/Spotify 2022 Top 50 /cleaned_out_new_header.csv", header=True, inferSchema=True)
weeks = spark.read.csv("/Users/macbookpro/Desktop/Big Data Project/profiling_code//ti2060//getSongCount/song.csv", header=True, inferSchema=True)

#Specify file name directly when running on dataproc pySpark Shell
#audio_features = spark.read.csv("cleaned_out_new_header.csv", header=True, inferSchema=True)
#weeks = spark.read.csv("song.csv", header=True, inferSchema=True)


# Join the two DataFrames
data = audio_features.select("song_name", "danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration").join(weeks.select("song_name", "weeks_on_top_chart"), "song_name", "inner")

# Assemble the  columns into a feature vector
assembler = VectorAssembler(inputCols=["danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration"], outputCol="features")
data_with_features = assembler.transform(data)

# Split the data into training and testing sets 80,20 split
(train_data, test_data) = data_with_features.randomSplit([0.8, 0.2], seed=1234)

# Fit a random forest regression model with 20 trees
rf = RandomForestRegressor(featuresCol="features", labelCol="weeks_on_top_chart", numTrees=20, seed=1234)
model = rf.fit(train_data)

# Evaluate the model on the testing set
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="weeks_on_top_chart", predictionCol="prediction", metricName="r2")
r_squared = evaluator.evaluate(predictions)
print("R-squared on test data = %g" % r_squared)

#Print the importance of each feature when creating the trees
importances = model.featureImportances
feature_names = assembler.getInputCols()
for i in range(len(importances)):
    print("Feature %s: %.4f" % (feature_names[i], importances[i]))
    
single_tree = model.trees[0] 
tree_structure = single_tree.toDebugString

#Function to convert tree to readable form
def pyspark_tree_to_dot(tree_structure, feature_names):
    for i, feature_name in enumerate(feature_names):
        tree_structure = tree_structure.replace(f'feature {i}', feature_name)

    tree_structure = tree_structure.replace('(', '[').replace(')', ']')
    tree_structure = tree_structure.replace('if', '->').replace('else', '->')

    dot_format = "digraph Tree {\n"
    dot_format += "node [shape=box] ;\n"
    dot_format += tree_structure
    dot_format += "}"

    return dot_format

# Convert tree_structure and print
dot_format = pyspark_tree_to_dot(tree_structure, ["danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration"])
print(dot_format)