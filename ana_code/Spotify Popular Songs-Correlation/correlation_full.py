#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 14:16:55 2023

@author: macbookpro
"""

from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Create a Spark session
spark = SparkSession.builder.appName("Correlation").getOrCreate()

#Read Data
#data = spark.read.csv('/Users/macbookpro/Desktop/Big Data Project/Data/full_audio_features.csv', header=True, inferSchema=True)
#specify file when running on dataproc pySpark Shell
data = spark.read.csv('full_audio_features.csv',header = True,inferSchema=True )
# Assemble the  columns into a feature vector
assembler = VectorAssembler(inputCols=["danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration_ms"], outputCol="features")
data_with_features = assembler.transform(data)

# Compute the correlation matrix
correlation_matrix = Correlation.corr(data_with_features, "features").head()

#print the correlation matrix
print("Correlation matrix:")
print(correlation_matrix[0])

correlation_df = pd.DataFrame(correlation_matrix[0].toArray(),
                              columns=["danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration_ms"],
                              index=["danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration_ms"])

# Create a heatmap using Seaborn
plt.figure(figsize=(12, 10))
sns.heatmap(correlation_df, annot=True, cmap='coolwarm', fmt='.2f', linewidths=.5)
plt.title('Correlation Matrix')
plt.show()