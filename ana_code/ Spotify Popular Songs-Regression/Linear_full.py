import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt

# Load audio features and streams data
audio_features = pd.read_csv("/Users/macbookpro/Desktop/Big Data Project/Data/Spotify Popular Songs /full_audio_features.csv")
streams = pd.read_csv("/Users/macbookpro/Desktop/Big Data Project/Data/Spotify Popular Songs /streams.csv")

#Specify file name directly when running on dataproc pySpark Shell
#audio_features = pd.read_csv("full_audio_features.csv")
#streams = pd.read_csv("streams.csv")

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
assembler = VectorAssembler(inputCols=["danceability", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration_ms"], outputCol="features")
data_with_features = assembler.transform(data_spark)

# Split the data into training and testing sets
(trainingData, testData) = data_with_features.randomSplit([0.8, 0.2])

# Create a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="streams", fitIntercept=True, maxIter = 1000000)

# Train the model
model = lr.fit(trainingData)

summary = model.summary
print("Training summary:")
print(summary)
# Make predictions on the test data
predictions = model.transform(testData)


# Print coefficients for interpretation
coefficients = model.coefficients
print(coefficients)

intercept = model.intercept
print("Intercept coefficient:", intercept)

# Evaluate the model
summary = model.summary
pValues = summary.pValues
print("P-values:", pValues)

evaluator = RegressionEvaluator(labelCol="streams", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)

print("R-squared error = %g" % r2)
