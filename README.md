# popularsongs
Public Repository for Big Data Final Project: Analysis of Popular Songs

Directories 
/ana_code →  files containing the pySpark code used for analytics
/profiling_code → profiling code 
/data_ingest → our code for fetching the audio features using Spotify API
/data →  copy of all the data used 


Data:
Spotify 2022 Top 50
"Top50_nofeatures"→ Original Dataset of 2022 Top50 songs w/out audio features

"Top50_features" → Fetched audio features for 2022 top50 songs

"cleaned_out_new.csv"→ Top50_features file after initial cleaning 

"cleaned_out_new_header.csv" → Dataset after initial cleaning w/headers

Spotify Popular Songs
"Streams.csv" → Dataset containing 4000 popular song, Spotify URIs and streams

"Full_audio_features"→ Dataset containing 4000 popular songs from Spotify along with their audio features using songs from "streams.csv"

Apple Music Popular Songs
"kworb_popularity_no_features.csv" →  Original dataset containing around 2000 popular songs from apple music
"Kworb_popularity.csv"→ Fetched audio features for popular songs from apple music

Cleaning :
Source code for ETL is called CleanV2. CleanV2 is a map-reduce job where only a mapper is used. CleanV2 runs on the "Top50_features" dataset as input and outputs the cleaned data doing the following things :
Drop all the columns that are irrelevant to the project
Normalizes the texts using regexes by turning all of them lowercase, removing all whitespace,punctuations and diacritic
Decided to not use  this because we need the texts to be the same to be able to join.
Creates two new binary columns positive and is_acoustic using the values of valence(positive/negative emotion of the song) and acousticness, comparing to a threshold 
Binary columns weren't really used so we dropped this as well

The output of the cleaning program is called "clean_out_new.csv" and the output with added header is called "clean_out_new_header.csv"


Profiling(ti2060):
Source code for Profiling is stored in etl_code subdirectory ti2060  it contains 4 MapReduce  programs. "Mean,Median,Mode" , "Print Distinct Values" ,"countRecs" and  "getSongCount" . The mean median mode and the print distinct values program is part of exploratory data analysis and the getSongCount program is used to count the number of times that a song shows up on the top chart 

countRecs:  takes "cleaned_out_new.csv" or any file and use MapReduce to count the number of records 

Mean_Median_mode : takes "cleaned_out_new.csv" as input and finds the mean,median and mode for all the numerical values (the audio features of each song)  the output is called "stats_out" 

Print Distinct Songs/Artists:  takes "cleaned_out_new.csv" as input and performs map-reduce to print out distinct songs and artists. The output is stored as "distinct_records" and "distinct_artists"

getSongCount: takes "cleaned_out_new_header.csv" as input and performs map-reduce to count each distinct songs and count the number of times they appeared on the top50 charts as a csv file called "songs.csv" with two columns : "song name" and "weeks_on_top_chart". This file is used for the analytics section later


Analytics for this project is done using SparkML through pySpark. Analytics program ran on both the spotify top50 data(cleaned_out_new_header.csv) and spotify popular songs data(full_audio_features and streams.csv) by performing joins on spotify URI. 

"Cleaned_out_new_header" is the output from the cleaning program with manually added headers.


Correlation analytics performed through pySpark using program "correlation.py"

Correlation on Top 50 Data:
Reads the data from "cleaned_out_new_header.csv" into a dataframe, then combines all the audio_features columns into a new column "features". Finally, using pySpark's correlation command, distributively calculate the matrix 
Visualization of matrix using seaborn


Correlation on Popular Songs
Read the data from "full_audio_features.csv" into a dataframe and perform similar analysis as above.

Regression:
Linear and Random forest regression models are used to try and model the way in which audio features can predict weeks on the top chart and the number of streams it will get. 
Linear on top 50(Linear_top50.py) to predict weeks on top charts
Read both "cleaned_out_new_header.csv" and "song.csv" into dataframe. Perform an inner join between them on the "song_name" column.Now the combined data frame has both the audio features and the weeks on the top50 on the same rows.Program then split the data in train and test set and train  SparkML's linear regression model and prints out the coefficients, R^2 errors and p-values. 


Linear on Popular Songs (Linear_full.py) to predict streams
Read "full_audio_features.csv" and "streams.csv" into dataframe. Perform an inner join between them on the "uri" column. Now the combined data frame has both the audio features and the streams for the popular songs. Program then performs the linear regression fitting like above. 


Random Forest on top 50(RandomForest_top50.py)  to predict weeks on top charts
Read both "cleaned_out_new_heder.csv" and "song.csv" into the dataframe. Perform inner join on the "song_name"columns. Then, split the data into a train and test set, and train SparkML's random forest (bagging) model with 20 trees. Test the model and print the R^2 values as well as print out the importance of each feature in making the prediction and an example of a tree the model made. 


Random Forest on Popular Songs (RandomForest_full.py) to predict streams
Read "full_audio_features.csv" and "streams.csv" into dataframe. Perform inner join on "uri" column . Do the same analysis as above.


