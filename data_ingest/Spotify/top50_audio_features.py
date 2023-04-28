#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  1 17:35:32 2023

@author: macbookpro
"""
import csv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os

#specify path to the top50 charts for 2022
with open('/Users/macbookpro/Downloads/regional-global-weekly-2022.csv', 'r') as csvfile:
    # Create a CSV reader object
    csvreader = csv.reader(csvfile)
    column_index = 1
    track_uris = []
    for row in csvreader:
        # Extract the value from the desired column for this row
        column_value = row[column_index]
        #Add the value to the list of column values
        track_uris.append(column_value)

# set up the Spotify API client
cache_path1 = os.path.join(os.getcwd(), '.cache-BigDataSpotify')
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id="6c3bb7a110824328ac2ce9e7412b306d",
                                               client_secret="e877cedaf95142a8813bdd95c72e6cc8",
                                               redirect_uri="http://localhost:8888/callback",
                                               scope=["user-library-read"],
                                               cache_path=cache_path1))

# Set the batch size
BATCH_SIZE = 50

# Split the list of track URIs into batches
batches = [track_uris[i:i+BATCH_SIZE] for i in range(0, len(track_uris), BATCH_SIZE)]

# get the audio features for each batch of tracks
audio_features = []
for i, batch in enumerate(batches):
    # Get the audio features for this batch of tracks
    features_batch = sp.audio_features(batch)
    for features in features_batch :
        audio_features.append(features)
        
        
       
with open('merged_data.csv', 'w', newline='') as csvfile:
    fieldnames =  ['index','rank','uri','artist_names','track_name','source','peak_rank','previous_rank','weeks_on_chart','streams'] + list(audio_features[0].keys())
    writer = csv.writer(csvfile)
    writer.writerow(fieldnames)
    with open('/Users/macbookpro/Downloads/regional-global-weekly-2022.csv', 'r') as csvfile2:
        csvreader2 = csv.reader(csvfile2)
        for i, (row, features) in enumerate(zip(csvreader2, audio_features)):
            # Write the original data from the CSV file and the audio features data
            writer.writerow([i+1] + row + list(features.values()))
