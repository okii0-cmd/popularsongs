#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 16 22:27:14 2023

@author: macbookpro
"""

import csv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os

#specify path to file containing track names
with open('/Users/macbookpro/Downloads/new_songs - Sheet1.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    column_index = 1
    track_names = []
    for row in csvreader:
        column_value = row[column_index]
        track_names.append(column_value)

cache_path1 = os.path.join(os.getcwd(), '.cache-popularSongsSpotify')
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id="a65b49b12ee94438a98d63209aeceeb1",
                                               client_secret="6b964affab7743bdb7e614a44da03370",
                                               redirect_uri="http://localhost:8888/callback",
                                               scope=["user-library-read"],
                                               cache_path=cache_path1))

BATCH_SIZE = 50
batches = [track_names[i:i+BATCH_SIZE] for i in range(0, len(track_names), BATCH_SIZE)]

track_name_uri_pairs = []
for i, batch in enumerate(batches):
    for track_name in batch:
        results = sp.search(q=track_name, limit=1, type='track')['tracks']['items']
        if results:
            track_uri = results[0]['uri']
            track_name_uri_pairs.append((track_name, track_uri))
    print("batch done")

# Write the track names and URIs to a new CSV file
with open('track_names_and_uris.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Track Name", "Track URI"])
    for track_name, track_uri in track_name_uri_pairs:
        writer.writerow([track_name, track_uri])
        
   



