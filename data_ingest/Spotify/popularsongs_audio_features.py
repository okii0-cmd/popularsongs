import csv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os

# specify the name of the output file
output_file = 'audio_features.csv'

#specify path to file with track URIS
with open('/Users/macbookpro/Desktop/Big Data Project/DataFetching/track_names_and_uris.csv', 'r') as csvfile:
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
    if features_batch is not None:
        for features in features_batch:
            if features is not None:
                audio_features.append(features)
    
# write audio features to a CSV file
with open("full_audio_features.csv", 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # write the header row
    writer.writerow(audio_features[0].keys())
    # write the data rows
    for features in audio_features:
        writer.writerow(features.values())
