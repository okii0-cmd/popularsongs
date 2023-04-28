# Status
All the files in this zip file were created after the submission of HW8. Here is a run-down of the approximate timeline in which the files should be run, along with what they do. Of course, the files themselves also have comments explaining the code.
- All code is working (although kworbanalysis_1.ipynb takes a while to run, see below).

## kworbanalysis_1.ipynb
Our team realized that it may be helpful to use a different set of popular songs to see if our results would vary. We decided to use iTunes popular songs data. Unfortunately iTunes does not offer an easy way to fetch / scrape data like Spotify does, but likely a website exists (kworb.net) that has a list of the 2000 most popular songs according to iTunes since 2013, so we decided to do that instead.

- Uses BeautifulSoup to parse the table at 'https://kworb.net/pop/cumulative.html' into a csv.
- Uses Spotipy to find the songs in the table by song name, and appends a string version of a dictionary of their audio features to their respective row in the csv.
- Output csv stored in 'kworb_popularity.csv'.

### Notes: 
- 'kworb_popularity.csv' is renamed to 'kworb_populairty_raw.csv' before kworbanalysis_2.ipynb is run.
- 'kworbanalysis_1.ipynb' took around 30 minutes to run because it does not utilize Spotify's batch processing capabilities.

## kworbanalysis_2.ipynb
After realizing that the column at index 3 was just a large string containing a dictionary of the audio features, we had to write code to split this string into actual columns in the dataframe. So that's what this file does.
Also, in 'kworb_populairty_raw.csv', 1999 out of 2000 songs were found using the Spotify web API. The 1 song that could not be found was the song at index 956, "Shawn Mendes - Lost in Japan". However, we discovered that it did exist in the Spotify database, so we just filled in that row manually by using the track_uri.

## spark_correlation.ipynb
Calculates the correlation matrix of all the variables.
- Used pandas instead of spark csv reader because spark csv reader is bad at parsing commas...

## spark_regression.ipynb
Calculates regression on the data. RMSE is not bad, but R^2 does not look good. May be worth looking into other models to reprent the data.
