# Spotify Archivist

## Archive your Spotify history in BigQuery

Spotify Archivist is a Python app built to archive Spotify listening history in BigQuery

### Why?

Spotify doesn't currently offer a complete listening history in the UI. The API will only offer the last 50 tracks and the response needs wrangling to be useful. I wanted to create a tool that records a full listening history in a database for tracking and analysis.

### Set up

Set up Spotify Archivist by cloning this repo, ensuring you have all the prerequisites listed below, and running `main.py`. This script will need to continue running for the bot to work - a cloud platform or Raspberry Pi acting as a server is ideal.

### Prerequisites
* A GCP Service Account created with write access to your chosen BigQuery destination, and the JSON keyfile stored in `./secrets`.
* A Spotify App created (`https://developer.spotify.com/dashboard`) and the Client ID, Secret, and Callback URL stored in `.env`
* A BigQuery destination dataset created. The app is set up to use a dataset called `spotify_archivist` by default (editable in `main.py`)
* All packages in `requirements.txt` installed

### How does it work?
* The app will run in the background, requesting your listening history from the Spotify API every 3 minutes. 
* New responses will be streamed into your chosen BigQuery destination

### Other features
* The `generate_schema` function in `main.py` references some custom code that will generate a BigQuery schema JSON document based on a sample record.

### Notes
* The full API response will be loaded to BigQuery with no transformation step. This is designed to archive a full set of data but results in a fairly complex table with nested fields. SQL to transform the table into a more user-friendly output will be added in future.
