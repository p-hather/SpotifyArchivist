import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import time
from datetime import datetime
import shelve

# TODO look into using token instead of auth each time

class spotifyHistory:

    def __init__(self):
        load_dotenv()  # TODO move this into main.py so all files can access env
        self.client = self.spotify_auth()
        
    def spotify_auth(self):
        scopes = 'user-read-recently-played'
        return spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scopes, open_browser=False))
    
    def unix_ms_strf(unix_ms):
        if isinstance(unix_ms, str):
            unix_ms = int(unix_ms)
        ts = datetime.utcfromtimestamp(unix_ms/1000)
        return ts.strftime('%Y-%m-%d %H:%M:%S')
    
    def get_history(self):
        with shelve.open('spotify_history') as sh:
            try:
                after = sh['after_ts']
            except KeyError:
                after = None

            print(f'Getting recently played tracks with an after timestamp of {after}')
            recently_played = self.client.current_user_recently_played(after=after, limit=50)
            
            now_ms = str(round(time.time() * 1000))
            sh['after_ts'] = now_ms

        print(recently_played)
        tracks = recently_played['items']

        if not tracks:
            print('No recent tracks found')
            return
        
        return tracks

spotifyHistory().get_history()

# now = str(round(time.time() * 1000))
# print(now)
