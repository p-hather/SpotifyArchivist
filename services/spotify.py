import spotipy
from spotipy.oauth2 import SpotifyOAuth
import time
from datetime import datetime
import shelve
import logging
from requests.exceptions import ReadTimeout, ConnectionError


class spotifyExtract:
    def __init__(self):
        self.client = self.spotify_auth()

    def spotify_auth(self):
        scopes = 'user-read-recently-played'
        logging.info(f'Attempting to authenticate Spotify access with scopes: {scopes}')
        return spotipy.Spotify(
            auth_manager=SpotifyOAuth(scope=scopes, open_browser=False),
            requests_timeout=10, retries=10)
    
    def unix_ms_strf(self, unix_ms):
        ts = datetime.utcfromtimestamp(int(unix_ms)/1000)
        return ts.strftime('%Y-%m-%d %H:%M:%S')
    
    def get_history(self):
        # Store a persistent timestamp to allow linear querying
        with shelve.open('spotify_history') as sh:
            try:
                after = sh['after_ts']
            except KeyError:
                after = None
            
            if not after:
                logging.info('Getting recently played tracks with no time constraints')
            else:
                logging.info(f'Getting recently played tracks after {self.unix_ms_strf(after)}')
            
            # Work around connection timeout issue by re-instantiating Spotify client
            try:
                recently_played = self.client.current_user_recently_played(after=after)
            except (ReadTimeout, ConnectionError):
                logging.info('Spotify connection lost - attempting to reconnect')
                self.client = self.spotify_auth()
                recently_played = self.client.current_user_recently_played(after=after)

            tracks = recently_played['items']

            # This endpoint will only return the last 50 results, so the pagination below is not 
            # currently useful. Retained just in case this changes in future. 
            while recently_played['next']:
                recently_played = self.client.next(recently_played)
                tracks.extend(recently_played['items'])
            
            now_ms = str(round(time.time() * 1000))
            sh['after_ts'] = now_ms

        return tracks
