from services.spotify import spotifyExtract
from services.bigquery import bigQueryLoad
from dotenv import load_dotenv

load_dotenv()


def main():
    sp = spotifyExtract()
    sp_listening_history = sp.get_history()
    
    bq = bigQueryLoad()

if __name__ == '__main__':
    main()

