from services.spotify import spotifyExtract
from services.bigquery import bigquery
from dotenv import load_dotenv
import os
import json
from time import sleep

env_path = './secrets/.env'
load_dotenv(env_path)


def generate_schema(fp, example_record):
    if os.path.exists(fp):
        print(f'Schema document already exists at {fp}')
    else:
        schema = bigquery.get_bq_schema(example_record)
        with open(fp, 'w') as file:
            json.dump(schema, file, indent=4)
            print(f'Schema document created at {fp}')


def extract_load_listening_history(sp_client, bq_client):
    listening_history = sp_client.get_history()
    if not listening_history:
        print('No listening history found - exiting function')
        return
    print(f'{len(listening_history)} tracks returned')
    bq_client.insert_rows(listening_history)


def main():
    sp = spotifyExtract()

    project = os.getenv('BIGQUERY_PROJECT')
    dataset = 'spotify_archivist'
    table = 'listening_history'
    schema_fp = './services/bigquery/schema/listening_history.json'

    bq = bigquery.bigQueryLoad(project, dataset, table)
    bq.create_table(schema_fp)

    while True:
        extract_load_listening_history(sp, bq)
        sleep(60)  # Run every 3 minutes
        

if __name__ == '__main__':
    main()
