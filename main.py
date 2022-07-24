from services.spotify import spotifyExtract
from services.bigquery import bigquery
from dotenv import load_dotenv
from os import path
import json

load_dotenv()


def generate_schema(fp, example_record):
    if path.exists(fp):
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

    project = 'sector-7g-296122'
    dataset = 'spotify_archivist'
    table = 'listening_history'
    schema_fp = './services/bigquery/schema/listening_history.json'
    bq = bigquery.bigQueryLoad(project, dataset, table)
    bq.create_table(schema_fp)
    extract_load_listening_history(sp, bq)


if __name__ == '__main__':
    main()
