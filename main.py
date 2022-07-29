from services.spotify import spotifyExtract
from services.bigquery import bigquery
from dotenv import load_dotenv
import os
import json
from time import sleep
import logging

env_path = './secrets/.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, filename="sbqt.log", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")


def generate_schema(fp, example_record):
    if os.path.exists(fp):
        logging.info(f'Schema document already exists at {fp}')
    else:
        schema = bigquery.get_bq_schema(example_record)
        with open(fp, 'w') as file:
            json.dump(schema, file, indent=4)
            logging.info(f'Schema document created at {fp}')


def extract_load_listening_history(sp_client, bq_client):
    listening_history = sp_client.get_history()
    if not listening_history:
        logging.info('No listening history found - exiting function')
        return
    logging.info(f'{len(listening_history)} tracks returned')
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
        sleep(180)  # Run every 3 minutes
        

if __name__ == '__main__':
    main()
