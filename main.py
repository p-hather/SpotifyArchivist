from services.spotify import spotifyExtract
from services.bigquery import bigquery
from dotenv import load_dotenv
from os import path
import json
import pyperclip

load_dotenv()


def generate_schema(fp, example_record):
    if path.exists(fp):
        print(f'Schema document already exists at {fp}')
    else:
        schema = bigquery.get_bq_schema(example_record)
        with open(path, 'w') as file:
            json.dump(schema, file, indent=4)
            print(f'Schema document created at {fp}')

def extract_load_listening_history(sp_client, bq_client):
    # listening_history = sp_client.get_history()
    # print(f'length of listening_history is {len(listening_history)}')
    # listening_history_json = [json.dumps(record) for record in listening_history]
    # print(f'length of listening_history_json is {len(listening_history_json)}')
    # with open('lh.json', 'w') as file:
    #     json.dump(listening_history, file, indent=4)
    # with open('lhj.json', 'w') as file:
    #     json.dump(listening_history_json, file, indent=4)
    #bq_client.insert_rows(listening_history_json)
    #bq_client.insert_rows(pyperclip.paste())

    with open('lh.json') as file:
        lh = json.load(file)
    bq_client.insert_rows(lh)

def main():
    sp = spotifyExtract()

    project = 'sector-7g-296122'
    dataset = 'spotify_archivist'
    table = 'listening_history'
    schema_fp = './services/bigquery/schema/listening_history.json'
    bq = bigquery.bigQueryLoad(project, dataset, table, schema_fp)

    extract_load_listening_history(sp, bq)


if __name__ == '__main__':
    main()
