from google.cloud import bigquery, exceptions


class bigQueryLoad:

    def __init__(self):
        self.project = 'sector-7g-296122'
        self.dataset = 'spotify_archivist'
        self.table = 'listening_history'
        self.table_id = '.'.join([self.project, self.dataset, self.table])
        self.bq = self.get_client()
        self.schema = self.bq.schema_from_json('./schema/listening_history.json')
        self.table_obj = bigquery.Table(self.table_id, schema=self.schema)
        self.create_table()
    
    def get_client(self):
        print('Authenticating BigQuery access with service account')
        return bigquery.Client()

    def create_table(self):
        try:
            self.bq.create_table(self.table_obj)
            print(f'Created table `{self.table_id}`')
        except exceptions.Conflict:
            print(f'Table `{self.table_id}` already exists')

    def insert_rows(self):
        print('Attempting to insert rows')
        self.bq.insert_rows(self.table_obj, [
            {"artist": "artist 1", "title": "test title 1"},
            {"artist": "artist 2", "title": "test title 2"}
        ])

bql = bigQueryLoad()
# bql.insert_rows()

# query_job = client.query(
#     '''
#         SELECT *
#         FROM `sector-7g-296122.spotify_archivist.test`
#     '''
# )

# results = query_job.result()
# for row in results:
#     print(row)


