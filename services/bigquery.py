from google.cloud import bigquery, exceptions
import re


def get_bq_schema(data):
    '''
    Recursively iterate through sample data and return
    schema dict for configuring BigQuery table.
    '''

    bq_ref = {
        str: {
            'type': 'STRING',
            'mode': 'NULLABLE'
        },
        dict: {
            'type': 'RECORD',
            'mode': 'REPEATED'
        },
        list: {
            "mode": 'REPEATED'
        },
        int: {
            'type': 'INTEGER',
            'mode': 'NULLABLE'
        },
        float: {
            'type': 'NUMERIC',
            'mode': 'NULLABLE'
        },
        bool: {
            'type': 'BOOLEAN',
            'mode': 'NULLABLE'
        }
    }

    fields = []
    for k, v in data.items():
        bq_mode = bq_ref[type(v)]["mode"]

        # Lookup first value in list for type
        if isinstance(v, list):
            v = v[0]
        bq_type = bq_ref[type(v)]["type"]

        # Check if string values are actually dates or timestamps
        if isinstance(v, str):
            # TODO refine regex
            if re.match("\d{4}(-\d{2}){2}$", v):
                bq_type = "DATE"
            elif re.match("\d{4}(-\d{2}){2}T(\d{2}:){2}\d{2}.\d{3}Z$", v):
                bq_type = "TIMESTAMP"

        schema_def = {
            "name": k,
            "type": bq_type,
            "mode": bq_mode
            }
        
        # Recursively trigger function for nested fields
        if isinstance(v, dict):
            schema_def["fields"] = get_bq_schema(v)

        fields.append(schema_def)
    
    return fields
    # Persist dict as json file
    # with open('schema.json') as out_file:
    #     json.dump(fields, out_file)


class bigQueryLoad:

    def __init__(self, project, dataset, table, schema_fp):
        self.table_id = '.'.join([project, dataset, table])
        self.bq = self.get_client()
        self.schema = self.bq.schema_from_json(schema_fp)
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
