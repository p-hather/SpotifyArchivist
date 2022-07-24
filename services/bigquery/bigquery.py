from google.cloud import bigquery, exceptions
import re


def get_bq_schema(sample_data):
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
            'mode': 'NULLABLE'
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
    for k, v in sample_data.items():
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

class bigQueryLoad:

    def __init__(self, project, dataset, table):
        self.table_id = '.'.join([project, dataset, table])
        self.bq = self.get_client()
    
    def get_client(self):
        print('Attempting to authenticate BigQuery access with service account')
        return bigquery.Client()

    def create_table(self, schema_fp):
        schema = self.bq.schema_from_json(schema_fp)
        table_obj = bigquery.Table(self.table_id, schema=schema)
        try:
            self.bq.create_table(table_obj)
            print(f'Created table `{self.table_id}`')
        except exceptions.Conflict:
            print(f'Table `{self.table_id}` already exists')

    def insert_rows(self, rows):
        print('Attempting to insert rows')
        try:
            errors = self.bq.insert_rows_json(self.table_id, rows)
            if errors:
                print(f'Errors occurred:\n{errors}')
            else:
                print('Rows inserted successfully')
        except exceptions.NotFound:
            print(f'Table not found - verify {self.table_id} exists and use create_table function if applicable')
