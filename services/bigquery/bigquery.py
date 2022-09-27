from google.cloud import bigquery, exceptions
import re
import logging


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

    date_regex = "\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
    timestamp_regex = f"{date_regex}T[0-2]\d(:[0-5][0-9]){2}.\d{1,6}Z"

    fields = []
    for k, v in sample_data.items():
        bq_mode = bq_ref[type(v)]["mode"]

        # Lookup first value in list for type
        if isinstance(v, list):
            v = v[0]
        bq_type = bq_ref[type(v)]["type"]

        # Check if string values are actually dates or timestamps
        if isinstance(v, str):
            if re.match(f'^{date_regex}$', v):
                bq_type = "DATE"
            elif re.match(f'^{timestamp_regex}$', v):
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

    def __init__(self, project, dataset, table, basic_view=None):
        self.table_id = '.'.join([project, dataset, table])
        self.basic_view = basic_view
        self.bq = self.get_client()
    
    def get_client(self):
        logging.info('Attempting to authenticate BigQuery access with service account')
        return bigquery.Client()

    def create_table(self, schema_fp):
        schema = self.bq.schema_from_json(schema_fp)
        table_obj = bigquery.Table(self.table_id, schema=schema)
        try:
            self.bq.create_table(table_obj)
            logging.info(f'Created table `{self.table_id}`')
        except exceptions.Conflict:
            logging.info(f'Table `{self.table_id}` already exists')
    
    def create_basic_view(self, sql_fp):
        if not self.basic_view:
            logging.info(f'No basic view specified - skipping')
            return
        
        basic_view_id = '.'.join([self.project, self.dataset, self.basic_view])
        
        with open(sql_fp, 'r') as file:
            sql = file.read()
        
        basic_view = bigquery.Table(basic_view_id)
        basic_view.view_query = sql.format(source=self.table_id)
        try:
            self.bq.create_table(basic_view)
            logging.info(f'Created view `{basic_view_id}`')
        except exceptions.Conflict:
            logging.info(f'View `{basic_view_id}` already exists')

    def insert_rows(self, rows):
        logging.info('Attempting to insert rows')
        try:
            errors = self.bq.insert_rows_json(self.table_id, rows)
            if errors:
                logging.info(f'Errors occurred:\n{errors}')
            else:
                logging.info('Rows inserted successfully')
        except exceptions.NotFound:
            logging.info(f'Table not found - verify {self.table_id} exists and use create_table function if applicable')
