from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import json
from classes import EventPayload
from fastapi import HTTPException

def load_config(section: str):
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config.get(section, {})


def write_event_bigquery(event_payload: EventPayload):
    try:
        config = load_config(section='bigquery')

        bqcreds = service_account.Credentials.from_service_account_file(
            config['credentials_file'],
            scopes=config['scopes']
        )
        client = bigquery.Client(credentials=bqcreds, project=bqcreds.project_id)

        transformed_payload = event_payload.model_dump()
        event_timestamp: datetime = transformed_payload['event_time']
        transformed_payload['event_time'] = event_timestamp.isoformat()

        rows_to_insert = [transformed_payload]
        table_id = config['table_id']

        table = client.get_table(table_id)
        errors = client.insert_rows_json(table, rows_to_insert)

        if errors:
            raise RuntimeError(f"Encountered errors while inserting rows: {errors}")

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

