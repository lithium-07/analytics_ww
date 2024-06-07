import json
from google.cloud import bigquery
import asyncio
from concurrent.futures import ThreadPoolExecutor
from google.oauth2 import service_account
from queries import checkout_completed_query, sessions_query, scroll_query, add_to_cart_query, total_revenue_query

def load_config(section: str):
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config.get(section, {})

config = load_config(section='bigquery')
bqcreds = service_account.Credentials.from_service_account_file(
    config['credentials_file'],
    scopes=config['scopes']
)
client = bigquery.Client(credentials=bqcreds, project=bqcreds.project_id)

# Create an event loop and executor for the async functions
loop = asyncio.get_event_loop()
executor = ThreadPoolExecutor(max_workers=4)

queries = {
    "add_to_cart_query": f'{add_to_cart_query}',
    "sessions_query": f'{sessions_query}',
    "checkut_events_query": f'{checkout_completed_query}',
    "scroll_query": f'{scroll_query}',
    "total_revenue_query": f'{total_revenue_query}',
}

def run_query(query: str):
    query_job = client.query(query)
    query_job.result()

# Async function to run queries concurrently
async def run_all_queries():
    tasks = [loop.run_in_executor(executor, run_query, query) for query in queries.values()]
    await asyncio.gather(*tasks)