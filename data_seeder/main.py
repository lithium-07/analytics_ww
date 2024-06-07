from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from google.cloud import bigquery
from google.oauth2 import service_account
import random
import string
from datetime import datetime, timedelta
import time

app = FastAPI()

# Path to your service account key file
key_path = "creds.json"

# Load credentials from the key file
credentials = service_account.Credentials.from_service_account_file(key_path)

# BigQuery configuration
dataset_id = ''
table_id = ''
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Helper function to create random event data
def create_event(event_type):
    user_agent = ''.join(random.choices(string.ascii_letters + string.digits, k=15))
    is_mobile_device = random.choice([True, False])
    platform = 'mobile' if is_mobile_device else 'web'
    random_int = random.randint(0,45)
    random_event_time = datetime.now() - timedelta(days=random_int)
    base_event = {
        'platform': platform,
        'event_time': random_event_time.isoformat(),
        'event_name': event_type,
        'event_id': ''.join(random.choices(string.ascii_letters + string.digits, k=10)),
        'user_id': ''.join(random.choices(string.ascii_letters + string.digits, k=10)),
        'session_id': f'sess-{"".join(random.choices(string.ascii_letters + string.digits, k=10))}',
        'page_url': f'https://example-{random_int}.com',
        'user_agent': user_agent,
        'language': 'en-US',
    }

    if event_type == 'page_viewed':
        return base_event
    elif event_type == 'page_scroll':
        return {**base_event, 'page_title': 'Example Page', 'percent_scroll': random.randint(0, 100)}
    elif event_type == 'product_added_to_cart':
        return {**base_event, 'product_id': ''.join(random.choices(string.ascii_letters + string.digits, k=10)), 'product_price': random.uniform(1, 100), 'currency': 'USD'}
    elif event_type == 'checkout_completed':
        return {**base_event, 'order_id': ''.join(random.choices(string.ascii_letters + string.digits, k=10)), 'order_value': random.uniform(20, 200), 'city': 'San Francisco', 'country': 'USA', 'currency': 'USD', 'user_email': 'user@example.com'}
    else:
        return base_event

# Function to insert data into BigQuery
def insert_data(event_data):
    print("Starting data insertion")
    
    table = client.dataset(dataset_id).table(table_id)
    
    start_time = time.perf_counter()
    errors = client.insert_rows_json(table, event_data)
    end_time = time.perf_counter()
    
    time_taken = end_time - start_time
    print(f"Time taken for data insertion: {time_taken:.6f} seconds")
    
    if errors:
        print('Errors:', errors)
    else:
        print(f'Inserted {len(event_data)} rows')

# Function to generate and insert events
def generate_and_insert_events():
    event_types = ['page_viewed', 'page_scroll', 'product_added_to_cart', 'checkout_completed']
    event_data = [create_event(random.choice(event_types)) for _ in range(5000)]
    insert_data(event_data)

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(generate_and_insert_events, 'interval', minutes=1)
scheduler.start()

@app.get("/")
def read_root():
    return {"message": "FastAPI server is running and inserting data into BigQuery every 5 minutes"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
