import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from google.cloud import bigquery
from bigquery import checkout_completed_table_id, add_to_cart_table_id, sessions_table_id, revenue_table_id, scroll_table_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

loop = asyncio.get_event_loop()
executor = ThreadPoolExecutor(max_workers=4)

def run_query(query: str, client:  bigquery.Client):
    query_job = client.query(query)
    return list(query_job.result())


async def get_checkout_completed_events(page_url: str, start_date: str, end_date: str, client: bigquery.Client):
    try:
        query = f"""
        SELECT SUM(checkout_completed_events) AS checkout_completed_events
        FROM `{checkout_completed_table_id}`
        WHERE page_url = '{page_url}' AND event_date BETWEEN '{start_date}' AND '{end_date}'
        """
        result = await loop.run_in_executor(executor, run_query, query, client)
        return result[0]['checkout_completed_events'] if result else 0
    except Exception as e:
        logger.exception(f"Failed to run query for checkout events between {start_date} and {end_date} for {page_url}", e)
        return 0


# Async function to get number_of_sessions
async def get_number_of_sessions(page_url: str, start_date: str, end_date: str, client: bigquery.Client):
    try:
        query = f"""
        SELECT SUM(number_of_sessions) AS number_of_sessions
        FROM `{sessions_table_id}`
        WHERE page_url = '{page_url}' AND event_date BETWEEN '{start_date}' AND '{end_date}'
        """
        result = await loop.run_in_executor(executor, run_query, query, client)
        return result[0]['number_of_sessions'] if result else 0
    except Exception as e:
        logger.exception(f"Failed to run query for sessions between {start_date} and {end_date} for {page_url}", e)
        return 0

# Async function to get total_revenue
async def get_total_revenue(page_url: str, start_date: str, end_date: str, client: bigquery.Client):
    try:
        query = f"""
        SELECT SUM(total_revenue) AS total_revenue
        FROM `{revenue_table_id}`
        WHERE page_url = '{page_url}' AND event_date BETWEEN '{start_date}' AND '{end_date}'
        """
        result = await loop.run_in_executor(executor, run_query, query, client)
        return result[0]['total_revenue'] if result else 0
    except Exception as e:
        logger.exception(f"Failed to run query for revenue between {start_date} and {end_date} for {page_url}", e)
        return 0


# Async function to get add_to_cart_events
async def get_add_to_cart_events(page_url: str, start_date: str, end_date: str, client: bigquery.Client):
    try:
        query = f"""
        SELECT SUM(add_to_cart_events) AS add_to_cart_events
        FROM `{add_to_cart_table_id}`
        WHERE page_url = '{page_url}' AND event_date BETWEEN '{start_date}' AND '{end_date}'
        """
        result = await loop.run_in_executor(executor, run_query, query, client)
        return result[0]['add_to_cart_events'] if result else 0
    except Exception as e:
        logger.exception(f"Failed to run query for add to cart events between {start_date} and {end_date} for {page_url}", e)
        return 0
    
async def get_scroll_events(page_url: str, start_date: str, end_date: str, client: bigquery.Client):
    try:
        query = f"""
        SELECT SUM(total_scroll_sum) AS total_scroll_sum, SUM(total_events) AS total_scroll_events
        FROM `{scroll_table_id}`
        WHERE page_url = '{page_url}' AND event_date BETWEEN '{start_date}' AND '{end_date}'
        """
        result = await loop.run_in_executor(executor, run_query, query, client)
        return result[0]['total_scroll_sum'], result[0]['total_scroll_events'] if result else 0, 0
    except Exception as e:
        logger.exception(f"Failed to run query for add to cart events between {start_date} and {end_date} for {page_url}", e)
        return 0, 0