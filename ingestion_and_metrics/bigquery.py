import asyncio
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from classes import EventPayload, MetricsRequest, MetricsResponse
from fastapi import HTTPException
from utils import get_add_to_cart_events, get_checkout_completed_events, get_number_of_sessions, get_total_revenue, get_scroll_events
from config import creds, scopes, base_table_id, add_to_cart_table_id, checkout_completed_table_id, sessions_table_id, scroll_table_id, revenue_table_id

bqcreds = service_account.Credentials.from_service_account_file(
    creds,
    scopes=scopes
)
client = bigquery.Client(credentials=bqcreds, project=bqcreds.project_id)


def write_event_bigquery(event_payload: EventPayload):
    try:
        transformed_payload = event_payload.model_dump()
        event_timestamp: datetime = transformed_payload['event_time']
        transformed_payload['event_time'] = event_timestamp.isoformat()

        rows_to_insert = [transformed_payload]

        table = client.get_table(base_table_id)
        errors = client.insert_rows_json(table, rows_to_insert)

        if errors:
            raise RuntimeError(f"Encountered errors while inserting rows: {errors}")

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))



def get_bigquery_metrics(request: MetricsRequest) -> MetricsResponse:
    page_url = request.page_url
    start_date = request.start_date
    end_date = request.end_date

    try:
        # Validate dates
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    # Query to get the required data from BigQuery
    query = f"""
    SELECT
        a.page_url,
        SUM(c.add_to_cart_events) AS add_to_cart_events,
        SUM(b.checkout_completed) AS checkout_completed_events,
        SUM(a.number_of_sessions) AS number_of_sessions,
        SUM(d.total_revenue) AS total_revenue,
        SUM(e.total_scroll_sum) AS total_scroll_sum, 
        SUM(e.total_events) AS total_scroll_events
    FROM
        `{sessions_table_id}` a
    LEFT JOIN
        `{checkout_completed_table_id}` b
    ON
        a.page_url = b.page_url AND a.event_date = b.event_date
    LEFT JOIN
        `{add_to_cart_table_id}` c
    ON
        a.page_url = c.page_url AND a.event_date = c.event_date
    LEFT JOIN
        `{revenue_table_id}` d
    ON
        a.page_url = d.page_url AND a.event_date = d.event_date
    LEFT JOIN
        `{scroll_table_id}` e
    ON
        a.page_url = e.page_url AND a.event_date = e.event_date
    WHERE
        a.page_url = '{page_url}'
        AND a.event_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
    GROUP BY
        a.page_url
    """

    query_job = client.query(query)
    results = list(query_job.result())

    if not results:
        raise HTTPException(status_code=404, detail="No data found for the given page URL and date range.")

    result = results[0]

    # Compute the metrics
    add_to_cart_events = result['add_to_cart_events']
    checkout_completed_events = result['checkout_completed_events']
    number_of_sessions = result['number_of_sessions']
    total_revenue = result['total_revenue']
    total_scroll_sum = result['total_scroll_sum']
    total_scroll_events = result['total_scroll_events']

    cart_percentage = (add_to_cart_events / number_of_sessions) * 100 if number_of_sessions else 0
    conversion_rate = (checkout_completed_events / number_of_sessions) * 100 if add_to_cart_events else 0
    average_order_value = total_revenue / checkout_completed_events if checkout_completed_events else 0
    revenue_per_session = total_revenue / number_of_sessions if number_of_sessions else 0
    average_scroll_percentage = total_scroll_sum / total_scroll_events if total_scroll_events else 0

    return MetricsResponse(
        page_url=page_url,
        cart_percentage=cart_percentage,
        conversion_rate=conversion_rate, 
        average_order_value=average_order_value,
        revenue_per_session=revenue_per_session,
        total_sessions=number_of_sessions,
        average_scroll_percentage=average_scroll_percentage
    )


async def get_bigquery_metrics_parallel(request: MetricsRequest) -> MetricsResponse:
    page_url = request.page_url
    start_date = request.start_date
    end_date = request.end_date

    try:
        # Validate dates
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Run queries concurrently
    add_to_cart_events, checkout_completed_events, number_of_sessions, total_revenue, scroll_events = await asyncio.gather(
        get_add_to_cart_events(page_url, start_date_str, end_date_str, client),
        get_checkout_completed_events(page_url, start_date_str, end_date_str, client),
        get_number_of_sessions(page_url, start_date_str, end_date_str, client),
        get_total_revenue(page_url, start_date_str, end_date_str, client),
        get_scroll_events(page_url, start_date_str, end_date_str, client)
    )
    total_scroll_sum = scroll_events[0]
    total_scroll_events = scroll_events[1]

    # Compute the metrics
    cart_percentage = (add_to_cart_events / number_of_sessions) * 100 if number_of_sessions else 0
    conversion_rate = (checkout_completed_events / number_of_sessions) * 100 if number_of_sessions else 0
    average_order_value = total_revenue / checkout_completed_events if checkout_completed_events else 0
    revenue_per_session = total_revenue / number_of_sessions if number_of_sessions else 0
    average_scroll_percentage = total_scroll_sum / total_scroll_events if total_scroll_events else 0

    return MetricsResponse(
        page_url=page_url,
        cart_percentage=cart_percentage,
        conversion_rate=conversion_rate,
        average_order_value=average_order_value,
        revenue_per_session=revenue_per_session,
        total_sessions=number_of_sessions,
        average_scroll_percentage=average_scroll_percentage
    )