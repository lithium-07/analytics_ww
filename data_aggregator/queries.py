from bigquery import config

dataset_id = config['dataset_id']
add_to_cart_table_id = ".".join(dataset_id, config['add_to_cart_table'])
sessions_table_id = ".".join(dataset_id, config['sessions_table'])
checkout_completed_table_id = ".".join(dataset_id, config['checkout_completed_table'])
revenue_table_id = ".".join(dataset_id, config['revenue_table'])
scroll_table_id = ".".join(dataset_id, config['scroll_values_table'])
base_table_id = ".".join(dataset_id, config['base_table'])

checkout_completed_query = f"""
DECLARE last_processed_time TIMESTAMP;

-- Get the last processed time for the 'checkout_completed_query'
SET last_processed_time = (SELECT COALESCE(MAX(last_event_time), TIMESTAMP('1970-01-01')) FROM `{dataset_id}.metadata_table` WHERE query_name = 'checkout_completed_query');

-- Create or replace temporary table with new data, partitioned and clustered
CREATE OR REPLACE TABLE `{dataset_id}.temp_aggregated_checkout_completed`
PARTITION BY event_date
CLUSTER BY page_url AS
SELECT
  page_url,
  DATE(event_time) AS event_date,
  COUNT(*) AS checkout_completed,
  MAX(event_time) AS max_event_time
FROM
  `{base_table_id}`
WHERE
  event_name = 'checkout_completed'
  AND event_time > last_processed_time
GROUP BY
  page_url,
  event_date;

-- Create the partitioned and clustered target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{checkout_completed_table_id}` (
    page_url STRING,
    event_date DATE,
    checkout_completed INT64
)
PARTITION BY event_date
CLUSTER BY page_url;

-- Merge the new data with the existing data
MERGE `{checkout_completed_table_id}` T
USING `{dataset_id}.temp_aggregated_checkout_completed` S
ON T.page_url = S.page_url AND T.event_date = S.event_date
WHEN MATCHED THEN
  UPDATE SET T.checkout_completed = T.checkout_completed + S.checkout_completed
WHEN NOT MATCHED THEN
  INSERT (page_url, event_date, checkout_completed)
  VALUES (S.page_url, S.event_date, S.checkout_completed);

-- Update the last processed time in the metadata table
UPDATE `{dataset_id}.metadata_table`
SET last_event_time = (SELECT MAX(max_event_time) FROM `{dataset_id}.temp_aggregated_checkout_completed`)
WHERE query_name = 'checkout_completed_query';
"""

add_to_cart_query = f"""
DECLARE last_processed_time TIMESTAMP;

-- Get the last processed time for the 'add_to_cart_query'
SET last_processed_time = (SELECT COALESCE(MAX(last_event_time), TIMESTAMP('1970-01-01')) FROM `{dataset_id}.metadata_table` WHERE query_name = 'add_to_cart_query');

-- Create or replace temporary table with new data, partitioned and clustered
CREATE OR REPLACE TABLE `{dataset_id}.temp_aggregated_product_added_to_cart`
PARTITION BY event_date
CLUSTER BY page_url AS
SELECT
  page_url,
  DATE(event_time) AS event_date,
  COUNT(*) AS add_to_cart_events,
  MAX(event_time) AS max_event_time
FROM
  `{base_table_id}`
WHERE
  event_name = 'product_added_to_cart'
  AND event_time > last_processed_time
GROUP BY
  page_url,
  event_date;

-- Create the partitioned and clustered target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{add_to_cart_table_id}` (
    page_url STRING,
    event_date DATE,
    add_to_cart_events INT64
)
PARTITION BY event_date
CLUSTER BY page_url;

-- Merge the new data with the existing data
MERGE `{add_to_cart_table_id}` T
USING `{dataset_id}.temp_aggregated_product_added_to_cart` S
ON T.page_url = S.page_url AND T.event_date = S.event_date
WHEN MATCHED THEN
  UPDATE SET T.add_to_cart_events = T.add_to_cart_events + S.add_to_cart_events
WHEN NOT MATCHED THEN
  INSERT (page_url, event_date, add_to_cart_events)
  VALUES (S.page_url, S.event_date, S.add_to_cart_events);

-- Update the last processed time in the metadata table
UPDATE `{dataset_id}.metadata_table`
SET last_event_time = (SELECT MAX(max_event_time) FROM `{dataset_id}.temp_aggregated_product_added_to_cart`)
WHERE query_name = 'add_to_cart_query';
"""

sessions_query = f"""
DECLARE last_processed_time TIMESTAMP;

-- Get the last processed time for the 'sessions_query'
SET last_processed_time = (SELECT COALESCE(MAX(last_event_time), TIMESTAMP('1970-01-01')) FROM `{dataset_id}.metadata_table` WHERE query_name = 'sessions_query');

-- Create or replace temporary table with new data, partitioned and clustered
CREATE OR REPLACE TABLE `{dataset_id}.temp_aggregated_sessions`
PARTITION BY event_date
CLUSTER BY page_url AS
SELECT
  page_url,
  DATE(event_time) AS event_date,
  COUNT(DISTINCT session_id) AS number_of_sessions,
  MAX(event_time) AS max_event_time
FROM
  `{base_table_id}`
WHERE
  event_time > last_processed_time
GROUP BY
  page_url,
  event_date;

-- Create the partitioned and clustered target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{sessions_table_id}` (
    page_url STRING,
    event_date DATE,
    number_of_sessions INT64
)
PARTITION BY event_date
CLUSTER BY page_url;

-- Merge the new data with the existing data
MERGE `{sessions_table_id}` T
USING `{dataset_id}.temp_aggregated_sessions` S
ON T.page_url = S.page_url AND T.event_date = S.event_date
WHEN MATCHED THEN
  UPDATE SET T.number_of_sessions = T.number_of_sessions + S.number_of_sessions
WHEN NOT MATCHED THEN
  INSERT (page_url, event_date, number_of_sessions)
  VALUES (S.page_url, S.event_date, S.number_of_sessions);

-- Update the last processed time in the metadata table
UPDATE `{dataset_id}.metadata_table`
SET last_event_time = (SELECT MAX(max_event_time) FROM `{dataset_id}.temp_aggregated_sessions`)
WHERE query_name = 'sessions_query';
"""

total_revenue_query = f"""
DECLARE last_processed_time TIMESTAMP;

-- Fetch the last processed time
SET last_processed_time = (
  SELECT last_event_time 
  FROM `{dataset_id}.metadata_table` 
  WHERE query_name = 'total_revenue_query'
);

-- Temporary table to store new data, using subqueries to minimize scans
CREATE OR REPLACE TABLE `{dataset_id}.temp_aggregated_total_revenue`
PARTITION BY event_date
CLUSTER BY page_url, event_date AS
WITH new_events AS (
  SELECT
    page_url,
    DATE(event_time) AS event_date,
    order_value,
    event_time
  FROM
    `{base_table_id}`
  WHERE
    event_name = 'checkout_completed'
    AND event_time > last_processed_time
)
SELECT
  page_url,
  event_date,
  SUM(order_value) AS total_revenue,
  MAX(event_time) AS max_event_time
FROM
  new_events
GROUP BY
  page_url,
  event_date;

-- Create or update the total_revenue table if it does not exist
CREATE TABLE IF NOT EXISTS `{revenue_table_id}` (
  page_url STRING,
  event_date DATE,
  total_revenue FLOAT64
)
PARTITION BY event_date
CLUSTER BY page_url;

-- Merge the new data with the existing data
MERGE `{revenue_table_id}` T
USING `{dataset_id}.temp_aggregated_total_revenue` S
ON T.page_url = S.page_url AND T.event_date = S.event_date
WHEN MATCHED THEN
  UPDATE SET T.total_revenue = T.total_revenue + S.total_revenue
WHEN NOT MATCHED THEN
  INSERT (page_url, event_date, total_revenue)
  VALUES (S.page_url, S.event_date, S.total_revenue);

-- Update the last processed time in the metadata table
UPDATE `{dataset_id}.metadata_table`
SET last_event_time = (
  SELECT MAX(max_event_time) 
  FROM `{dataset_id}.temp_aggregated_total_revenue`
)
WHERE query_name = 'total_revenue_query';
"""

scroll_query = f"""
DECLARE last_processed_time TIMESTAMP;

-- Get the last processed time for the 'daily_scroll_query'
SET last_processed_time = (
  SELECT COALESCE(MAX(last_event_time), TIMESTAMP('1970-01-01'))
  FROM `{dataset_id}.metadata_table`
  WHERE query_name = 'scroll_query'
);

-- Create or replace temporary table with new data, partitioned and clustered
CREATE OR REPLACE TABLE `{dataset_id}.temp_aggregated_scroll`
PARTITION BY event_date
CLUSTER BY page_url AS
SELECT
  page_url,
  DATE(event_time) AS event_date,
  SUM(percent_scroll) AS total_scroll_sum,
  COUNT(*) AS total_events,
  MAX(event_time) AS max_event_time
FROM
  `{base_table_id}`
WHERE
  (event_name = 'page_scroll' OR event_name = 'page_viewed')
  AND event_time > last_processed_time
GROUP BY
  page_url,
  event_date;

-- Create the partitioned and clustered target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{scroll_table_id}` (
    page_url STRING,
    event_date DATE,
    total_scroll_sum FLOAT64,
    total_events INT64
)
PARTITION BY event_date
CLUSTER BY page_url;

-- Merge the new data with the existing data
MERGE `{scroll_table_id}` T
USING `{dataset_id}.temp_aggregated_scroll` S
ON T.page_url = S.page_url AND T.event_date = S.event_date
WHEN MATCHED THEN
  UPDATE SET 
    T.total_scroll_sum = T.total_scroll_sum + S.total_scroll_sum,
    T.total_events = T.total_events + S.total_events
WHEN NOT MATCHED THEN
  INSERT (page_url, event_date, total_scroll_sum, total_events)
  VALUES (S.page_url, S.event_date, S.total_scroll_sum, S.total_events);

-- Update the last processed time in the metadata table
UPDATE `{dataset_id}.metadata_table`
SET last_event_time = (SELECT MAX(max_event_time) FROM `{dataset_id}.temp_aggregated_scroll`)
WHERE query_name = 'scroll_query';
"""