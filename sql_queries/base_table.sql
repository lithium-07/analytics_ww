CREATE TABLE `your_project.your_dataset.your_table` (
    platform STRING,
    event_time TIMESTAMP NOT NULL,
    event_name STRING NOT NULL,
    event_id STRING NOT NULL,
    user_id STRING,
    session_id STRING,
    page_url STRING,
    order_id STRING,
    order_value FLOAT,
    city STRING,
    country STRING,
    user_agent STRING,
    language STRING,
    currency STRING,
    user_email STRING,
    percent_scroll FLOAT,
    product_id STRING,
    product_price FLOAT,
    page_title STRING
)
PARTITION BY DATE(event_time)
CLUSTER BY page_url, event_name;