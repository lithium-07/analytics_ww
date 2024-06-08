import json

def load_config(section: str):
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config.get(section, {})

config = load_config(section='bigquery')

dataset_id = config['dataset_id']
add_to_cart_table_id = ".".join([dataset_id, config['add_to_cart_table']])
sessions_table_id = ".".join([dataset_id, config['sessions_table']])
checkout_completed_table_id = ".".join([dataset_id, config['checkout_completed_table']])
revenue_table_id = ".".join([dataset_id, config['revenue_table']])
scroll_table_id = ".".join([dataset_id, config['scroll_values_table']])
base_table_id = ".".join([dataset_id, config['base_table']])
creds=config['credentials_file']
scopes=config['scopes']