import requests
import pandas as pd
from io import StringIO
import time

DUNE_KEY = 'cp6G2OF5wnUnpc4xblcBc5nKq43As6UM'
QUERY_ID = 4289320

# # executes our query to refresh data
def execute_query():

    url = f"https://api.dune.com/api/v1/query/{QUERY_ID}/execute"

    headers = {"X-DUNE-API-KEY": DUNE_KEY}

    response = requests.request("POST", url, headers=headers)

    execution_id = response.json()['execution_id']

    return execution_id

def get_query_status(execution_id):
    """
    Takes in an execution ID.
    Fetches the status of query execution using the API
    Returns the status response object
    """
    url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"

    headers = {"X-DUNE-API-KEY": DUNE_KEY}

    response = requests.request("GET", url, headers=headers)

    return response.json()['is_execution_finished']

def get_cdx_usd_revenue_df():
    
    df = pd.DataFrame()

    url = f'https://api.dune.com/api/v1/query/{QUERY_ID}/results/csv'
    headers = {
        'X-Dune-API-Key': DUNE_KEY
    }
    params = {
        'limit': 25000
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        # Convert the CSV string data to a pandas DataFrame
        df = pd.read_csv(StringIO(response.text))
        df['day'] = pd.to_datetime(df['day']).dt.strftime('%Y-%m-%d')
        
        # # Optional: Display first few rows to verify the data
        # print(df.head())
    else:
        print(f"Request failed with status code: {response.status_code}")

    df['day'] = pd.to_datetime(df['day']).dt.strftime('%Y/%m/%d')

    return df

# # formats our dataframe to match our other revenue dataframes
def format_df(df):

    df['deployment'] = 'make.fun'

    df['daily_revenue'] = df['daily_swap_fees']

    df['total_deployment_revenue'] = df['cumulative_swap_fees']

    df['revenue_type'] = 'cdxUSD_amo'

    df['total_revenue'] = df['total_deployment_revenue']

    df = df[['day', 'deployment', 'daily_revenue', 'total_deployment_revenue', 'total_revenue', 'revenue_type']]

    return df

# # executes our query to refresh the data
# # then returns a nicely formatted dataframe
def run_all():

    execution_id = execute_query()

    query_is_finished = get_query_status(execution_id)

    # # will wait for our query to finish
    while query_is_finished == False:
        time.sleep(2.5)
        query_is_finished = get_query_status(execution_id)

    df = get_cdx_usd_revenue_df()

    df = format_df(df)

    return df

# df = run_all()
# print(df)