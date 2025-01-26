import requests
import pandas as pd
from io import StringIO
import time as t
from datetime import datetime, time, date


# DUNE_KEY = 'cp6G2OF5wnUnpc4xblcBc5nKq43As6UM'
# QUERY_ID = 4289320

# DUNE_KEY = 'VXuyds2VnSrnvUYBYsdrcl4NecZZxMwP'
# QUERY_ID = 4613480

KEY_LIST = ['cp6G2OF5wnUnpc4xblcBc5nKq43As6UM', 'VXuyds2VnSrnvUYBYsdrcl4NecZZxMwP']
QUERY_LIST = [4289320, 4613480]

AMO_QUERY_ID_LIST = [4289320]
MEME_TOKEN_LP_QUERY_ID_LIST = [4613480]

# # executes our query to refresh data
def execute_query(query_id, dune_key):

    url = f"https://api.dune.com/api/v1/query/{query_id}/execute"

    headers = {"X-DUNE-API-KEY": dune_key}

    response = requests.request("POST", url, headers=headers)

    execution_id = response.json()['execution_id']

    return execution_id

def get_query_status(execution_id, dune_key):
    """
    Takes in an execution ID.
    Fetches the status of query execution using the API
    Returns the status response object
    """
    url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"

    headers = {"X-DUNE-API-KEY": dune_key}

    response = requests.request("GET", url, headers=headers)

    return response.json()['is_execution_finished']

def get_query_dataframe(query_id, dune_key):
    
    df = pd.DataFrame()

    url = f'https://api.dune.com/api/v1/query/{query_id}/results/csv'
    headers = {
        'X-Dune-API-Key': dune_key
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
def format_df(df, query_id):

    df['deployment'] = 'make.fun'
    
    df['daily_revenue'] = df['daily_swap_fees']

    df['total_deployment_revenue'] = df['cumulative_swap_fees']

    if query_id in AMO_QUERY_ID_LIST:
        df['revenue_type'] = 'cdxUSD_amo'

    elif query_id in MEME_TOKEN_LP_QUERY_ID_LIST:
        df['revenue_type'] = 'memecoin_lp'
    
    else:
        df['revenue_type'] = 'unknown'

    df['total_revenue'] = df['total_deployment_revenue']


    df = df[['day', 'deployment', 'daily_revenue', 'total_deployment_revenue', 'total_revenue', 'revenue_type']]

    return df

# # executes our query to refresh the data
# # then returns a nicely formatted dataframe
def run_all():

    df_list = []

    i = 0
    while i < len(QUERY_LIST):
        query_id = QUERY_LIST[i]
        dune_key = KEY_LIST[i]

        df = get_query_dataframe(query_id, dune_key)

        df = format_df(df, query_id)

        last_day_checked = df['day'].max()

        current_day = datetime.now().strftime('%Y/%m/%d')

        # # if today hasn't been checked
        if last_day_checked != current_day:
            
            # Get start of 16:00 (4 PM) for current day
            today = date.today()  # You'll need this first
            twenty_hour = datetime.combine(today, time(12, 0))  # 17:00
            twenty_hour_unix = int(twenty_hour.timestamp())

            # # we will only execute the query data if the current unix timestamp is greater than 16th hour of the day
            if datetime.now().timestamp() >= twenty_hour_unix:

                execution_id = execute_query(query_id, dune_key)

                query_is_finished = get_query_status(execution_id, dune_key)

                # # will wait for our query to finish
                while query_is_finished != True:
                    t.sleep(2.5)
                    query_is_finished = get_query_status(execution_id, dune_key)

                df = get_query_dataframe(query_id, dune_key)

                df = format_df(df, query_id)

        df_list.append(df)

        i += 1

    df = pd.concat(df_list)

    return df

# df = run_all()
# print(df)