import requests
import pandas as pd
from io import StringIO

def get_cdx_usd_revenue_df():
    
    df = pd.DataFrame()

    url = 'https://api.dune.com/api/v1/query/4289320/results/csv'
    headers = {
        'X-Dune-API-Key': ''
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

def run_all():

    df = get_cdx_usd_revenue_df()

    df = format_df(df)

    return df



# df = get_cdx_usd_revenue_df()
# print(df)