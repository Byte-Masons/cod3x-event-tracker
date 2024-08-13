import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os
import time

protocol_df = lph.get_lp_config_df()

protocol_list = protocol_df['index'].unique()

# # gets a list of our actively tracked protocol deployments
def get_protocol_revenue_filename_list():
    df = lph.get_lp_config_df()
    
    df = df[df['index'].str.contains('lend', case=False, na=False)]

    protocol_list = df['index'].unique()

    # # reformats our list to their revenue file names
    protocol_list = [protocol.split('_events')[0] for protocol in protocol_list]
    protocol_list = [protocol + '_revenue.zip' for protocol in protocol_list]

    return protocol_list

# # will return a revenue list of only protocols of a certain type
# # will exclude any aggregate file names
def get_specified_revenue_file_list(protocol_revenue_list, revenue_type_string):

    specified_revenue_list = []

    for protocol in protocol_revenue_list:
        if revenue_type_string in protocol:
            if protocol.split('_')[0] != 'aggregate' and protocol.split('_')[0] != 'lend':
                specified_revenue_list.append(protocol)

    return specified_revenue_list

# # combined all of our revenue dataframes together
def get_combined_revenue_df(protocol_revenue_list):

    # # just gets the deployments name without any suffixes
    deployment_name_list = [protocol.split('_')[0] for protocol in protocol_revenue_list]

    df_list = []

    i = 0
    while i < len(protocol_revenue_list):
        revenue_cloud_name = protocol_revenue_list[i]
        revenue_deployment_name = deployment_name_list[i]

        df = cs.read_zip_csv_from_cloud_storage(revenue_cloud_name, 'cooldowns2')
        df['deployment'] = revenue_deployment_name

        df_list.append(df)

        time.sleep(0.1)
        i += 1
    
    df = pd.concat(df_list)

    return df

# # gets the daily_revenue per token irregardless of deployment
def get_daily_aggregate_revenue(df):

    df['daily_revenue'] = df['daily_revenue'].astype(float)
    daily_revenue_df = df.groupby(['day','deployment'])['daily_revenue'].max().reset_index()

    daily_revenue_df = daily_revenue_df.groupby(['day','deployment'])['daily_revenue'].sum().reset_index()

    return daily_revenue_df

# # gets our cumulative revenue per day
def get_total_aggregate_revenue_per_day(df):

    # # deployment level
    df['total_deployment_revenue'] = df.groupby('deployment')['daily_revenue'].cumsum()
    
    # # all deployments combined
    df['total_revenue'] = df['daily_revenue'].cumsum()

    return df

# # sets a moving average for our dataframe
def set_n_days_avg_revenue(df, new_column_name, lookback_days):

    df = df.sort_values(by=['day'], ascending=True)

    day_revenue_df = df.groupby(['day'])['daily_revenue'].max().reset_index()

    # Calculate the rolling average
    day_revenue_df[new_column_name] = day_revenue_df['daily_revenue'].rolling(window=lookback_days, min_periods=1).mean()

    # Merge the rolling average back to the original dataframe
    df = df.merge(day_revenue_df[['day', new_column_name]], on='day', how='left')

    return df

# # will find the total revenue earned per token
def get_total_revenue_per_token(df):
    
    df['daily_revenue_per_token'] = df['daily_revenue_per_token'].astype(float)

    token_revenue_df = df.groupby(['token_name'])['daily_revenue_per_token'].sum().reset_index()

    token_revenue_df.rename(columns = {'daily_revenue_per_token':'token_revenue'}, inplace = True)

    token_revenue_df = token_revenue_df.sort_values(by='token_revenue', ascending=False)

    return token_revenue_df

# # a general function that will give us the total revenue given a specified number of days
def get_n_days_revenue(df, new_column_name, lookback_days):

    df['daily_revenue'] = df['daily_revenue'].astype(float)

    df = df.sort_values(by=['day'], ascending=False)

    df = df[:lookback_days]

    df[new_column_name] = df['daily_revenue'].sum()

    return df

# # makes our moving average columns
def get_ma_df(df):
    df = set_n_days_avg_revenue(df, '7_days_ma_revenue', 7)
    df = set_n_days_avg_revenue(df, '30_days_ma_revenue', 30)
    df = set_n_days_avg_revenue(df, '90_days_ma_revenue', 90)
    df = set_n_days_avg_revenue(df, '180_days_ma_revenue', 180)

    return df

# # gets our data_card_df
def get_data_card_df(df):

    total_revenue = df['total_revenue'].max()
    revenue_data_card_df = get_n_days_revenue(df, '180_day_revenue', 180)
    revenue_data_card_df = get_n_days_revenue(revenue_data_card_df, '90_day_revenue', 90)
    revenue_data_card_df = get_n_days_revenue(revenue_data_card_df, '30_day_revenue', 30)
    revenue_data_card_df = get_n_days_revenue(revenue_data_card_df, '30_day_revenue', 30)
    revenue_data_card_df = get_n_days_revenue(revenue_data_card_df, '7_day_revenue', 7)
    revenue_data_card_df = get_n_days_revenue(revenue_data_card_df, 'todays_revenue', 1)

    revenue_data_card_df = revenue_data_card_df[['day', 'todays_revenue', '7_day_revenue', '30_day_revenue', '90_day_revenue', '180_day_revenue']]

    revenue_data_card_df = revenue_data_card_df[:1]

    revenue_data_card_df['total_revenue'] = total_revenue

    revenue_data_card_df['target_daily_revenue'] = 7000

    return revenue_data_card_df

# # essentially gets any of our revenue dataframes
def get_general_revenue_df(protocol_revenue_list, revenue_type):
    
    revenue_list = get_specified_revenue_file_list(protocol_revenue_list, revenue_type)

    df = get_combined_revenue_df(revenue_list)

    try:
        df = df.drop_duplicates(subset=['day', 'token_address', 'deployment'], keep='last')
    except:
        df = df.drop_duplicates(subset=['day', 'deployment'], keep='last')

    df = get_daily_aggregate_revenue(df)

    df = get_total_aggregate_revenue_per_day(df)

    df['revenue_type'] = revenue_type

    return df

# # finds our combined daily, cumulative, and moving averages of revenue
def run_all():
    protocol_revenue_list = cs.get_all_revenue_files('cooldowns2')

    lend_df = get_general_revenue_df(protocol_revenue_list, 'lend')
    cdp_df = get_general_revenue_df(protocol_revenue_list, 'cdp')
    o_token_df = get_general_revenue_df(protocol_revenue_list, 'o_token')

    concat_df = pd.concat([lend_df, cdp_df, o_token_df])

    deployment_df = concat_df

    deployment_df = deployment_df.groupby(['day','deployment'])['daily_revenue'].sum().reset_index()
    deployment_df['total_deployment_revenue'] = deployment_df.groupby(['deployment'])['daily_revenue'].cumsum()
    deployment_df['total_aggregate_revenue'] = deployment_df['daily_revenue'].cumsum()

    # # df.rename(columns = {'daily_revenue_per_token':'daily_revenue'}, inplace = True)

    # df = df[['day', 'daily_revenue', 'total_revenue']]

    # df = get_ma_df(df)

    # revenue_data_card_df = get_data_card_df(df)

    cs.df_write_to_cloud_storage_as_zip(deployment_df, 'combined_deployment_revenue.zip', 'cooldowns2')
    # cs.df_write_to_cloud_storage_as_zip(token_revenue_df, 'total_revenue_per_token.zip', 'cooldowns2')
    # cs.df_write_to_cloud_storage_as_zip(revenue_data_card_df, 'lend_revenue_data_card.zip', 'cooldowns2')

    return concat_df