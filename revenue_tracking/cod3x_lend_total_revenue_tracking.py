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

    df['daily_revenue_per_token'] = df['daily_revenue_per_token'].astype(float)
    daily_revenue_df = df.groupby(['day'])['daily_revenue_per_token'].sum().reset_index()

    return daily_revenue_df

# # gets our cumulative revenue per day
def get_total_aggregate_revenue_per_day(df):

    df['total_revenue'] = df['daily_revenue_per_token'].cumsum()

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
    
# # finds our combined daily, cumulative, and moving averages of revenue
def run_all():
    protocol_revenue_list = get_protocol_revenue_filename_list()
    df = get_combined_revenue_df(protocol_revenue_list)
    df = get_daily_aggregate_revenue(df)
    df = get_total_aggregate_revenue_per_day(df)

    df.rename(columns = {'daily_revenue_per_token':'daily_revenue'}, inplace = True)

    df = df[['day', 'daily_revenue', 'total_revenue']]

    df = set_n_days_avg_revenue(df, '7_days_ma_revenue', 7)
    df = set_n_days_avg_revenue(df, '30_days_ma_revenue', 30)
    df = set_n_days_avg_revenue(df, '90_days_ma_revenue', 90)
    df = set_n_days_avg_revenue(df, '180_days_ma_revenue', 180)

    cs.df_write_to_cloud_storage_as_zip(df, 'aggregate_lend_revenue.zip', 'cooldowns2')

    return df