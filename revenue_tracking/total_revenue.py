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

# # will return the list of events for a specified event_type
def get_specified_event_list(protocol_list, event_type):

    event_list = [name for name in protocol_list if 'aggregate' not in name.lower() and 'revenue' not in name.lower()]

    return event_list

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

# # sets a moving average for our dataframe
def set_deployment_n_days_avg_revenue(df, new_column_name, lookback_days):
    # Sort the dataframe by day and deployment
    df = df.sort_values(by=['day', 'deployment'])

    # Group by day and deployment, and calculate the daily revenue
    day_deployment_revenue = df.groupby(['day', 'deployment'])['daily_revenue'].sum().reset_index()

    # Create a helper column for sorting within each deployment group
    # day_deployment_revenue['day'] = pd.to_datetime(day_deployment_revenue['day'])
    day_deployment_revenue['rank'] = day_deployment_revenue.groupby('deployment')['day'].rank(method='dense')

    # Calculate the rolling average for each deployment
    day_deployment_revenue[new_column_name] = day_deployment_revenue.groupby('deployment')['daily_revenue'].transform(
        lambda x: x.rolling(window=lookback_days, min_periods=1).mean()
    )

    # Merge the rolling average back to the original dataframe
    df = df.merge(day_deployment_revenue[['day', 'deployment', new_column_name]], 
                  on=['day', 'deployment'], 
                  how='left')

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

# # will set all of our moving averages in our dataframe
def get_ma_df(df):
    column_names_list = ['7_days_ma_revenue','30_days_ma_revenue','90_days_ma_revenue','180_days_ma_revenue']
    lookback_days_list = [7, 30, 90, 180]

    i = 0

    while i < len(column_names_list):
        column_name = column_names_list[i]
        lookback_days = lookback_days_list[i]

        df = set_deployment_n_days_avg_revenue(df, column_name, lookback_days)

        i += 1

    # Group by day and sum the MA metrics across all deployments
    total_ma_df = df.groupby('day')[column_names_list].sum().reset_index()
    
    # Rename the columns to indicate they are totals
    total_ma_columns = ['total_' + col for col in column_names_list]
    total_ma_df.columns = ['day'] + total_ma_columns
    
    # Merge the total MA metrics back to the original dataframe
    df = df.merge(total_ma_df, on='day', how='left')

    return df

# # will return a list of file names for a given event type
def get_event_type_list(event_type):
    df = lph.get_lp_config_df()
    
    df = df[df['index'].str.contains('lend', case=False, na=False)]

    protocol_list = df['index'].unique()

    # # reformats our list to their revenue file names
    protocol_list = [protocol.split('_events')[0] for protocol in protocol_list]
    protocol_list = [protocol + '_' + event_type + '_events.zip' for protocol in protocol_list]

    return protocol_list


# # finds the sum of daily_revenue across all deployments per day
# # finds the max total_revenue value per day
# # finds the first ma metric per day, these are all the same per day, so arbitrarily picking the first should be fine
def aggregate_daily_revenue(df):
    
    # Group by day and aggregate
    aggregated_df = df.groupby('day').agg({
        'daily_revenue': 'sum',
        'total_revenue': 'max',
        '7_days_ma_revenue': 'first',
        '30_days_ma_revenue': 'first',
        '90_days_ma_revenue': 'first',
        '180_days_ma_revenue': 'first'
    }).reset_index()
    
    return aggregated_df

# # finds our combined daily, cumulative, and moving averages of revenue
def run_all():
    protocol_revenue_list = cs.get_all_prefix_files('cooldowns2', 'revenue')

    lend_df = get_general_revenue_df(protocol_revenue_list, 'lend')
    lend_df['revenue_type'] = 'reserve_factor'
    cdp_df = get_general_revenue_df(protocol_revenue_list, 'cdp')
    cdp_df['revenue_type'] = 'cdp_mint_fee'
    o_token_df = get_general_revenue_df(protocol_revenue_list, 'o_token')

    concat_df = pd.concat([lend_df, cdp_df, o_token_df])

    deployment_df = concat_df

    deployment_df = deployment_df.groupby(['day','deployment'])['daily_revenue'].sum().reset_index()
    deployment_df['total_deployment_revenue'] = deployment_df.groupby(['deployment'])['daily_revenue'].cumsum()
    deployment_df['total_aggregate_revenue'] = deployment_df['daily_revenue'].cumsum()

    deployment_df = get_ma_df(deployment_df)

    # # gets the total revenue per token
    lend_revenue_list = cs.get_all_prefix_files('cooldowns2', 'lend_revenue')
    token_revenue_df = get_combined_revenue_df(lend_revenue_list)
    token_revenue_df = get_total_revenue_per_token(token_revenue_df)

    # # makes our data_card df
    revenue_data_card_df = deployment_df[['day', 'daily_revenue', 'total_aggregate_revenue', 'total_7_days_ma_revenue','total_30_days_ma_revenue','total_90_days_ma_revenue','total_180_days_ma_revenue']]
    revenue_data_card_df.rename(columns = {'total_aggregate_revenue':'total_revenue', 'total_7_days_ma_revenue': '7_days_ma_revenue', 'total_30_days_ma_revenue': '30_days_ma_revenue', 'total_90_days_ma_revenue': '90_days_ma_revenue','total_180_days_ma_revenue': '180_days_ma_revenue'}, inplace = True)
    revenue_data_card_df = aggregate_daily_revenue(revenue_data_card_df)
    revenue_data_card_df = get_data_card_df(revenue_data_card_df)

    # # revenue per revenue_type
    concat_df = concat_df.sort_values(by='day')
    concat_df['cumulative_revenue'] = concat_df.groupby('revenue_type')['daily_revenue'].cumsum()
    concat_df = concat_df[['day', 'revenue_type', 'daily_revenue', 'cumulative_revenue']]

    # # writes our juicy dataframes to the cloud
    cs.df_write_to_cloud_storage_as_zip(deployment_df, 'combined_deployment_revenue.zip', 'cooldowns2')
    cs.df_write_to_cloud_storage_as_zip(token_revenue_df, 'total_revenue_per_token.zip', 'cooldowns2')
    cs.df_write_to_cloud_storage_as_zip(revenue_data_card_df, 'lend_revenue_data_card.zip', 'cooldowns2')
    cs.df_write_to_cloud_storage_as_zip(concat_df, 'revenue_by_type.zip', 'cooldowns2')

    return concat_df