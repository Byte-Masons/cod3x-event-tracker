import sys
import os
import time
import pandas as pd
import sqlite3

from lending_pool import lending_pool_helper as lph
from helper_classes import ERC_20, Protocol_Data_Provider, Sanitize
from cloud_storage import cloud_storage as cs
from sql_interfacer import sql
from revenue_tracking import Transaction_Labeler as tl

# # makes a dataframe from our .zip cloud file
def get_cloud_file_df(protocol_name):

    cloud_name = protocol_name + '_lend_events.zip'
    bucket_name = 'cooldowns2'

    df = cs.read_zip_csv_from_cloud_storage(cloud_name, bucket_name)

    return df

# # will combine dataframes of protocols that have multiple cloud files
def handle_protocol_multiple_file_df(protocol_name):

    df_list = []

    config_df = lph.get_protocol_config_df()

    protocol_name_list = config_df['protocol'].unique()

    protocol_list = []

    for protocol_config_name in protocol_name_list:

        # # only check configs with our protocol_name
        if protocol_config_name.startswith(protocol_name):
            if '_' not in protocol_config_name:
                protocol_list = [item for item in protocol_name_list if item.startswith(protocol_name)]

    
    if len(protocol_list) > 0:
        for protocol_name in protocol_list:
            temp_df = get_cloud_file_df(protocol_name)
            time.sleep(0.5)
            df_list.append(temp_df)

    df = pd.concat(df_list)

    # # drop some potential duplicates
    df = df.drop_duplicates(subset=['from_address', 'to_address', 'tx_hash', 'token_address', 'token_volume'])

    return df

# # deposit and borrow are positive
# # withdraw and repay are negative
def get_user_plus_minus_value_transfers(df):

    df.loc[df['event_type'].isin(['withdraw', 'repay']), 'token_volume'] *= -1

    return df

# # will return a dataframe of token_addresses and their most recent token_prices
def get_most_recent_token_prices_df(df):

    token_list = df['token_address'].unique()

    most_recent_price_list = []

    i = 0

    while i < len(token_list):

        df[['block_number', 'asset_price']] = df[['block_number', 'asset_price']].astype(float)
        token_address = token_list[i]

        temp_df = df.loc[df['token_address'] == token_address]

        temp_df = temp_df.sort_values(by='block_number', ascending=False)

        temp_df = temp_df[:1]

        most_recent_price = temp_df['asset_price'].unique()[0]

        most_recent_price_list.append(most_recent_price)

        i += 1
    
    df = pd.DataFrame()

    df['token_address'] = token_list
    df['most_recent_price'] = most_recent_price_list

    return df

# # will return a dataframe of the number of decimals that each token has
def get_token_decimal_df(df, config_df):

    rpc_url = config_df['rpc_url'].unique()[0]

    token_address_list = df['token_address'].tolist()

    decimal_list = []

    for token_address in token_address_list:
        token_contract = ERC_20.ERC_20(token_address, rpc_url)
        
        token_decimals = token_contract.decimals
        decimal_list.append(token_decimals)


    df['decimals'] = decimal_list

    return df

def get_user_balance_usd(df, pricing_df):

    df = df.loc[df['user_balance'] > 1]

    i = 0

    while i < len(pricing_df):
        token_address = pricing_df['token_address'].tolist()[i]
        token_decimals = pricing_df['decimals'].tolist()[i]
        token_price = pricing_df['most_recent_price'].tolist()[i]

        df.loc[df['token_address'] == token_address, 'user_balance'] /= token_decimals
        df.loc[df['token_address'] == token_address, 'user_balance'] *= token_price

        i += 1

    return df

# # groups by user and tokens to find their balances by essentially summing together each user_token combination
def get_user_token_combo_balances(df):

    df['user_address'] = df['to_address']

    # Group by both columns and sum token_volume
    user_balances = df.groupby(['user_address', 'token_address'])['token_volume'].sum().reset_index()

    # Rename the summed column to user_balance
    user_balances = user_balances.rename(columns={'token_volume': 'user_balance'})

    return user_balances


def run_all():

    config_df = lph.get_protocol_config_df()

    protocol_list = config_df['protocol'].tolist()
    rpc_list = config_df['rpc_url'].tolist()
    wait_time_list = config_df['wait_time'].tolist()

    i = 0

    # df_list = [pd.read_csv('./test_test.csv')]

    df_list = []

    while i < len(config_df):

        protocol_name = protocol_list[i]
        rpc_url = rpc_list[i]
        wait_time = wait_time_list[i]
        
        if '_' not in protocol_name:
            event_df = handle_protocol_multiple_file_df(protocol_name)

            event_df['protocol'] = protocol_name

            df = event_df

            # # eliminates any revenue rows since we are just trying to track user balances
            df = df.loc[df['event_type'] != 'revenue']

            df = df.drop_duplicates(subset=['from_address', 'to_address', 'tx_hash', 'token_address', 'token_volume'])

            most_recent_pricing_df = get_most_recent_token_prices_df(df)

            # # adds decimals to our pricing df
            most_recent_pricing_df = get_token_decimal_df(most_recent_pricing_df, config_df)

            # # temporarily to check net deposits
            df = df.loc[df['event_type'].isin(['deposit', 'withdraw'])]

            df = df.loc[df['token_address'].isin(['0xe7334Ad0e325139329E747cF2Fc24538dD564987', '0x02CD18c03b5b3f250d2B29C87949CDAB4Ee11488', '0x9c29a8eC901DBec4fFf165cD57D4f9E03D4838f7', '0x272CfCceFbEFBe1518cd87002A8F9dfd8845A6c4', '0x58254000eE8127288387b04ce70292B56098D55C', '0xe3f709397e87032E61f4248f53Ee5c9a9aBb6440', '0xC17312076F48764d6b4D263eFdd5A30833E311DC', '0x4522DBc3b2cA81809Fa38FEE8C1fb11c78826268', '0x0F4f2805a6d15dC534d43635314444181A0e82CD'])]
            
            # # debits and credits
            df = get_user_plus_minus_value_transfers(df)

            # # user balancer per token
            df = get_user_token_combo_balances(df)

            # # net usd balance per user
            df = get_user_balance_usd(df, most_recent_pricing_df)

        if len(df) > 0:
            df_list.append(df)
        i += 1

    df = pd.concat(df_list)
    
    # # # eliminates any revenue rows since we are just trying to track user balances
    # df = df.loc[df['event_type'] != 'revenue']

    # df = df.drop_duplicates(subset=['from_address', 'to_address', 'tx_hash', 'token_address', 'token_volume'])

    # most_recent_pricing_df = get_most_recent_token_prices_df(df)

    # # # adds decimals to our pricing df
    # most_recent_pricing_df = get_token_decimal_df(most_recent_pricing_df, config_df)

    # # # temporarily to check net deposits
    # df = df.loc[df['event_type'].isin(['deposit', 'withdraw'])]

    # df = df.loc[df['token_address'].isin(['0xe7334Ad0e325139329E747cF2Fc24538dD564987', '0x02CD18c03b5b3f250d2B29C87949CDAB4Ee11488', '0x9c29a8eC901DBec4fFf165cD57D4f9E03D4838f7', '0x272CfCceFbEFBe1518cd87002A8F9dfd8845A6c4', '0x58254000eE8127288387b04ce70292B56098D55C', '0xe3f709397e87032E61f4248f53Ee5c9a9aBb6440', '0xC17312076F48764d6b4D263eFdd5A30833E311DC', '0x4522DBc3b2cA81809Fa38FEE8C1fb11c78826268', '0x0F4f2805a6d15dC534d43635314444181A0e82CD'])]
    
    # df = get_user_plus_minus_value_transfers(df)

    # df = get_user_token_combo_balances(df)

    # df = get_user_balance_usd(df, most_recent_pricing_df)

    return df