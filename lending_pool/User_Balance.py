import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking, total_revenue, cdp_mint_fee_revenue_tracking
from cdp import CDP
from cloud_storage import cloud_storage as cs

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class User_Balance():

    def __init__(self, index:str, event_type:str, token_address:str, token_name:str, token_decimals:int, leverager_address:str, contract_blacklist:list):
        self.index = index
        self.event_type = event_type
        self.cloud_file_name = self.get_event_cloud_file_name()
        self.cloud_bucket_name = 'cooldowns2'
        self.token_address = token_address
        self.token_name = token_name
        self.decimals = 10 ** token_decimals
        self.leverager_address = leverager_address
        self.contract_blacklist = contract_blacklist

        self.balance_file_name = self.token_name + '_balances.zip'

    def get_event_cloud_file_name(self):
        cloud_file_name = self.index + '_' + self.event_type + '_events.zip'

        return cloud_file_name
    
    # # will return a dataframe only containing the asset we specify
    def get_asset_df(self):
        df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)
        max_block = df['block_number'].astype(int).max()
        df = df.loc[df['token_address'] == self.token_address]

        df['max_block'] = max_block
        df = df[['from_address', 'to_address', 'tx_hash', 'token_address', 'reserve_address', 'token_volume', 'timestamp', 'block_number', 'max_block', 'event_type']]

        return df

    # # will classify leverager transaction as a deposit or withdraw
    def set_leverager_flow(self, df):

        df.loc[(df['to_address'] == self.leverager_address), 'event_type'] = 'withdraw'
        
        return df
    
    def attribute_positive_and_negative_token_flows(self, df):

        df = df.loc[df['event_type'].isin(['deposit','withdraw', 'unknown'])]
        
        df = self.set_leverager_flow(df)

        negative_flow_event_list = ['withdraw']

        df['token_volume'] = df['token_volume'].astype(float)

        df.loc[df['event_type'].isin(negative_flow_event_list), 'token_volume'] *= -1
        
        return df
    
    # # will essentially make a 'user_address' associated with each transaction
    def get_user_address_list(self, df):
        
        unique_user_list = df['to_address'].unique()

        return unique_user_list

    # # will use the from_address and to_address columns to make a single 'user_address' column attributed to each transaction
    def set_user_transactions(self, df):

        df.loc[df['to_address'].isin(self.contract_blacklist), 'user_address'] = df['from_address']
        df.loc[df['from_address'].isin(self.contract_blacklist), 'user_address'] = df['to_address']
        
        df = df.loc[~df['user_address'].isin(self.contract_blacklist)]

        return df
    
    # # will set the cumulative balance of each user over time
    def set_user_balances_over_time(self, df):
        df['timestamp'] = df['timestamp'].astype(float)

        df = df.sort_values(by=['timestamp'], ascending=True)
        
        # Group by user_address and token_address
        grouped = df.groupby(['user_address', 'token_address'])
        
        # Calculate cumulative sum of token_volume for each group
        df['balance'] = grouped['token_volume'].cumsum()

        df = df.sort_values(['user_address', 'token_address', 'timestamp'])

        df['balance'] /= self.decimals

        df.loc[df['balance'] < 0, 'balance'] = 0

        return df
    
    def run_all(self):
        df = self.get_asset_df()
        df = self.attribute_positive_and_negative_token_flows(df)
        df = self.set_user_transactions(df)
        df = df[['user_address', 'tx_hash', 'token_address', 'reserve_address', 'token_volume', 'timestamp', 'block_number', 'max_block', 'event_type']]
        df = df.drop_duplicates(subset=['user_address', 'tx_hash', 'token_address', 'reserve_address', 'token_volume', 'timestamp', 'block_number', 'event_type'])
        df = self.set_user_balances_over_time(df)

        df.rename(columns = {'user_address':'user', 'balance':'effective_balance'}, inplace = True)
        df = df[df['user'].notna()]
        df = df.loc[df['user'] != '0x574F42132cB7C9f57Ac9B832960ee4e5Af807E54']
        cs.df_write_to_cloud_storage_as_zip(df, self.balance_file_name, self.cloud_bucket_name)
        
        return df