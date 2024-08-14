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

    def __init__(self, index:str, event_type:str, token_address:str, token_decimals:int):
        self.index = index
        self.event_type = event_type
        self.cloud_file_name = self.get_event_cloud_file_name()
        self.cloud_bucket_name = 'cooldowns2'
        self.token_address = token_address

    def get_event_cloud_file_name(self):
        cloud_file_name = self.index + '_' + self.event_type + '_events.zip'

        return cloud_file_name
    
    # # will return a dataframe only containing the asset we specify
    def get_asset_df(self):
        df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)
        df = df.loc[df['token_address'] == self.token_address]

        df = df[['from_address', 'to_address', 'tx_hash', 'token_address', 'reserve_address', 'token_volume', 'event_type']]
        return df
    
    def attribute_positive_and_negative_token_flows(self, df):

        df = df.loc[df['event_type'].isin(['deposit','withdraw'])]
        negative_flow_event_list = ['withdraw']

        df['token_volume'] = df['token_volume'].astype(float)

        df.loc[df['event_type'].isin(negative_flow_event_list), 'token_volume'] *= -1
        
        return df
    

    def run_all_modules(self):
        self.run_all_lend_event_tracking()
        self.lend_revenue_object.run_all_lend_revenue()
        self.cdp_object.run_all_cdp_tracking()
        self.cdp_revenue_object.run_all_cdp_revenue()
        total_revenue.run_all()
        return