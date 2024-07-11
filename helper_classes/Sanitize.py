import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Sanitize(ERC_20.ERC_20):
    
    WAIT_TIME = 1

    def __init__(self, df: pd.DataFrame, rpc_url: str):
        
        self.df = df
        self.rpc_url = rpc_url
    
    # # makes our erc20 object for a given token_address
    def make_erc_20(self, token_address, rpc_url):

        erc_20_object = ERC_20.ERC_20(token_address, rpc_url)

        return erc_20_object
    

    # # makes a list of our dataframes token_addresses
    def make_erc_20_object_list(self):

        erc_20_object_list = []

        unique_token_address_list = self.df['token_address'].unique()

        for unique_token_address in unique_token_address_list:
            erc_20_object = self.make_erc_20(unique_token_address, self.rpc_url)

            erc_20_object_list.append(erc_20_object)

        self.erc_20_object_list = erc_20_object_list

        return erc_20_object_list
    
    # # makes sure that all of our reserve addresses are correct
    def sanitize_reserve_addresses(self):

        df = self.df

        erc_20_object_list = self.erc_20_object_list

        unique_token_address_list = self.df['token_address'].unique()

        i = 0

        for unique_token_address in unique_token_address_list:

            for erc_20_object in erc_20_object_list:
                if erc_20_object.token_address == unique_token_address:
                    reserve_address = erc_20_object.token_contract.functions.UNDERLYING_ASSET_ADDRESS().call()

                    df.loc[df['token_address'] == unique_token_address, 'reserve_address'] = reserve_address

                    time.sleep(0.2)

        self.df = df

        return

    # # updates the usd_token_amount
    def sanitize_df_decimal_columns(self):

        unique_reserve_address_list = self.df['reserve_address'].unique()

        reserve_erc_20_object_list = []

        self.df[['token_volume', 'asset_price']] = self.df[['token_volume', 'asset_price']].astype(float)
        
        df_list = []

        for token_address in unique_reserve_address_list:
            erc_20_object = self.make_erc_20(token_address, self.rpc_url)
            reserve_erc_20_object_list.append(erc_20_object)
            time.sleep(0.2)

        for reserve_erc_20_object in reserve_erc_20_object_list:
            df = self.df
            temp_df = df.loc[df['reserve_address'] == reserve_erc_20_object.token_address]
            
            temp_df['usd_token_amount'] = temp_df['token_volume'] / reserve_erc_20_object.decimals * temp_df['asset_price']

            df_list.append(temp_df)
        
        df = pd.concat(df_list)
        
        self.df = df

        return df
    
    # # runs all of our sanitation functions and returns our dataframe
    def sanitize_df(self):
        self.make_erc_20_object_list()
        self.sanitize_reserve_addresses()
        df = self.sanitize_df_decimal_columns()

        return df