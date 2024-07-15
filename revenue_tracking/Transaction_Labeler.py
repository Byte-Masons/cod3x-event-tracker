import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Transaction_Labeler(Protocol_Data_Provider.Protocol_Data_Provider):
    
    def __init__(self, protocol_data_provider_address: str, rpc_url: str, df: pd.DataFrame, index: str):
        
        self.protocol_data_provider_address = protocol_data_provider_address
        self.rpc_url = rpc_url
        self.web3 = lph.get_web_3(rpc_url)
        self.abi = self.get_protocol_data_provider_abi()
        self.lending_pool_address = self.get_lending_pool_address()
        self.index = index
    
    def label_events(self, df):

        self.get_reserve_address_list()
        self.get_receipt_token_list()

        deposit_token_list = self.get_a_token_list()
        borrow_token_list = self.get_v_token_list()

        unique_user_df = sql.set_unique_users(self.index)

        unique_user_list = unique_user_df['to_address'].to_list()

        combo_df = pd.DataFrame()
        temp_df = pd.DataFrame()

        # # handles deposits and borrows
        temp_df = df.loc[df['to_address'].isin(unique_user_list)]
        deposit_df = temp_df.loc[temp_df['token_address'].isin(deposit_token_list)]
        borrow_df = temp_df.loc[temp_df['token_address'].isin(borrow_token_list)]

        # # handles withdrawals and repays
        temp_df = df.loc[df['from_address'].isin(unique_user_list)]
        withdraw_df = temp_df.loc[temp_df['token_address'].isin(deposit_token_list)]
        repay_df = temp_df.loc[temp_df['token_address'].isin(borrow_token_list)]

        deposit_df['event_type'] = 'deposit'
        borrow_df['event_type'] = 'borrow'
        withdraw_df['event_type'] = 'withdraw'
        repay_df['event_type'] = 'borrow'

        combo_df = pd.concat([deposit_df, borrow_df, withdraw_df, repay_df])
        combo_df = combo_df.drop_duplicates(subset=['to_address', 'from_address', 'tx_hash', 'token_address', 'token_volume'])

        return combo_df