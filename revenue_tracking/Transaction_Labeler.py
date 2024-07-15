import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Transaction_Labeler(Protocol_Data_Provider.Protocol_Data_Provider):
    
    def __init__(self, protocol_data_provider_address: str, rpc_url: str, df: pd.DataFrame, index: str, gateway_address: str, treasury_address: str):
        
        self.protocol_data_provider_address = protocol_data_provider_address
        self.rpc_url = rpc_url
        self.web3 = lph.get_web_3(rpc_url)
        self.abi = self.get_protocol_data_provider_abi()
        self.lending_pool_address = self.get_lending_pool_address()
        self.index = index
        self.gateway_address = gateway_address
        self.treasury_address = treasury_address
        self.null_address = '0x0000000000000000000000000000000000000000'
    
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
        # temp_df = df.loc[df['to_address'].isin(unique_user_list)]
        
        # # handles deposit labeling
        temp_df = df.loc[df['token_address'].isin(deposit_token_list)]
        deposit_df = temp_df.loc[(temp_df['from_address'] == self.gateway_address) | (temp_df['from_address'] == self.null_address)]
        deposit_df = deposit_df.loc[(deposit_df['to_address'] != self.gateway_address) & (deposit_df['to_address'] != self.treasury_address)]

        # # handles withdraw labeling
        temp_df = df.loc[df['token_address'].isin(deposit_token_list)]
        withdraw_df = temp_df.loc[(temp_df['to_address'] == self.gateway_address) | (temp_df['to_address'] == self.null_address)]
        withdraw_df = withdraw_df.loc[withdraw_df['from_address'] != self.gateway_address]

        # # handles revenue labeling
        revenue_df = temp_df.loc[(temp_df['to_address'] == self.treasury_address) &  (temp_df['from_address'] == self.null_address)]

        # # handles borrow labeling
        temp_df = df.loc[df['token_address'].isin(borrow_token_list)]
        borrow_df = temp_df.loc[(temp_df['from_address'] == self.gateway_address) | (temp_df['from_address'] == self.null_address)]
        borrow_df = borrow_df.loc[(borrow_df['to_address'] != self.gateway_address) & (borrow_df['to_address'] != self.treasury_address)]

        # # handles repay labeling
        temp_df = df.loc[df['token_address'].isin(borrow_token_list)]
        repay_df = temp_df.loc[(temp_df['to_address'] == self.gateway_address) | (temp_df['to_address'] == self.null_address)]
        repay_df = repay_df.loc[repay_df['from_address'] != self.gateway_address]

        # # does the actual labeling
        deposit_df['event_type'] = 'deposit'
        withdraw_df['event_type'] = 'withdraw'
        revenue_df['event_type'] = 'revenue'
        borrow_df['event_type'] = 'borrow'
        repay_df['event_type'] = 'repay'

        print(len(deposit_df), len(withdraw_df), len(revenue_df), len(borrow_df), len(repay_df))

        combo_df = pd.concat([deposit_df, withdraw_df, revenue_df, borrow_df, repay_df])
        combo_df = combo_df.drop_duplicates(subset=['to_address', 'from_address', 'tx_hash', 'token_address', 'token_volume'])

        return combo_df