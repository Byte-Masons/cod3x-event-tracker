import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Transaction_Labeler(Protocol_Data_Provider.Protocol_Data_Provider):
    
    def __init__(self, protocol_data_provider_address: str, rpc_url: str, index: str, gateway_address: str, treasury_address: str):
        
        self.protocol_data_provider_address = protocol_data_provider_address
        self.rpc_url = rpc_url
        self.web3 = lph.get_web_3(rpc_url)
        self.abi = self.get_protocol_data_provider_abi()
        self.lending_pool_address = self.get_lending_pool_address()
        self.index = index
        self.gateway_address = gateway_address
        self.treasury_address = treasury_address
        self.null_address = '0x0000000000000000000000000000000000000000'
        
        self.reserve_address_list = self.get_reserve_address_list()
        self.receipt_token_list = self.get_receipt_token_list()

        self.deposit_token_list = self.get_a_token_list()
        self.borrow_token_list = borrow_token_list = self.get_v_token_list()
    
    def label_events(self, df):

        # self.get_reserve_address_list()
        # self.get_receipt_token_list()

        deposit_token_list = self.deposit_token_list
        borrow_token_list = self.borrow_token_list

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

        combo_df = pd.concat([deposit_df, withdraw_df, revenue_df, borrow_df, repay_df])
        combo_df = combo_df.drop_duplicates(subset=['to_address', 'from_address', 'tx_hash', 'token_address', 'token_volume'])

        # Create a new DataFrame with all rows from df and event_type as 'unknown'
        result_df = df.copy()
        result_df['event_type'] = 'unknown'

        # Update the event_type for rows that are in combo_df
        result_df.update(combo_df[['to_address', 'from_address', 'tx_hash', 'token_address', 'token_volume', 'event_type']])

        return result_df
    
    # # sets our rolling balance for each of our event types
    def set_rolling_volume(self, df):
        
        df[['timestamp', 'usd_token_amount']] = df[['timestamp', 'usd_token_amount']].astype(float)


        df = df.sort_values(by=['timestamp'], ascending=True)

        event_list = df['event_type'].unique()

        for event in event_list:
            new_column_name = event + '_rolling_volume'
            df.loc[df['event_type'] == event, new_column_name] = df.loc[df['event_type'] == event]['usd_token_amount'].cumsum()
        
        return df
    
    # # finds the change in volume per day for each event type
    def set_volume_diffs(self, df):

        unique_event_list = df['event_type'].unique()

        rolling_volume_column_name_list = [event + '_rolling_volume' for event in unique_event_list]

        print(rolling_volume_column_name_list)

        df[rolling_volume_column_name_list] = df[rolling_volume_column_name_list].astype(float)

        df_list = []

        for unique_event_type in unique_event_list:
            day_list = []
            max_volume_list = []

            temp_df = df.loc[df['event_type'] == unique_event_type]

            temp_df = temp_df.sort_values(by='day', ascending=True)

            unique_day_list = temp_df['day'].unique()

            day_df = pd.DataFrame()
            day_df['day'] = unique_day_list

            day_df = day_df.sort_values(by=['day'], ascending=True)

            unique_day_list = day_df['day'].tolist()

            rolling_volume_event_type = unique_event_type + '_rolling_volume'

            max_rolling_volume_column_name = unique_event_type + '_daily_max_volume'

            for unique_day in unique_day_list:
                temp_day_df = temp_df.loc[temp_df['day'] == unique_day]

                day_column_max_value = temp_day_df[rolling_volume_event_type].max()
                
                temp_df.loc[temp_df['day'] == unique_day, max_rolling_volume_column_name] = day_column_max_value
                day_list.append(unique_day)
                max_volume_list.append(day_column_max_value)
            
            event_type_df = pd.DataFrame()
            event_type_df['day'] = day_list

            column_total_diff_name = unique_event_type + '_total_volume'
            column_daily_diff_name = unique_event_type + '_daily_volume'

            event_type_df[column_total_diff_name] = max_volume_list

            event_type_df[column_daily_diff_name] = event_type_df[column_total_diff_name].diff().fillna(0)

            df_list.append(event_type_df)

        # # full outer merges all of our dataframes
        combo_df = df_list[0]
        i = 1
        while i < len(df_list):
            combo_df = combo_df.merge(df_list[i], on='day', how='outer')

            i += 1

        # # fills NaN columns with 0
        combo_df = combo_df.fillna(0)

        combo_df = combo_df.sort_values(by='day', ascending=True)

        for col in combo_df.columns:

            if col != 'day' and 'daily' not in col:
                mask = combo_df[col] != 0
                combo_df[col] = combo_df[col].mask(~mask).ffill().fillna(0)

        return combo_df
    
    def run_all(self, df):
        df = self.label_events(df)
        df = self.set_rolling_volume(df)

        df = lph.make_day_from_timestamp(df)

        df = self.set_volume_diffs(df)

        return df