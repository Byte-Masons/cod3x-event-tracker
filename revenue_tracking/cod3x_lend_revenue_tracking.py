import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Cod3x_Lend_Revenue_Tracking(Protocol_Data_Provider.Protocol_Data_Provider):
    
    def __init__(self, protocol_data_provider_address: str, treasury_address: str, rpc_url: str, index: str):
        
        self.protocol_data_provider_address = protocol_data_provider_address
        self.treasury_address = treasury_address
        self.rpc_url = rpc_url
        self.index = index
        self.web3 = lph.get_web_3(self.rpc_url)
        self.cloud_file_name = index + '.zip'
        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.index
    
    # # finds if a transaction adds to or reduces a balance 
    # # (deposit + borrow add to a balance and withdraw + repay reduce a balance)
    def set_token_flows(self):
        # event_df = pd.read_csv(csv_name, usecols=['from_address','to_address','timestamp','token_address', 'token_volume','tx_hash'], dtype={'from_address': str,'to_address': str,'timestamp' : str,'token_address': str, 'token_volume': float,'tx_hash': str})
        
        # # tries to remove the null address to greatly reduce computation needs
        # event_df = event_df.loc[event_df['to_address'] != '0x0000000000000000000000000000000000000000']
        
        table_name = self.table_name
        
        # event_df = sql.get_transaction_data_df(table_name)
        event_df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)

        event_df = event_df.drop_duplicates(subset=['tx_hash', 'to_address', 'from_address', 'token_address', 'token_volume'])

        event_df[['usd_token_amount', 'token_volume']] = event_df[['usd_token_amount', 'token_volume']].astype(float)

        event_df = event_df.loc[event_df['to_address'] == self.treasury_address]

        unique_user_df = sql.set_unique_users(table_name)

        # # reduces dataframe down to our treasury address
        temp_df = unique_user_df.loc[unique_user_df['to_address'] == self.treasury_address]
        if len(temp_df) > 1:
            unique_user_df = temp_df

        unique_user_list = unique_user_df['to_address'].to_list()

        reserve_list = self.get_reserve_address_list()
        receipt_list = self.get_receipt_token_list()


        deposit_token_list = self.get_a_token_list()
        borrow_token_list = self.get_v_token_list()

        combo_df = pd.DataFrame()
        temp_df = pd.DataFrame()

        # # handles deposits and borrows
        temp_df = event_df.loc[event_df['to_address'].isin(unique_user_list)]
        deposit_df = temp_df.loc[temp_df['token_address'].isin(deposit_token_list)]

        deposit_df['user_address'] = deposit_df['to_address']

        combo_df = pd.concat([deposit_df])
        combo_df = combo_df[['user_address', 'tx_hash', 'token_address', 'token_volume', 'usd_token_amount', 'timestamp']]

        combo_df.drop_duplicates(subset=['user_address', 'tx_hash', 'token_address', 'token_volume'])
        # make_user_data_csv(combo_df, token_flow_csv)

        # # tries to remove the null address to greatly reduce computation needs
        # combo_df = combo_df.loc[combo_df['user_address'] != '0x0000000000000000000000000000000000000000']

        return combo_df
    
    # # sets rolling balances for each of a users tokens
    def set_rolling_balance(self, df):
        
        df['timestamp'] = df['timestamp'].astype(float)

        df.sort_values(by=['timestamp'], ascending=True)

        df['usd_rolling_balance'] = df['usd_token_amount'].cumsum()

        df = df.reset_index()

        df = df[['user_address', 'tx_hash', 'token_address', 'token_volume', 'usd_token_amount', 'timestamp', 'usd_rolling_balance']]

        return df
    
    def set_token_and_day_diffs(self, df):

        df['usd_rolling_balance'] = df['usd_rolling_balance'].astype(float)

        daily_token_sum = df.groupby(['day', 'token_address', 'user_address'])['usd_rolling_balance'].sum().reset_index()

        daily_token_sum = daily_token_sum.sort_values(['day', 'token_address', 'user_address'])
        
        df = df.merge(daily_token_sum, on=['day', 'token_address', 'user_address'], suffixes=('', '_daily_token_sum'))

        daily_revenue_sum = df.groupby(['day', 'user_address'])['usd_rolling_balance'].sum().reset_index()

        df = df.merge(daily_revenue_sum, on=['day'], suffixes=('', '_daily_total_balance'))

        df.rename(columns = {'usd_rolling_balance_daily_token_sum':'token_day_rolling_balance', 'usd_rolling_balance_daily_total_balance':'total_rolling_balance'}, inplace = True)

        # # casts our day to a datetime
        df['day'] = pd.to_datetime(df['day'], format='%Y-%m-%d')
        # df['day'] = df['day'].astype(str)
        df[['total_rolling_balance_change', 'token_day_rolling_balance']] = df[['total_rolling_balance', 'token_day_rolling_balance']].astype(float)

        df = df.sort_values(by=['day', 'token_address', 'user_address'])

        df['token_day_rolling_balance'] = df.groupby(['token_address', 'user_address', 'day'])['token_day_rolling_balance'].fillna(method='ffill')
        df['total_rolling_balance'] = df.groupby(['token_address', 'user_address', 'day'])['total_rolling_balance'].fillna(method='ffill')

        # Calculate the difference between consecutive non-NaN values (optional)
        df['token_day_diff'] = df['token_day_rolling_balance'].diff().fillna(0)
        df['day_diff'] = df['total_rolling_balance'].diff().fillna(0)

        return df
    
    # # will make our dataframe only have 1 row per day_diff
    def set_total_day_diff_1_line(self, df):

        daily_total_balance_diff_df = df.groupby(['day', 'user_address', 'total_rolling_balance'])['day_diff'].max().reset_index()
        
        return daily_total_balance_diff_df

    def update_daily_total_revenue(self):
        

        # df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)

        df = self.set_token_flows()

        df = self.set_rolling_balance(df)
        
        df = lph.make_day_from_timestamp(df)
        # df = self.set_token_and_day_diffs(df)
        # df = self.set_total_day_diff_1_line(df)
        
        return df

    def get_revenue_by_day_cloud_name(index):

        table_name = lph.get_lp_config_value('table_name', index)

        blockchain_name = table_name.split('_')[0]

        revenue_cloud_name = blockchain_name + '_total_lend_revenue_by_day.csv'

        return revenue_cloud_name
    
    # # our runner function which reads our event df from the cloud
    # # calculates the revenue per day and balance of our deployment's treasury
    # # writes to a cloud csv file to save the data
    def run_total_revenue_by_day(index):

        df = update_daily_total_revenue(index)
        df = df[:1000]

        revenue_filename = get_revenue_by_day_cloud_name(index)

        cloud_bucket_name = lph.get_lp_config_value('cloud_bucket_name', index)
        cs.df_write_to_cloud_storage_as_zip(df, revenue_filename, cloud_bucket_name)

        return df
