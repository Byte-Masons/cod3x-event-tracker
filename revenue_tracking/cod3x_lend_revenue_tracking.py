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

        df = df.sort_values(by=['timestamp'], ascending=True)

        df['usd_rolling_balance'] = df['usd_token_amount'].cumsum()

        df = df.reset_index()

        df = df[['user_address', 'tx_hash', 'token_address', 'token_volume', 'usd_token_amount', 'timestamp', 'usd_rolling_balance']]

        df.sort_values(by=['timestamp'], ascending=True)

        return df
    
    
    def set_token_and_day_diffs(self, df):

        df['usd_rolling_balance'] = df['usd_rolling_balance'].astype(float)


        print(df.loc[df['tx_hash'] == '0x349e6c3aea1b82db7a0ce7f26ce827fafdbab706b5d01a0de4877f4d73c80a20'])

        daily_max_balance_list = []

        unique_day_list = df['day'].unique()

        day_df = pd.DataFrame()
        day_df['day'] = unique_day_list

        day_df = day_df.sort_values(by=['day'], ascending=True)

        unique_day_list = day_df['day'].tolist()

        day_list = []
        max_balance_list = []

        for unique_day in unique_day_list:
            temp_df = df.loc[df['day'] == unique_day]

            daily_max_balance = temp_df['usd_rolling_balance'].max()
            
            df.loc[df['day'] == unique_day, 'daily_max_balance'] = daily_max_balance
            day_list.append(unique_day)
            max_balance_list.append(daily_max_balance)
        
        df = pd.DataFrame()
        df['day'] = day_list
        df['total_revenue'] = max_balance_list

        #     df.loc[(df['day'] == unique_day) & (df['usd_rolling_balance'] == daily_max_balance), 'daily_max_balance'] = daily_max_balance
        
        # df = df.dropna()

        # print(df)

        # # Calculate the difference between consecutive non-NaN values (optional)
        df['daily_revenue'] = df['total_revenue'].diff().fillna(0)

        return df
    
    def set_n_days_avg_revenue(self, df, new_column_name, lookback_days):

        df = df.sort_values(by=['day'], ascending=True)

        df[new_column_name] = df['daily_revenue'].rolling(window=lookback_days, min_periods=1).mean()

        return df

    def update_daily_total_revenue(self):
        

        # df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)

        df = self.set_token_flows()

        df = self.set_rolling_balance(df)
        
        df = lph.make_day_from_timestamp(df)
        df = self.set_token_and_day_diffs(df)

        # # makes our moving average daily revenues
        df = self.set_n_days_avg_revenue(df, '30_days_ma_revenue', 30)
        df = self.set_n_days_avg_revenue(df, '90_days_ma_revenue', 90)
        df = self.set_n_days_avg_revenue(df, '180_days_ma_revenue', 180)
        
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
