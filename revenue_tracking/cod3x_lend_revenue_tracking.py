import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class cod3x_lend_revenue_tracking(Protocol_Data_Provider.Protocol_Data_Provider, ERC_20.ERC_20):
    
    def __init__(self, protocol_data_provider_address: str, treasury_address: str, rpc_url: str, index: str):
        
        self.protocol_data_provider_address = protocol_data_provider_address
        self.treasury_address = treasury_address
        self.rpc_url = rpc_url
        self.index = self.get_event_index(index)
        self.web3 = lph.get_web_3(self.rpc_url)
        self.cloud_file_name = self.get_cloud_filename()

        self.revenue_cloud_file_name = self.get_lend_revenue_cloud_name()

        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.index
    
    # # adds onto our index for o_token_events
    def get_event_index(self, index):
        index = index + '_lend_events'
        
        return index
    
    # # makes our revenue cloud filename
    def get_cloud_filename(self):
        cloud_filename = self.index + '.zip'

        return cloud_filename
    
    # # makes our revenue cloud filename
    def get_lend_revenue_cloud_name(self):
        revenue_cloud_filename = self.index.split('_')[0] + '_lend_revenue.zip'

        return revenue_cloud_filename
    
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
    
    # # sets rolling balances for each of a users tokens
    def set_rolling_balance_2(self, df):
        
        df[['timestamp', 'usd_token_amount']] = df[['timestamp', 'usd_token_amount']].astype(float)

        df = df.sort_values(by=['timestamp'], ascending=True)

        # Group the DataFrame by 'name' and calculate cumulative sum

        token_address_list = df['token_address'].unique()

        for token_address in token_address_list:
            df.loc[df['token_address'] == token_address, 'usd_rolling_balance'] = df.loc[df['token_address'] == token_address]['usd_token_amount'].cumsum()
        
        # name_groups = df.groupby(['token_address'])['usd_token_amount'].transform(pd.Series.cumsum)

        # Print the DataFrame with the new 'amount_cumulative' column
        # df = df.assign(usd_rolling_balance=name_groups)

        # df = df.reset_index()

        df = df[['user_address', 'tx_hash', 'token_address', 'token_volume', 'usd_token_amount', 'timestamp', 'usd_rolling_balance']]
        
        df = df.sort_values(by=['timestamp'], ascending=True)
        
        return df
    
    def set_token_and_day_diffs(self, df):

        df['usd_rolling_balance'] = df['usd_rolling_balance'].astype(float)


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
    
    # # will ffill total_revenue_per_token for every day in our dataframe irregardelous if there was a token transfer for that token on that day
    def fill_missing_total_revenue_per_token_values(self, df):
        # Assuming your dataframe is called 'df'
        # Sort the dataframe by day and token_address
        df = df.sort_values(['day', 'token_address'])

        # Set day and token_address as index
        df_indexed = df.set_index(['day', 'token_address'])

        # Unstack the dataframe
        df_unstacked = df_indexed.unstack()

        # Forward fill the missing values
        df_filled = df_unstacked

        df_filled['total_revenue_per_token'] = df_unstacked['total_revenue_per_token'].ffill()

        # Stack the dataframe back
        df_stacked = df_filled.stack()

        # Reset the index
        df_result = df_stacked.reset_index()

        # Rename the columns if needed
        # df_result.columns = ['day', 'token_address', 'total_revenue']
        
        return df_result
    
    # # gets the change in daily and total revenue per token per day
    def set_token_and_day_diffs_2(self, df):

        df = df.sort_values(by='day', ascending=True)

        df['usd_rolling_balance'] = df['usd_rolling_balance'].astype(float)

        daily_token_sum = df.groupby(['day', 'token_address'])['usd_rolling_balance'].max().reset_index()

        daily_token_sum = daily_token_sum.sort_values(['day', 'token_address'], ascending=True)

        daily_token_sum.rename(columns = {'usd_rolling_balance':'total_revenue_per_token'}, inplace = True)

        df = daily_token_sum

        unique_token_address_list = df['token_address'].unique()

        for token_address in unique_token_address_list:
            df.loc[df['token_address'] == token_address, 'daily_revenue_per_token'] = df.loc[df['token_address'] == token_address]['total_revenue_per_token'].diff().fillna(0)

        df = self.fill_missing_total_revenue_per_token_values(df)
        df = df.fillna(0)
        total_revenue_df = df.groupby(['day'])['total_revenue_per_token'].sum().reset_index()
        
        daily_revenue_df = df.groupby(['day'])['daily_revenue_per_token'].sum().reset_index()
        
        unique_day_list = df['day'].unique()
        total_revenue_list = total_revenue_df['total_revenue_per_token'].tolist()
        daily_revenue_list = daily_revenue_df['daily_revenue_per_token'].tolist()

        i = 0

        while i < len(unique_day_list):
            day = unique_day_list[i]
            total_revenue = total_revenue_list[i]
            daily_revenue = daily_revenue_list[i]

            df.loc[df['day'] == day, 'total_revenue'] = total_revenue
            df.loc[df['day'] == day, 'daily_revenue'] = daily_revenue

            i += 1

        return df

    # # sets a moving average for our dataframe
    def set_n_days_avg_revenue(self, df, new_column_name, lookback_days):

        df = df.sort_values(by=['day'], ascending=True)

        day_revenue_df = df.groupby(['day'])['daily_revenue'].max().reset_index()

        # Calculate the rolling average
        day_revenue_df[new_column_name] = day_revenue_df['daily_revenue'].rolling(window=lookback_days, min_periods=1).mean()

        # Merge the rolling average back to the original dataframe
        df = df.merge(day_revenue_df[['day', new_column_name]], on='day', how='left')

        return df

    # # adds token names to our dataframe
    def add_token_names(self, df):

        unique_token_list = df['token_address'].unique()

        erc_20_list = [ERC_20.ERC_20(token_address, self.rpc_url) for token_address in unique_token_list]

        for erc_20 in erc_20_list:
            token_address = erc_20.token_address
            token_name = erc_20.name

            df.loc[df['token_address'] == token_address, 'token_name'] = token_name

        return df
    
    def run_all_lend_revenue(self):

        df = self.set_token_flows()

        df = self.set_rolling_balance_2(df)
        
        df = lph.make_day_from_timestamp(df)

        df = self.set_token_and_day_diffs_2(df)

        # # makes our moving average daily revenues
        df = self.set_n_days_avg_revenue(df, '7_days_ma_revenue', 7)
        df = self.set_n_days_avg_revenue(df, '30_days_ma_revenue', 30)
        df = self.set_n_days_avg_revenue(df, '90_days_ma_revenue', 90)
        df = self.set_n_days_avg_revenue(df, '180_days_ma_revenue', 180)
        
        df = self.add_token_names(df)

        cs.df_write_to_cloud_storage_as_zip(df, self.revenue_cloud_file_name, self.cloud_bucket_name)

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
