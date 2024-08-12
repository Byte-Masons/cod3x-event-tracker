import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class cdp_mint_fee_revenue_tracking():
    
    def __init__(self, index: str):
        
        self.index = index

        self.event_cloud_filename = self.get_cdp_event_cloud_filename()
        self.lend_revenue_cloud_filename = self.get_lend_revenue_cloud_filename()
        self.revenue_cloud_filename = self.get_cdp_revenue_cloud_filename()

        self.cloud_bucket_name = 'cooldowns2'
    
    # # makes our o_token_event cloud filename
    def get_cdp_event_cloud_filename(self):
        cloud_filename = self.index
        cloud_filename = cloud_filename + '_cdp_events.zip'

        return cloud_filename
    
    # # makes our revenue cloud filename
    def get_lend_revenue_cloud_filename(self):
        cloud_filename = self.index + '_lend_revenue.zip'

        return cloud_filename
    
    # # makes our revenue cloud filename
    def get_cdp_revenue_cloud_filename(self):
        cloud_filename = self.index
        cloud_filename = cloud_filename + '_cdp_revenue.zip'

        return cloud_filename
    
    # # returns a rolling_balance of our oToken redemption rewards
    def set_rolling_balance(self):

        df = cs.read_zip_csv_from_cloud_storage(self.event_cloud_filename, self.cloud_bucket_name)
        df[['timestamp', 'mint_fee']] = df[['timestamp', 'mint_fee']].astype(float)

        df = df.sort_values(by=['timestamp'], ascending=True)
        df['usd_rolling_balance'] = df['mint_fee'].cumsum()
        
        return df
    
    # # finds out how much revenue we generated per day
    def set_day_diffs(self, df):

        df = df.sort_values(by='day', ascending=True)

        df['usd_rolling_balance'] = df['usd_rolling_balance'].astype(float)

        daily_max_balance = df.groupby(['day'])['usd_rolling_balance'].max().reset_index()

        daily_max_balance.rename(columns = {'usd_rolling_balance':'total_revenue'}, inplace = True)

        df = daily_max_balance

        df['daily_revenue'] = df['total_revenue'].diff().fillna(0)

        return df
    
    # # will add our o_token_revenue to our lend_revenue dataframe and return our updated lend_revenue dataframe
    def combine_lend_revenue_with_cdp_revenue(self, df):
        
        lend_revenue_df = cs.read_zip_csv_from_cloud_storage(self.lend_revenue_cloud_filename, self.cloud_bucket_name)

        lend_revenue_df[['total_revenue', 'daily_revenue']] = lend_revenue_df[['total_revenue', 'daily_revenue']].astype(float)

        unique_o_token_days = df['day'].unique()

        for unique_day in unique_o_token_days:
            o_token_total_revenue = df.loc[df['day'] == unique_day]['total_revenue'].max()
            o_token_daily_revenue = df.loc[df['day'] == unique_day]['daily_revenue'].max()

            lend_revenue_df.loc[lend_revenue_df['day'] == unique_day, 'total_revenue'] = lend_revenue_df.loc[lend_revenue_df['day'] == unique_day]['total_revenue'] + o_token_total_revenue
            lend_revenue_df.loc[lend_revenue_df['day'] == unique_day, 'daily_revenue'] = lend_revenue_df.loc[lend_revenue_df['day'] == unique_day]['daily_revenue'] + o_token_daily_revenue

        return lend_revenue_df
    
    def update_moving_averages(self, df):
        
        # # makes our moving average daily revenues
        df = lph.set_n_days_avg_revenue(df, '7_days_ma_revenue', 7)
        df = lph.set_n_days_avg_revenue(df, '30_days_ma_revenue', 30)
        df = lph.set_n_days_avg_revenue(df, '90_days_ma_revenue', 90)
        df = lph.set_n_days_avg_revenue(df, '180_days_ma_revenue', 180)

        return df
    
    # # runs all of our necessary code to add our o_token_revenue to our lend_revenue cloud file
    def run_all_cdp_revenue(self):

        df = self.set_rolling_balance()

        # # removes our placeholder row
        df = df.loc[df['block_number'] != '1776']
        
        df = lph.make_day_from_timestamp(df)

        df = self.set_day_diffs(df)

        df = self.update_moving_averages(df)

        # # # writes to our cloud files
        cs.df_write_to_cloud_storage_as_zip(df, self.revenue_cloud_filename, self.cloud_bucket_name)

        return df