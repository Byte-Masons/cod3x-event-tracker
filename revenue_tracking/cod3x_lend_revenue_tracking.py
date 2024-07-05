import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from sql_interfacer import sql

def update_daily_total_revenue(index):
    
    cloud_filename = lph.get_lp_config_value('cloud_filename', index)
    cloud_bucket_name = lph.get_lp_config_value('cloud_bucket_name', index)

    df = cs.read_zip_csv_from_cloud_storage(cloud_filename, cloud_bucket_name)

    treasury_address = lph.get_lp_config_value('treasury_address', index)

    # df = sql.get_transaction_data_df('metis_events')

    df = lph.set_token_flows(2)
    df = lph.set_rolling_balance(df)
    # df = df.loc[df['user_address'] == '0xCba1A275e2D858EcffaF7a87F606f74B719a8A93']
    df = df.loc[df['user_address'] == treasury_address]
    df = lph.make_day_from_timestamp(df)
    df = lph.set_token_and_day_diffs(df)
    df = lph.set_total_day_diff_1_line(df)
    # df = lph.set_token_wallet_address_diff_1_line(df)
    # df = df.loc[df['day'] == '2024-01-16']
    # print(df[['day', 'token_day_diff', 'day_diff']])
    
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
