import pandas as pd
from lending_pool import lp_tracker
from lending_pool import balance_and_points as bp
from sql_interfacer import sql
from cloud_storage import cloud_storage as cs
# from api import api
from flask import Flask, request, jsonify
import json
from web3 import Web3
import time
import queue
from concurrent.futures import ThreadPoolExecutor
import threading
import sqlite3
from lending_pool import approval_tracker
from lending_pool import lending_pool_helper as lph
# from lending_pool import current_balance_tracker as cbt
from lending_pool import treasury_tracker as tt
from cdp import cdp

# # will try to run the function it it fails for whatever reason
def run_robust_function(function, input):

    try:
        function(input)
    except:
        run_robust_function(function, input)

    return

def loop_all_functions():
    index_list = [1]

    run_robust_function(lp_tracker.run_all, index_list)

    # for index in index_list:
    #     run_robust_function(cdp.find_all_trove_updated_transactions, index)

    # for index in index_list:
    #     run_robust_function(cbt.loop_through_current_balances, index)

    loop_all_functions()

def loop_all_functions_2():
    index_list = [1]

    # cdp.find_all_trove_updated_transactions(1)

    lp_tracker.run_all(index_list)


    # for index in index_list:
    #     cbt.loop_through_current_balances(index)


    loop_all_functions_2()

def run_all_treasury():
    index_list = [0]

    for index in index_list:
        run_robust_function(tt.find_all_revenue_transactions, index)

    print('Run it back Turbo')
    time.sleep(250)

    run_all_treasury()

def run_all_treasury_2():
    index_list = [0]

    for index in index_list:
        tt.find_all_revenue_transactions(index)

    print('Run it back Turbo')
    time.sleep(250)

    run_all_treasury()

# loop_all_functions_2()

df = lph.set_token_flows(0)
df = lph.set_rolling_balance(df)
df = df.loc[df['token_address'] == '0x9c29a8eC901DBec4fFf165cD57D4f9E03D4838f7']
print(df)


# 0x515F4055395db22C06DA6FbDD7Cac92A08a01EEa
# loop_all_functions_2()

# cdp.find_all_trove_updated_transactions(1)

# column_list = ['borrower_address', 'tx_hash', 'collateral_address', 'mint_fee', 'block_number', 'timestamp']
# df = sql.get_transaction_data_df('aurelius_mint_fees', column_list)

# df.loc[df['mint_fee'] == '450000000000000000', 'mint_fee'] = 0.45
# df = df.drop_duplicates(subset=['borrower_address', 'tx_hash', 'collateral_address', 'mint_fee'])
# print(df)

# cooldowns2/metis_events.csv

# cloud_df = cs.read_from_cloud_storage('metis_events.csv', 'cooldowns2')
# cloud_df = cloud_df.loc[cloud_df['token_address'] != '0xe7334Ad0e325139329E747cF2Fc24538dD564987']

# temp_df = cloud_df.drop_duplicates(subset=['token_address'])
# token_address_list = temp_df['token_address'].tolist()

# print(token_address_list)

# index = 2
# rpc_url = lph.get_lp_config_value('rpc_url', index)
# web3 = lph.get_web_3(rpc_url)

# cloud_df = lph.set_df_prices(cloud_df, web3, index)

# print(cloud_df['usd_token_amount'].sum())
# cloud_df.to_csv('2024_06_18_metis_events.csv', index=False)

# cloud_df = cloud_df.loc[cloud_df['token_address'] == '0xFdD2eBc184b4ff6dF14562715452E970c82Fe49A']
# print('Minimum Price:', cloud_df['asset_price'].min())
# print('Maximum Price', cloud_df['asset_price'].max())

# df_list = [df, cloud_df]

# new_df = pd.concat(df_list)

# new_df = df.drop_duplicates(subset=['tx_hash', 'token_address', 'token_volume', 'from_address', 'to_address'])

# cs.df_write_to_cloud_storage(df, 'aurelius_mint_fees.csv', 'cooldowns2')