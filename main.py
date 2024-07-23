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
from revenue_tracking import Cod3x_Lend_Revenue_Tracking as cod3x, Transaction_Labeler as tl
from datetime import datetime, timezone
from protocol import Aurelius,Ironclad, Arbitrum, Optimism, Metis
import logging
from helper_classes import ERC_20 as erc20

logging.basicConfig(level=logging.ERROR)


runtime_pause = 600
# PROTOCOL_LIST = [Aurelius.Aurelius(),Optimism.Optimism(),Ironclad.Ironclad(),Metis.Metis(),Arbitrum.Arbitrum()]
PROTOCOL_LIST = [Optimism.Optimism()]

# # will try to run the function it it fails for whatever reason
def run_robust_function(function, input):

    try:
        function(input)
    except:
        run_robust_function(function, input)

    return

def loop_all_functions():

    i = 1
    for protocol in PROTOCOL_LIST:
        run_robust_function(protocol.run_all_modules)

        # cod3x.run_total_revenue_by_day(index)

        print('Protocol Completed:', i, '/', len(PROTOCOL_LIST))
        i += 1

    # for index in index_list:
    #     run_robust_function(cdp.find_all_trove_updated_transactions, index)

    # for index in index_list:
    #     run_robust_function(cbt.loop_through_current_balances, index)

    time.sleep(runtime_pause)

    loop_all_functions()

def loop_all_functions_2():

    i = 1
    for protocol in PROTOCOL_LIST:
        try:
            protocol.run_all_modules()

        # df = cod3x.run_total_revenue_by_day(index)
        # print(df)

            print('Protocol Completed:', i, '/', len(PROTOCOL_LIST))

        except Exception as e:
            print(f"An error occurred: {e}")
            
            print('Index Failed: ', i)
            pass

        i += 1

    # cdp.find_all_trove_updated_transactions(1)

    # for index in index_list:
    #     cbt.loop_through_current_balances(index)

    time.sleep(runtime_pause)
    
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

loop_all_functions_2()

# lend_events = Optimism.Optimism()
# lend_events.run_all_modules()

# sql.drop_table('arbitrum_lend_events')

# PROTOCOL_DATA_PROVIDER_ADDRESS = '0x96bCFB86F1bFf315c13e00D850e2FAeA93CcD3e7'
# RPC_URL = 'https://arbitrum.llamarpc.com'
# TREASURY_ADDRESS = '0xb17844F6E50f4eE8f8FeC7d9BA200B0E034b8236'
# INDEX = 'arbitrum_lend_events'
# CLOUD_BUCKET_NAME = 'cooldowns2'
# INTERVAL = 2500
# WAIT_TIME = 1.05
# GATEWAY_ADDRESS = '0x3CC0a623f1aFFab5D5514A453965cE8C80B45549'

# aurelius_lend_revenue = cod3x.Cod3x_Lend_Revenue_Tracking(PROTOCOL_DATA_PROVIDER_ADDRESS, TREASURY_ADDRESS, RPC_URL, INDEX)

# df = aurelius_lend_revenue.update_daily_total_revenue()
# print(df)

# df.to_csv('test_test.csv', index=False)

# lend_events = Metis.Metis()
# lend_events.run_all_modules()

# df = sql.get_transaction_data_df('optimism_lend_events')
# df = cs.read_zip_csv_from_cloud_storage('optimism_lend_events.zip', 'cooldowns2')
# df['usd_token_amount'] = df['usd_token_amount'].astype(float)

# print(df['usd_token_amount'].sum())

# df = pd.read_csv('ironclad_lend_events.csv')
# df = sql.get_transaction_data_df(INDEX)
# df = cs.read_zip_csv_from_cloud_storage('optimism_lend_events.zip', 'cooldowns2')
# labeler = tl.Transaction_Labeler(PROTOCOL_DATA_PROVIDER_ADDRESS, RPC_URL, INDEX, GATEWAY_ADDRESS, TREASURY_ADDRESS)
# df = labeler.label_events(df)

# df = df.drop_duplicates(subset=['tx_hash', 'to_address', 'from_address', 'token_address', 'token_volume'])
# print(df)

# if len(df) > 0:
#     cs.df_write_to_cloud_storage_as_zip(df, 'ironclad_lend_events.zip', 'cooldowns2')

# df.to_csv('test_test.csv', index=False)

# df.to_csv('aurelius_lend_revenue.csv', index=False)

# formats old .zips into new ones
# df = sql.get_transaction_data_df('optimism_lend_events')
# print(df)
# df = cs.read_zip_csv_from_cloud_storage('optimism_lend_events.zip', 'cooldowns2')

# df = pd.read_csv('arbitrum_lend_events.csv')
# df = df.astype(str)
# df = df.drop_duplicates(subset=['tx_hash', 'to_address', 'from_address', 'token_address', 'token_volume'])
# print(df)

# lend_events = Arbitrum.Arbitrum()
# lend_events.create_lend_table(df)

# df = sql.get_transaction_data_df('arbitrum_lend_events')
# df['timestamp'] = df['timestamp'].astype(int)
# df = df.sort_values(by='timestamp', ascending=False)
# df = df[:2500]
# print(df)

# labeler = tl.Transaction_Labeler(PROTOCOL_DATA_PROVIDER_ADDRESS, RPC_URL, INDEX, GATEWAY_ADDRESS, TREASURY_ADDRESS)
# df = labeler.label_events(df)
# print(df)

# cs.df_write_to_cloud_storage_as_zip(df, 'arbitrum_lend_events.zip', 'cooldowns2')

# # df.to_csv('optimism_lend_events.csv', index=False)
# df = df[['from_address', 'to_address', 'tx_hash', 'timestamp', 'token_address', 'reserve_address', 'token_volume', 'asset_price', 'usd_token_amount', 'block_number']]
# cs.df_write_to_cloud_storage_as_zip(df, 'optimism_lend_events.zip', 'cooldowns2')