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
from revenue_tracking import cod3x_lend_revenue_tracking as cod3x
from notion import notion_database_updater as ndu
from datetime import datetime, timezone
from lending_pool import Lending_Pool

runtime_pause = 60

# # will try to run the function it it fails for whatever reason
def run_robust_function(function, input):

    try:
        function(input)
    except:
        run_robust_function(function, input)

    return

def loop_all_functions():
    index_list = [0,1,2]

    for index in index_list:
        run_robust_function(lp_tracker.run_all, index)

        cod3x.run_total_revenue_by_day(index)

        print('Index Completed:', index, '/', len(index_list))

    # for index in index_list:
    #     run_robust_function(cdp.find_all_trove_updated_transactions, index)

    # for index in index_list:
    #     run_robust_function(cbt.loop_through_current_balances, index)

    time.sleep(runtime_pause)

    loop_all_functions()

def loop_all_functions_2():
    index_list = [0,1,2]

    for index in index_list:
        lp_tracker.run_all(index)

        df = cod3x.run_total_revenue_by_day(index)
        print(df)

        print('Index Completed:', index, '/', len(index_list))
    
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

# loop_all_functions()

treasury_address = '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF'
protocol_data_provider_address = '0x29563f73De731Ae555093deb795ba4D1E584e42E'
rpc_url = 'wss://mode.drpc.org'
index = 'aurelius_lend_events_2'
interval = 250
wait_time = 0.2

ironclad_lending_pool = Lending_Pool.Lending_Pool(protocol_data_provider_address, rpc_url, wait_time, interval, index)
# ironclad_lending_pool.get_non_stable_receipt_token_list()

# print(ironclad_lending_pool.receipt_list)

ironclad_lending_pool.run_all()

# receipt_token_address_list = ironclad_lending_pool.get_receipt_token_list()

# a_token_list = ironclad_lending_pool.get_a_token_list()
# v_token_list = ironclad_lending_pool.get_v_token_list()
# non_stable_receipt_token_list = ironclad_lending_pool.get_non_stable_receipt_token_list()

# print(non_stable_receipt_token_list)

# filename = 'metis_lend_events.csv'
# table_name = 'metis_events'

# cloud_df = cs.read_from_cloud_storage(filename, 'cooldowns2')
# lp_tracker.create_tx_table(table_name, cloud_df)

# df = sql.get_transaction_data_df(table_name)
# df = df.dropna()
# print(df)

# cs.df_write_to_cloud_storage_as_zip(df, 'metis_lend_events.zip', 'cooldowns2')