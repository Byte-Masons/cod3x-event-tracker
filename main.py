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
from lending_pool import current_balance_tracker as cbt
from lending_pool import treasury_tracker as tt

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
    #     run_robust_function(cbt.loop_through_current_balances, index)

    loop_all_functions()

def loop_all_functions_2():
    index_list = [1]

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

# loop_all_functions()

# df = sql.get_transaction_data_df('metis_events')
# print(df)

# cloud_df = cs.read_from_cloud_storage('metis_events.csv', 'cooldowns2')

# df_list = [df, cloud_df]

# new_df = pd.concat(df_list)

# new_df = df.drop_duplicates(subset=['tx_hash', 'token_address', 'token_volume', 'from_address', 'to_address'])

# cs.df_write_to_cloud_storage(new_df, 'metis_events.csv', 'cooldowns2')