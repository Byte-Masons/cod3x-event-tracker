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

runtime_pause = 60

# # will try to run the function it it fails for whatever reason
def run_robust_function(function, input):

    try:
        function(input)
    except:
        run_robust_function(function, input)

    return

def loop_all_functions():
    index_list = [2]

    for index in index_list:
        run_robust_function(lp_tracker.run_all, index)

        cod3x_lend_revenue_tracking.run_total_revenue_by_day(index)

        print('Index Completed:', index, '/', len(index_list))

    # for index in index_list:
    #     run_robust_function(cdp.find_all_trove_updated_transactions, index)

    # for index in index_list:
    #     run_robust_function(cbt.loop_through_current_balances, index)

    time.sleep(runtime_pause)

    loop_all_functions()

def loop_all_functions_2():
    index_list = [2]

    for index in index_list:
        lp_tracker.run_all(index)

        df = cod3x_lend_revenue_tracking.run_total_revenue_by_day(index)
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

# loop_all_functions_2()

# df = cs.read_from_cloud_storage('metis_events.csv', 'cooldowns2')

# lph.insert_bulk_data_into_table(df, 'metis_events')

# index = 2
# df = cod3x_lend_revenue_tracking.update_daily_total_revenue(index)
# df = sql.get_transaction_data_df('metis_events')
# df = lph.set_token_flows(2)
# df = lph.set_rolling_balance(df)
# df = df.loc[df['user_address'] == '0xCba1A275e2D858EcffaF7a87F606f74B719a8A93']
# df = df.loc[df['user_address'] == '0xd2abC5d7841d49C40Fd35A1Ec832ee1daCC8D339']
# df = lph.make_day_from_timestamp(df)
# df = lph.set_token_and_day_diffs(df)
# df = lph.set_total_day_diff_1_line(df)
# df = lph.set_token_wallet_address_diff_1_line(df)
# df = df.loc[df['day'] == '2024-01-16']
# print(df[['day', 'token_day_diff', 'day_diff']])
# print(df)

# df = cod3x_lend_revenue_tracking.update_cloud_total_revenue_by_day(index)
# print(df)