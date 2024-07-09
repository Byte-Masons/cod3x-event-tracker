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

treasury_address = '0xCE4975E63b6e737c41C9c0e5aCd248Ef0145B51A'
protocol_data_provider_address = '0xedB4f24e4b74a6B1e20e2EAf70806EAC19E1FA54'
rpc_url = 'https://rpc.mantle.xyz'
index = 'aurelius_lend_events_2'
interval = 500
wait_time = 0.2

ironclad_lending_pool = Lending_Pool.Lending_Pool(protocol_data_provider_address, rpc_url, wait_time, interval, index)

ironclad_lending_pool.run_all()

# reserve_list = ['0xd988097fb8612cc24eeC14542bC03424c656005f', '0xDfc7C877a950e49D2610114102175A06C2e3167a']
# price_list = ironclad_lending_pool.get_oracle_price(reserve_list)

# print(price_list)