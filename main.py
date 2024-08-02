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
from cdp import CDP
from revenue_tracking import cod3x_lend_revenue_tracking as cod3x, Transaction_Labeler as tl, cod3x_lend_total_revenue_tracking as cdx_total
from datetime import datetime, timezone
from protocol import Aurelius,Ironclad, Arbitrum, Optimism, Metis, Glyph, Base, Fantom
import logging
from helper_classes import ERC_20 as erc20, oToken

logging.basicConfig(level=logging.ERROR)


runtime_pause = 600
# PROTOCOL_LIST = [Aurelius.Aurelius(),Optimism.Optimism(),Ironclad.Ironclad(),Metis.Metis(),Arbitrum.Arbitrum(),Glyph.Glyph(),Base.Base(), Fantom.Fantom()]
# PROTOCOL_LIST = [Aurelius.Aurelius(),Optimism.Optimism(),Ironclad.Ironclad(),Metis.Metis(),Arbitrum.Arbitrum(),Glyph.Glyph(),Base.Base(), Fantom.Fantom()]

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

# loop_all_functions_2()

lend_events = Ironclad.Ironclad()
lend_events.run_all_modules()

# df = cdx_total.run_all()

EXERCISE_ADDRESS = '0xcb727532e24dFe22E74D3892b998f5e915676Da8'
BORROWER_OPERATIONS_ADDRESS = '0x4Cd23F2C694F991029B85af5575D0B5E70e4A3F1'
# FROM_BLOCK = 51922639
FROM_BLOCK = 52092639
RPC_URL = 'https://rpc.mantle.xyz'
INTERVAL = 5000
WAIT_TIME = 0.6
INDEX = 'aurelius'

# mode_o_token = oToken.oToken(EXERCISE_ADDRESS, FROM_BLOCK, RPC_URL, WAIT_TIME, INTERVAL, INDEX)
# mode_o_token.run_all_o_token_tracking()

# cdp_events = CDP.CDP(BORROWER_OPERATIONS_ADDRESS, FROM_BLOCK, RPC_URL, WAIT_TIME, INTERVAL, INDEX)
# cdp_events.run_all_cdp_tracking()
# sql.drop_table('aurelius_cdp_events')

# cdx_total.run_all()
# bucket_name = 'cooldowns2'
# df = cs.read_zip_csv_from_cloud_storage('aurelius_cdp_events.zip', bucket_name)
# df = pd.read_csv('test_test.csv')
# df = sql.get_transaction_data_df('ironclad_lend_events')
# df = sql.get_o_token_data_df('ironclad_o_token_events')
# df = sql.get_cdp_token_data_df('aurelius_cdp_events')
# print(df)
# df = df.loc[df['collateral_amount'] != '0x0000000000000000000000000000000000000000']
# df.to_csv('test_test.csv', index=False)

# df['block_number'] = df['block_number'].astype(int)
# df = df.sort_values(by='block_number', ascending=False)
# df = df[:555]

# if len(df) > 1:
#     cs.df_write_to_cloud_storage_as_zip(df, 'aurelius_cdp_events.zip', 'cooldowns2')