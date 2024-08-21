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
from lending_pool import treasury_tracker as tt, User_Balance
from cdp import CDP
from revenue_tracking import cod3x_lend_revenue_tracking as cod3x, Transaction_Labeler as tl, cod3x_lend_total_revenue_tracking as cdx_total, total_revenue
from datetime import datetime, timezone
from protocol import Aurelius,Ironclad, Arbitrum, Optimism, Metis, Glyph, Base, Fantom, Lore
import logging
from helper_classes import ERC_20 as erc20, oToken, Rewarder

logging.basicConfig(level=logging.ERROR)


runtime_pause = 7200
# PROTOCOL_LIST = [Aurelius.Aurelius(),Optimism.Optimism(),Ironclad.Ironclad(),Metis.Metis(),Arbitrum.Arbitrum(),Glyph.Glyph(),Base.Base(), Fantom.Fantom(), Lore.Lore()]
PROTOCOL_LIST = [Aurelius.Aurelius(),Lore.Lore(),Optimism.Optimism(),Lore.Lore(),Ironclad.Ironclad(),Lore.Lore(),Metis.Metis(),Lore.Lore(),Arbitrum.Arbitrum(),Lore.Lore(),Glyph.Glyph(),Lore.Lore(),Base.Base(),Lore.Lore(),Fantom.Fantom(),Lore.Lore()]

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

loop_all_functions_2()

# lend_events = Lore.Lore()
# lend_events.run_all_modules()

# df = cdx_total.run_all()

# df = total_revenue.run_all()
# print(df)

# rewarder = Rewarder.Rewarder('https://mainnet.mode.network', '0xC043BA54F34C9fb3a0B45d22e2Ef1f171272Bc9D', 'lending_pool', 0.6, 'ironclad')
# df = rewarder.run_all()
# print(df)
# print(df['link'][0])

# mode_o_token = oToken.oToken(EXERCISE_ADDRESS, FROM_BLOCK, RPC_URL, WAIT_TIME, INTERVAL, INDEX)
# mode_o_token.run_all_o_token_tracking()

# cdp_events = CDP.CDP(BORROWER_OPERATIONS_ADDRESS, FROM_BLOCK, RPC_URL, WAIT_TIME, INTERVAL, INDEX)
# cdp_events.run_all_cdp_tracking()
# sql.drop_table('ironclad_2_o_token_events')

# cdx_total.run_all()
# bucket_name = 'cooldowns2'
# df = cs.read_zip_csv_from_cloud_storage('weeth_balances.zip', bucket_name)
# df = pd.read_csv('ironclad_o_token_events.csv')
# df['o_token_amount'] = df['o_token_amount'].astype(float)
# print(df['o_token_amount'].sum() / 1e18)
# df = sql.get_transaction_data_df('ironclad_lend_events')
# df = sql.get_o_token_data_df('ironclad_o_token_events')
# df = sql.get_cdp_token_data_df('aurelius_cdp_events')
# df = df.drop_duplicates(subset=['tx_hash', 'from_address', 'to_address', 'token_address', 'token_volume'])
# df['timestamp'] = df['timestamp'].astype(float)
# df = df.sort_values(by='timestamp', ascending=False)
# df = df[-5:]
# print(df)
# df = df.loc[df['collateral_amount'] != '0x0000000000000000000000000000000000000000']
# df.to_csv('test_test.csv', index=False)

# df['block_number'] = df['block_number'].astype(int)
# df = df.sort_values(by='block_number', ascending=False)
# df = df[:555]

# if len(df) > 1:
#     cs.df_write_to_cloud_storage_as_zip(df, 'ironclad_2_o_token_events.zip', 'cooldowns2')