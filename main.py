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
from cdp import CDP, Stability_Pool
from revenue_tracking import cod3x_lend_revenue_tracking as cod3x, Transaction_Labeler as tl, cod3x_lend_total_revenue_tracking as cdx_total, total_revenue
from datetime import datetime, timezone
from protocol import Aurelius,Ironclad, Arbitrum, Optimism, Metis, Glyph, Base, Fantom, Lore, Harbor
from misc import VeMode_Voting
import logging
from helper_classes import ERC_20 as erc20, oToken, Rewarder, Reliquary
from cdx_usd import cdx_usd

logging.basicConfig(level=logging.ERROR)


runtime_pause = 3600
PROTOCOL_LIST = [Aurelius.Aurelius(),Optimism.Optimism(),Ironclad.Ironclad(),Metis.Metis(),Arbitrum.Arbitrum(),Glyph.Glyph(),Base.Base(), Fantom.Fantom(), Lore.Lore(), Harbor.Harbor()]
# PROTOCOL_LIST = [Ironclad.Ironclad()]

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

# lend_events = Ironclad.Ironclad()
# lend_events.run_all_modules()

# df = cdx_usd.run_all()
# print(df)

# total_revenue.run_all()

# ve_mode_voting = VeMode_Voting.VeMode_Voting('0x2aA8A5C1Af4EA11A1f1F10f3b73cfB30419F77Fb', 14405098, 'https://mainnet.mode.network', 0.43, 2500, 'ironclad_bpt')
# ve_mode_voting.run_all_event_tracking()

# df = sql.get_vote_df('ironclad_bpt_vote_events')
# df = VeMode_Voting.get_user_ironclad_votes()

# print(df)
# df.to_csv('vote_test.csv', index=False)

# sql.drop_table('ironclad_vote_events')

# df = cs.read_zip_csv_from_cloud_storage('our_lend_revenue_data_card.zip', 'cooldowns2')
# df.to_csv('ironclad_lend_events.csv', index=False)

# df['block_number'] = df['block_number'].astype(int)
# print(df['block_number'].min())

# df = df.loc[df['block_number'] == df['block_number'].min()]

# df['block_number'] = [3929754]
# print(df)

# cs.df_write_to_cloud_storage_as_zip(df, 'ironclad_retro_lend_events.zip', 'cooldowns2')

# df = sql.get_stability_pool_df('lore_stability_pool_events')
# print(df)

# sql.drop_table('ironclad_lend_events')

# ve_mode_voting = VeMode_Voting.VeMode_Voting('0x71439Ae82068E19ea90e4F506c74936aE170Cf58', 14405098, 'https://mainnet.mode.network', 0.6, 2500, 'ironclad')
# ve_mode_voting.run_all_event_tracking()

# [Aurelius.Aurelius(),Optimism.Optimism(),Ironclad.Ironclad(),Metis.Metis(),Arbitrum.Arbitrum(),Glyph.Glyph(),Base.Base(), Fantom.Fantom(), Lore.Lore(), Harbor.Harbor()]


# df_list = []

# protocol_name_list = ['ironclad', 'ironclad_2', 'aurelius', 'optimism', 'metis', 'arbitrum', 'glyph', 'base', 'fantom_lz', 'lore']

# for protocol in protocol_name_list:
#     protocol_file_name = protocol + '_lend_events.zip'
#     df = cs.read_zip_csv_from_cloud_storage(protocol_file_name, 'cooldowns2')
#     df = df.loc[df['event_type'].isin(['borrow', 'deposit'])]
#     df = df.drop_duplicates(subset=['tx_hash', 'from_address', 'to_address', 'token_volume', 'event_type'])
    
#     df['usd_token_amount'] = df['usd_token_amount'].astype(float)

#     temp_borrow_df = df.loc[df['event_type'] == 'borrow']
#     temp_deposit_df = df.loc[df['event_type'] == 'deposit']

#     total_borrows = temp_borrow_df['usd_token_amount'].sum()
#     total_deposits = temp_deposit_df['usd_token_amount'].sum()

#     temp_df = pd.DataFrame()
#     temp_df['protocol'] = [protocol]
#     temp_df['total_borrows'] = [total_borrows]
#     temp_df['total_deposits'] = [total_deposits]
#     df_list.append(temp_df)
#     time.sleep(2)

# df = pd.concat(df_list)
# print(df)
# df.to_csv('test_test.csv', index=False)

# df = cs.read_zip_csv_from_cloud_storage('ironclad_2_lend_events.zip', 'cooldowns2')
# df['block_number'] = df['block_number'].astype(int)
# min_block = df['block_number'].min()
# print(min_block)
# df = rewarder.run_all()
# print(df)
# print(df['link'].tolist()[0])

# mode_o_token = oToken.oToken(EXERCISE_ADDRESS, FROM_BLOCK, RPC_URL, WAIT_TIME, INTERVAL, INDEX)
# mode_o_token.run_all_o_token_tracking()

# cdp_events = CDP.CDP(BORROWER_OPERATIONS_ADDRESS, FROM_BLOCK, RPC_URL, WAIT_TIME, INTERVAL, INDEX)
# cdp_events.run_all_cdp_tracking()
# sql.drop_table('ironclad_vote_events')

# cdx_total.run_all()
# bucket_name = 'cooldowns2'
# df = cs.read_zip_csv_from_cloud_storage('ironclad_stability_pool_events.zip', bucket_name)
# df['stability_pool'] = 'old'
# df.to_csv('wrseth_1.csv', index=False)
# df_2 = cs.read_zip_csv_from_cloud_storage('ironclad_2_stability_pool_events.zip', bucket_name)
# df_2['stability_pool'] = 'new'
# df_2 = pd.read_csv('ironclad_lend_events.csv')
# df['o_token_amount'] = df['o_token_amount'].astype(float)
# print(df['o_token_amount'].sum() / 1e18)
# df_2 = sql.get_transaction_data_df('ironclad_lend_events')
# print(df_2)
# df = sql.get_o_token_data_df('ironclad_o_token_events')
# df = sql.get_cdp_token_data_df('aurelius_cdp_events')
# df = pd.concat([df, df_2])
# df = df.drop_duplicates(subset=['tx_hash', 'depositer_address', 'net_deposit_amount'])
# df['timestamp'] = df['timestamp'].astype(float)
# df = df.sort_values(by='timestamp', ascending=False)
# df = df[-5:]
# print(df.loc[df['from_address'] == '0x62365E0af7b6A188D063825AFa3399Ba0b5AA7Cd'])
# df = df.loc[(df['to_address'] == '0x62365E0af7b6A188D063825AFa3399Ba0b5AA7Cd') | (df['from_address'] == '0x62365E0af7b6A188D063825AFa3399Ba0b5AA7Cd')]
# print(df)
# df.to_csv('test_test.csv', index=False)
# print(len(df['tx_hash'].unique()))
# df['block_number'] = df['block_number'].astype(int)
# df = df.sort_values(by='block_number', ascending=False)
# df = df[:555]

# if len(df) > 1:
#     cs.df_write_to_cloud_storage_as_zip(df, 'ironclad_2_o_token_events.zip', 'cooldowns2')

# df = pd.read_csv('test_test.csv')
# print(df)
# temp_df = df.loc[df['stability_pool'] == 'new']
# temp_df = temp_df.sort_values('timestamp', ascending=False)
# temp_df = temp_df.groupby('depositer_address').first().reset_index()
# temp_df = temp_df.sort_values('timestamp', ascending=False).groupby('depositer_address').first().reset_index()

# temp_df = temp_df.loc[temp_df['block_number'] > 0]
# print(temp_df)

# temp_df.to_csv('test_trach.csv', index=False)