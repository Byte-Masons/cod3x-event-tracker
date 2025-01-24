import sys
import os
import time
import pandas as pd
import sqlite3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import lending_pool_helper as lph
from helper_classes import ERC_20, Protocol_Data_Provider, Sanitize
from cloud_storage import cloud_storage as cs
from sql_interfacer import sql
from revenue_tracking import Transaction_Labeler as tl

RPC_URL = 'https://mainnet.mode.network'

ORACLE_ABI = [{"inputs":[{"internalType":"address","name":"_proxy","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getTokenType","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"latestAnswer","outputs":[{"internalType":"int256","name":"value","type":"int256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"latestTimestamp","outputs":[{"internalType":"uint256","name":"timestamp","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxy","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]

UNIBTC_ORACLE_ADDRESS = '0x4EEB40C0379B8654db64966b2C7C6039486d4F9f'

WBTC_ORACLE_ADDRESS = '0x49eC6249e12ac555eA6D8b8bC47fe3e286949c02'

WAIT_TIME = 0.5

CLOUD_FILENAME = 'Oracle_Comparer.zip'

BUCKET_NAME = 'cooldowns2'

def get_data_provider():
    protocol_data_provider_address = '0x29563f73De731Ae555093deb795ba4D1E584e42E'

    data_provider = Protocol_Data_Provider.Protocol_Data_Provider(protocol_data_provider_address, RPC_URL)

    return data_provider


def run_all():
    web3 = lph.get_web_3(RPC_URL)

    oracle_address_list = [UNIBTC_ORACLE_ADDRESS, WBTC_ORACLE_ADDRESS]
    asset_name_list = ['uniBTC', 'wBTC']


    i = 0

    price_list = []

    while i < len(oracle_address_list):
        oracle_address = oracle_address_list[i]

        oracle_contract = lph.get_contract(oracle_address, ORACLE_ABI, web3)

        oracle_price = oracle_contract.functions.latestAnswer().call()
        
        price_list.append(oracle_price)

        time.sleep(WAIT_TIME)

        i += 1

    run_df = pd.DataFrame()

    current_time = int(time.time())

    run_df['asset_name'] = asset_name_list
    run_df['asset_price'] = price_list
    run_df['timestamp'] = current_time

    try:
        cloud_df = cs.read_zip_csv_from_cloud_storage(CLOUD_FILENAME, BUCKET_NAME)
    except:
        cloud_df = pd.DataFrame()

    if len(cloud_df) > 0:
        df = pd.concat([run_df, cloud_df])
    
    else:
        df = run_df
    
    df = df.drop_duplicates(subset=['asset_name', 'asset_price', 'timestamp'])
    
    cs.df_write_to_cloud_storage_as_zip(df, CLOUD_FILENAME, BUCKET_NAME)
    
    return df
