import pandas as pd
from web3 import Web3
from web3.middleware import geth_poa_middleware  
import time

# import transaction_finder as tf
# rpc_url = 'https://mainnet.mode.network'

# contract_address = '0x3a4ea21729f8556547E1243608872C8827DdEEd7'

# web3 = tf.get_web_3(rpc_url)

# contract_abi = [ { "inputs": [ { "internalType": "address", "name": "_proxy", "type": "address" }, { "internalType": "address", "name": "_asset", "type": "address" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "inputs": [], "name": "asset", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "decimals", "outputs": [ { "internalType": "uint8", "name": "", "type": "uint8" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "latestAnswer", "outputs": [ { "internalType": "int256", "name": "value", "type": "int256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "latestTimestamp", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "proxy", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" } ]

# contract = tf.get_contract(contract_address, contract_abi, web3)

# asset = contract.functions.asset().call()
# decimals = contract.functions.decimals().call()
# latestAnswer = contract.functions.latestAnswer().call()
# timestamp = contract.functions.latestTimestamp().call()
# proxy = contract.functions.proxy().call()

# print('Asset: ', asset)
# print('Decimals: ', decimals)
# print('Latest Answer: ', latestAnswer)
# print('Timestamp: ', timestamp)
# print('Proxy: ', proxy)

# csv_list = ['ironclad_events.csv', 'ironclad_events_2.csv', 'ironclad_events_3.csv', 'ironclad_events_4.csv', 'ironclad_events_5.csv', 'ironclad_events_6.csv', 'ironclad_events_6-1.csv', 'ironclad_events_7.csv', 'ironclad_events_8.csv', 'ironclad_events_9.csv',
#             'ironclad_events_10.csv', 'ironclad_events_22.csv', 'ironclad_events_99.csv']

# i = 1

# while i < len(csv_list):
#     print(i)
#     df = pd.read_csv('ironclad_events_combined.csv', dtype={'from_address': str, 'to_address': str, 'tx_hash': str, 'timestamp': str, 'token_address': str, 'reserve_address': str, 'token_volume': str, 'asset_price': str, 'usd_token_amount': str, 'log_index': str, 'transaction_index': str, 'block_number': str})

#     df_2 = pd.read_csv(csv_list[i], dtype={'from_address': str, 'to_address': str, 'tx_hash': str, 'timestamp': str, 'token_address': str, 'reserve_address': str, 'token_volume': str, 'asset_price': str, 'usd_token_amount': str, 'log_index': str, 'transaction_index': str, 'block_number': str})

#     print(df_2)

#     df_list = [df, df_2]

#     combined_df = pd.concat(df_list)

#     combined_df = combined_df.drop_duplicates(subset=['tx_hash', 'log_index', 'transaction_index'])

#     print(len(df), len(df_2), len(combined_df))

#     combined_df.to_csv('ironclad_events_combined.csv', index=False)

#     i += 1


def set_botched_df(df, column_name):
    value_list = df[column_name].tolist()

    botched_index_list = []
    botched_column_list = []

    i = 0

    while i < len(value_list):

        print(i, '/', len(value_list), 'Remaining: ', len(value_list) - i)

        value = value_list[i]

        try:
            # checksum_address = Web3.to_checksum_address(value)
            value = float(value)
            # value = int(value)

        except:
            print('Index: ', i, 'Address: ', value)
            botched_index_list.append(i)
            botched_column_list.append(value)
        
        i += 1

    df = pd.DataFrame()
    df['botched_index'] = botched_index_list
    df['botched_value'] = botched_column_list
    df.to_csv('botched.csv', index=False)
    # time.sleep(0.25/3)

def get_botched_list():
    botched_list = pd.read_csv('botched.csv')
    botched_list = botched_list['botched_index'].tolist()

    return botched_list

def fix_event_df(csv_name, df, column_name, botched_list):


    print('Old Df Length: ', len(df))

    df = df.drop(botched_list, axis=0)
    # for botched_value in botched_list:
    #     df = df.loc[df[column_name] != botched_value]

    print('New Df Length: ', len(df))

    df.to_csv(csv_name, index=False)

df = pd.read_csv('ironclad_events.csv', usecols=['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number'],dtype={'from_address': str, 'to_address': str, 'tx_hash': str, 'timestamp': str, 'token_address': str, 'reserve_address': str, 'token_volume': str, 'asset_price': str, 'usd_token_amount': str, 'log_index': int, 'transaction_index': int, 'block_number': int})

df = df.drop_duplicates(subset=['tx_hash', 'log_index', 'tx_hash'])
df.to_csv('ironclad_events.csv', index=False)

# column_name = 'token_volume'
# csv_name = 'ironclad_events.csv'

# set_botched_df(df, column_name)

# botched_list = get_botched_list()

# fix_event_df(csv_name, df, column_name, botched_list)