import pandas as pd
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

csv_list = ['ironclad_events.csv', 'ironclad_events_2.csv', 'ironclad_events_3.csv', 'ironclad_events_4.csv', 'ironclad_events_5.csv', 'ironclad_events_6.csv', 'ironclad_events_6-1.csv', 'ironclad_events_7.csv', 'ironclad_events_8.csv', 'ironclad_events_9.csv',
            'ironclad_events_10.csv', 'ironclad_events_22.csv', 'ironclad_events_99.csv']

i = 11

while i < len(csv_list):
    print(i)
    df = pd.read_csv('ironclad_events_combined.csv', dtype={'from_address': str, 'to_address': str, 'tx_hash': str, 'timestamp': str, 'token_address': str, 'reserve_address': str, 'token_volume': str, 'asset_price': str, 'usd_token_amount': str, 'log_index': str, 'transaction_index': str, 'block_number': str})

    df_2 = pd.read_csv(csv_list[i], dtype={'from_address': str, 'to_address': str, 'tx_hash': str, 'timestamp': str, 'token_address': str, 'reserve_address': str, 'token_volume': str, 'asset_price': str, 'usd_token_amount': str, 'log_index': str, 'transaction_index': str, 'block_number': str})

    print(df_2)

    df_list = [df, df_2]

    combined_df = pd.concat(df_list)

    combined_df = combined_df.drop_duplicates(subset=['tx_hash', 'log_index', 'transaction_index'])

    print(len(df), len(df_2), len(combined_df))

    combined_df.to_csv('ironclad_events_combined.csv', index=False)

    i += 1
