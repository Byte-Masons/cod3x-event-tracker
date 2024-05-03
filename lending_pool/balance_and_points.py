from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import time
import datetime

def set_unique_users(csv_name):
    df = pd.read_csv(csv_name, usecols=['to_address'], dtype={'to_address': str})

    df = df.drop_duplicates(subset=['to_address'])

    # df.to_csv('unique_user_list.csv', index=False)

    return df

def get_unique_users():
    df = pd.read_csv('unique_user_list.csv')

    return df


def get_token_config_df(index):
    df = pd.read_csv('token_config.csv')
    df = df.loc[df['chain_index'] == index]

    return df

def get_deposit_token_df(index):
    token_df = get_token_config_df(index)

    token_list = token_df['token_name'].tolist()

    token_list = [token for token in token_list if token[0] != 'v']

    deposit_df = pd.DataFrame()

    deposit_df['token_name'] = token_list

    token_df = token_df.loc[token_df['token_name'].isin(token_list)]

    return token_df

def get_borrow_token_df(index):
    token_df = get_token_config_df(index)

    token_list = token_df['token_name'].tolist()

    token_list = [token for token in token_list if token[0] == 'v']

    borrow_df = pd.DataFrame()

    borrow_df['token_name'] = token_list

    token_df = token_df.loc[token_df['token_name'].isin(token_list)]

    return token_df

#makes a dataframe and stores it in a csv file
def make_user_data_csv(df, csv_name):

    old_df = pd.read_csv(csv_name, dtype={'user_address': str, 'token_address': str, 'token_volume': float, 'tx_hash': str, 'timestamp': float})
    old_df = old_df.drop_duplicates(subset=['user_address','token_address', 'token_volume', 'tx_hash'], keep='last')

    combined_df_list = [df, old_df]
    combined_df = pd.concat(combined_df_list)
    combined_df = combined_df.drop_duplicates(subset=['user_address','token_address', 'token_volume', 'tx_hash'], keep='last')
    
    if len(combined_df) >= len(old_df):
        combined_df.to_csv(csv_name, index=False)
        print()
        print('Event CSV Updated. Old Length: ', len(old_df), ' New Length: ', len(combined_df), ' Events Added: ', len(combined_df) - len(old_df))
        print()
    return

# # finds if a transaction adds to or reduces a balance 
# # (deposit + borrow add to a balance and withdraw + repay reduce a balance)
def set_token_flows(csv_name, index):
    event_df = pd.read_csv(csv_name, usecols=['from_address','to_address','timestamp','token_address', 'token_volume','tx_hash'], dtype={'from_address': str,'to_address': str,'timestamp' : str,'token_address': str, 'token_volume': float,'tx_hash': str})
    
    # # tries to remove the null address to greatly reduce computation needs
    event_df = event_df.loc[event_df['to_address'] != '0x0000000000000000000000000000000000000000']
    
    unique_user_df = set_unique_users(csv_name)

    unique_user_list = unique_user_df['to_address'].to_list()

    deposit_token_df = get_deposit_token_df(index)
    borrow_token_df = get_borrow_token_df(index)

    deposit_token_list = deposit_token_df['token_address'].tolist()
    borrow_token_list = borrow_token_df['token_address'].tolist()

    token_flow_csv = 'token_flow.csv'

    i = 1

    combo_df = pd.DataFrame()
    temp_df = pd.DataFrame()

    # # handles deposits and borrows
    temp_df = event_df.loc[event_df['to_address'].isin(unique_user_list)]
    deposit_df = temp_df.loc[temp_df['token_address'].isin(deposit_token_list)]
    borrow_df = temp_df.loc[temp_df['token_address'].isin(borrow_token_list)]

    # # handles withdrawals and repays
    temp_df = event_df.loc[event_df['from_address'].isin(unique_user_list)]
    withdraw_df = temp_df.loc[temp_df['token_address'].isin(deposit_token_list)]
    repay_df = temp_df.loc[temp_df['token_address'].isin(borrow_token_list)]

    withdraw_df['token_volume'] = [x * -1 for x in withdraw_df['token_volume'].tolist()]
    repay_df['token_volume'] = [x * -1 for x in repay_df['token_volume'].tolist()]

    deposit_df['user_address'] = deposit_df['to_address']
    borrow_df['user_address'] = borrow_df['to_address']
    
    withdraw_df['user_address'] = withdraw_df['from_address']
    repay_df['user_address'] = repay_df['from_address']

    combo_df = pd.concat([deposit_df, borrow_df, withdraw_df, repay_df])
    combo_df = combo_df[['user_address', 'tx_hash', 'token_address','token_volume', 'timestamp']]

    # make_user_data_csv(combo_df, token_flow_csv)

    return combo_df

def get_token_flows():
    df = pd.read_csv('token_flow.csv', dtype={'from_address': str,'to_address': str,'timestamp' : float,'token_address': str, 'token_volume': float,'tx_hash': str})
    
    return df

def set_first_n_addresses(number_of_addresses):
    df = get_token_flows()
    df = df.sort_values(by=['timestamp'], ascending=True)
    df = df.drop_duplicates(subset=['user_address'], keep='first')
    df = df.reset_index()
    df = df[:number_of_addresses]
    df = df[['user_address','tx_hash','token_address','token_volume','timestamp']]
    df.to_csv('first_users.csv', index=False)

    return

def get_first_n_addresses():
    df = pd.read_csv('first_users.csv')

    return df

def set_rolling_balance(df):
    # df = get_token_flows()

    # df = df.loc[df['user_address'] == '0xE692256D270946A407f8Ba9885D62e883479F0b8']
    df.sort_values(by=['timestamp'], ascending=True)

    # Group the DataFrame by 'name' and calculate cumulative sum
    name_groups = df.groupby(['user_address','token_address'])['token_volume'].transform(pd.Series.cumsum)

    # Print the DataFrame with the new 'amount_cumulative' column
    df = df.assign(amount_cumulative=name_groups)

    # df.to_csv('rolling_balance.csv', index=False)

    return df

def get_rolling_balance():
    df = pd.read_csv('rolling_balance.csv', dtype={'user_address': str,'tx_hash': str, 'token_address':str, 'token_volume': float, 'timestamp': float, 'amount_cumulative': float})
    
    return df

def get_aave_oracle_abi():
    abi = [{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address[]","name":"sources","type":"address[]"},{"internalType":"address","name":"fallbackOracle","type":"address"},{"internalType":"address","name":"baseCurrency","type":"address"},{"internalType":"uint256","name":"baseCurrencyUnit","type":"uint256"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"asset","type":"address"},{"indexed":True,"internalType":"address","name":"source","type":"address"}],"name":"AssetSourceUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"baseCurrency","type":"address"},{"indexed":False,"internalType":"uint256","name":"baseCurrencyUnit","type":"uint256"}],"name":"BaseCurrencySet","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"fallbackOracle","type":"address"}],"name":"FallbackOracleUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":True,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"BASE_CURRENCY","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"BASE_CURRENCY_UNIT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getAssetPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"}],"name":"getAssetsPrices","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getFallbackOracle","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getSourceOfAsset","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address[]","name":"sources","type":"address[]"}],"name":"setAssetSources","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"fallbackOracle","type":"address"}],"name":"setFallbackOracle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}]

    return abi

# # makes our web3 object and injects it's middleware
def get_web_3(rpc_url):

    if 'wss' in rpc_url:
        provider = Web3.WebsocketProvider(rpc_url)
        web3 = Web3(provider)
    else:
        web3 = Web3(Web3.HTTPProvider(rpc_url))
    time.sleep(2.5)
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    time.sleep(2.5)
    
    return web3

# # gets our web3 contract object
def get_contract(contract_address, contract_abi, web3):

    contract = web3.eth.contract(address=contract_address, abi=contract_abi)
    
    return contract

# # reads from our static config csv
def get_lp_config_df():
    lp_config_df = pd.read_csv('lp_config.csv')

    return lp_config_df

# # returns the relevant value from our config csv
def get_lp_config_value(column_name, index):
    df = get_lp_config_df()

    config_list = df[column_name].tolist()

    config_value = config_list[index]

    return config_value

# # returns our token_config value
def get_token_config_value(column_name, token_address, index):
    df = get_token_config_df(index)

    df = df.loc[df['chain_index'] == index]

    temp_df = df.loc[df['token_address'] == token_address]

    if len(temp_df) < 1:
        df = df.loc[df['underlying_address'] == token_address]

    config_list = df[column_name].tolist()

    config_value = config_list[0]

    return config_value

def get_tx_usd_amount(reserve_address, token_amount, web3, index):

    asset_price_tx_usd_value_list = []

    contract_address = get_lp_config_value('aave_oracle_address', index)
    contract_abi = get_aave_oracle_abi()

    contract = get_contract(contract_address, contract_abi, web3)

    value_usd = contract.functions.getAssetPrice(reserve_address).call()
    time.sleep(0.1)
    decimals = get_token_config_value('decimals', reserve_address, index)
    usd_amount = (value_usd/1e8)*(token_amount/decimals)
    # print(usd_amount)
    asset_price_tx_usd_value_list.append(value_usd/1e8)
    asset_price_tx_usd_value_list.append(usd_amount)

    return asset_price_tx_usd_value_list

def get_time_difference(df):

    # # df = df.loc[df['user_address'] == '0xE692256D270946A407f8Ba9885D62e883479F0b8']
    # # df = df.loc[df['user_address'] == '0x67D69CA5B47F7d45D9A7BB093479fcA732023dfa']

    # Sort by Name and timestamp for correct grouping
    df = df.sort_values(by=['user_address', 'token_address', 'timestamp'])

    # Calculate time difference for each name group
    df['timestamp'] = df['timestamp'].astype(float)
    time_diff = df.groupby(['user_address', 'token_address'])['timestamp'].diff()

    # Handle the first row for each name (no difference)
    # time_diff.iloc[::2] = pd.NA  # Set difference to NaN for the first row of each name group

    # Calculate difference in seconds (adjust as needed)
    time_diff_seconds = time_diff.fillna(0)

    df['time_difference'] = time_diff_seconds

    # Print the time difference with formatting (optional)
    # print(time_diff_seconds.astype(int).rename('time_diff_seconds'))
    return df

def calculate_accrued_points(df):
  df['previous_amount'] = df['amount_cumulative'].shift(1)
  df['accrued_embers'] = (df['embers'] * df['time_difference'] / 86400) * df['previous_amount'].fillna(0)

  df = df[['user_address', 'token_address', 'tx_hash', 'timestamp', 'time_difference', 'embers', 'amount_cumulative', 'accrued_embers']]
  return df

def set_realized_embers(df):

    df = df.groupby(['user_address','token_address']).apply(calculate_accrued_points)

    return df

def get_last_tracked_embers(df):

    df['timestamp'] = df['timestamp'].astype(float)
    df['accrued_embers'] = df['accrued_embers'].astype(float)
    # Group by wallet_address and token_address
    grouped_df = df.groupby(['user_address', 'token_address'])

    ember_balance = grouped_df['accrued_embers'].sum()

    # df['ember_balance'] = ember_balance.reset_index(drop=True)  # Drop unnecessary index

    # Get max embers and corresponding timestamp using agg
    max_embers_df = grouped_df.agg(max_embers=('accrued_embers', max), max_timestamp=('timestamp', max))

    # Reset index to remove multi-level indexing
    max_embers_df = max_embers_df.reset_index()

    max_embers_df['max_embers'] = ember_balance.reset_index(drop=True) 
    timestamp_list = max_embers_df['max_timestamp'].tolist()
    timestamp_list = [float(timestamp) for timestamp in timestamp_list]

    max_embers_df.rename(columns = {'max_timestamp':'timestamp', 'max_embers': 'ember_balance'}, inplace = True) 

    merged_df = max_embers_df.merge(df, how='inner', on=['user_address', 'token_address', 'timestamp'])

    merged_df = merged_df[['user_address', 'token_address', 'tx_hash', 'timestamp', 'time_difference', 'embers', 'amount_cumulative', 'ember_balance']]

    # print(merged_df)
    # Set amount_cumulative values less than 0 to 0 (in-place modification)
    merged_df['amount_cumulative'] = merged_df['amount_cumulative'].clip(lower=0)
    merged_df['ember_balance'] = merged_df['ember_balance'].clip(lower=0)
    
    # print(merged_df)

    return merged_df

# # function we will apply to out dataframe to estimate how many embers users have earned since their last event
def simulate_accrued_points(df):
  df['ember_balance'] += (df['embers'] * df['time_difference'] / 86400) * df['amount_cumulative'].fillna(0)

  df = df[['user_address', 'token_address', 'tx_hash', 'timestamp', 'time_difference', 'embers', 'amount_cumulative', 'ember_balance']]
  return df

# # takes in a dataframe with the last known balance and accrued ember amount
# # outputs a dataframe with expected accrued embers since last event
def accrue_latest_embers(df):

    current_unix = int(time.time())
    df['time_difference'] = current_unix - df['timestamp']

    df = df.groupby(['user_address','token_address']).apply(simulate_accrued_points)

    df['total_ember_balance'] = df.groupby('user_address')['ember_balance'].transform('sum')

    df = df.loc[df['total_ember_balance'] > 0]

    return df

def set_embers_2(index):
    csv_name = get_lp_config_value('event_csv_name', index)

    df = set_token_flows(csv_name, index)

    df = set_rolling_balance(df)

    config_df = get_token_config_df(index)

    reserve_address_list = config_df['underlying_address'].tolist()
    token_address_list = config_df['token_address'].tolist()
    embers_list = config_df['embers'].tolist()
    
    contract_address = get_lp_config_value('aave_oracle_address', index)
    contract_abi = get_aave_oracle_abi()
    rpc_url = get_lp_config_value('rpc_url', index)
    web3 = get_web_3(rpc_url)

    contract = get_contract(contract_address, contract_abi, web3)

    df_list = []

    i = 0
    while i < len(reserve_address_list):
        reserve_address = reserve_address_list[i]
        token_address = token_address_list[i]
        embers = embers_list[i]
        
        time.sleep(0.25)
        value_usd = contract.functions.getAssetPrice(reserve_address).call()
        time.sleep(0.25)

        value_usd = value_usd / 1e8

        decimals = get_token_config_value('decimals', reserve_address, index)

        temp_df = df.loc[df['token_address'] == token_address]

        if len(temp_df) > 0:

            temp_df['amount_cumulative'] = temp_df['amount_cumulative'] / decimals
            temp_df['amount_cumulative'] = temp_df['amount_cumulative'] * value_usd
            temp_df['embers'] = embers


            df_list.append(temp_df)
        # print(temp_df[['user_address', 'amount_cumulative']])
        
        i += 1
    
    df = pd.concat(df_list)

    df = get_time_difference(df)

    df = df.reset_index(drop=True)

    df = set_realized_embers(df)

    df = get_last_tracked_embers(df)

    df = accrue_latest_embers(df)

    df.to_csv('test.csv', index=False)
    
    return df

def set_embers(index):

    df = get_rolling_balance()

    # df = df.loc[df['user_address'].isin(['0x67D69CA5B47F7d45D9A7BB093479fcA732023dfa', '0xE692256D270946A407f8Ba9885D62e883479F0b8'])]

    # # df = df.loc[df['user_address'] != '0x0000000000000000000000000000000000000000']

    config_df = get_token_config_df(index)

    reserve_address_list = config_df['underlying_address'].tolist()
    token_address_list = config_df['token_address'].tolist()
    embers_list = config_df['embers'].tolist()
    
    contract_address = get_lp_config_value('aave_oracle_address', index)
    contract_abi = get_aave_oracle_abi()
    rpc_url = get_lp_config_value('rpc_url', index)
    web3 = get_web_3(rpc_url)

    contract = get_contract(contract_address, contract_abi, web3)

    df_list = []

    i = 0
    while i < len(reserve_address_list):
        reserve_address = reserve_address_list[i]
        token_address = token_address_list[i]
        embers = embers_list[i]
        
        time.sleep(0.25)
        value_usd = contract.functions.getAssetPrice(reserve_address).call()
        time.sleep(0.25)

        value_usd = value_usd / 1e8

        decimals = get_token_config_value('decimals', reserve_address, index)

        temp_df = df.loc[df['token_address'] == token_address]

        if len(temp_df) > 0:

            temp_df['amount_cumulative'] = temp_df['amount_cumulative'] / decimals
            temp_df['amount_cumulative'] = temp_df['amount_cumulative'] * value_usd
            temp_df['embers'] = embers


            df_list.append(temp_df)
        # print(temp_df[['user_address', 'amount_cumulative']])
        
        i += 1
    
    df = pd.concat(df_list)

    df = get_time_difference(df)

    df = df.reset_index(drop=True)

    df.to_csv('test.csv', index=False)
    
    return df
    
csv_name = 'ironclad_events.csv'
index = 0

# df = set_embers(index)

# df = set_realized_embers(df)

# df = get_last_tracked_embers(df)

# df = accrue_latest_embers(df)

# df.to_csv('test.csv', index=False)

df = set_embers_2(index)
print(df)