from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import time
import datetime
import sqlite3
from sql_interfacer import sql
from cloud_storage import cloud_storage

def set_unique_users(cursor):
    column_list = ['to_address']

    rows = sql.select_specific_columns(cursor, column_list)

    df = sql.get_sql_df(rows, column_list)

    df = df.drop_duplicates(subset=['to_address'])

    # df.to_csv('unique_user_list.csv', index=False)

    return df

def get_unique_users():
    df = pd.read_csv('unique_user_list.csv')

    return df


def get_token_config_df(index):
    df = pd.read_csv('./config/token_config.csv')
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
def set_token_flows(event_df, cursor, index):
    # event_df = pd.read_csv(csv_name, usecols=['from_address','to_address','timestamp','token_address', 'token_volume','tx_hash'], dtype={'from_address': str,'to_address': str,'timestamp' : str,'token_address': str, 'token_volume': float,'tx_hash': str})
    
    # # tries to remove the null address to greatly reduce computation needs
    # event_df = event_df.loc[event_df['to_address'] != '0x0000000000000000000000000000000000000000']
    
    unique_user_df = set_unique_users(cursor)

    unique_user_list = unique_user_df['to_address'].to_list()

    deposit_token_df = get_deposit_token_df(index)
    borrow_token_df = get_borrow_token_df(index)

    deposit_token_list = deposit_token_df['token_address'].tolist()
    borrow_token_list = borrow_token_df['token_address'].tolist()

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

    # # tries to remove the null address to greatly reduce computation needs
    combo_df = combo_df.loc[combo_df['user_address'] != '0x0000000000000000000000000000000000000000']

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
    lp_config_df = pd.read_csv('./config/lp_config.csv')

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

    return df

def calculate_accrued_points(df):

    # # temporarily reduces sample size to two people
    # df = df.loc[df['user_address'].isin(['0x67D69CA5B47F7d45D9A7BB093479fcA732023dfa', '0xE692256D270946A407f8Ba9885D62e883479F0b8'])]

    df['previous_amount'] = df['amount_cumulative'].shift(1)
    df['accrued_embers'] = (df['embers'] * df['time_difference'] / 86400) * df['previous_amount'].fillna(0)
    
    # # Set accrued_embers to 0 for rows with timestamps before a specific threshold (March 22nd in this case)
    # df.loc[df['timestamp'] < 1711080000, 'accrued_embers'] = 0

    df = df[['user_address', 'token_address', 'tx_hash', 'timestamp', 'time_difference', 'embers', 'amount_cumulative', 'accrued_embers', 'token_cumulative']]
    return df

def get_double_ember_list():
    double_ember_token_list = ['0xe7334Ad0e325139329E747cF2Fc24538dD564987', '0x02CD18c03b5b3f250d2B29C87949CDAB4Ee11488', '0x272CfCceFbEFBe1518cd87002A8F9dfd8845A6c4', '0x58254000eE8127288387b04ce70292B56098D55C', 
                               '0xC17312076F48764d6b4D263eFdd5A30833E311DC', '0xe5415Fa763489C813694D7A79d133F0A7363310C', '0xBcE07537DF8AD5519C1d65e902e10aA48AF83d88', '0x5eEA43129024eeE861481f32c2541b12DDD44c08', 
                               '0x05249f9Ba88F7d98fe21a8f3C460f4746689Aea5', '0x3F332f38926b809670b3cac52Df67706856a1555', '0x4522DBc3b2cA81809Fa38FEE8C1fb11c78826268', '0xF8D68E1d22FfC4f09aAA809B21C46560174afE9c']
    
    return double_ember_token_list

def get_quadriple_ember_list():
    quadriple_ember_token_list = ['0x9c29a8eC901DBec4fFf165cD57D4f9E03D4838f7', '0xe3f709397e87032E61f4248f53Ee5c9a9aBb6440', '0x06D38c309d1dC541a23b0025B35d163c25754288', '0x083E519E76fe7e68C15A6163279eAAf87E2addAE']
    
    return quadriple_ember_token_list

def set_realized_embers(df):

    df['timestamp'] = df['timestamp'].astype(float)

    double_ember_token_list = get_double_ember_list()
    quadriple_ember_token_list = get_quadriple_ember_list()

    df = df.groupby(['user_address','token_address']).apply(calculate_accrued_points)

    # 1711080000 = March 22d the start of the embers program
    df.loc[df['timestamp'] < 1711080000, 'accrued_embers'] = 0 

    # 1714147200 = April 26th when 2x and 4x embers began
    df.loc[(df['timestamp'] >= 1714147200) & (df['token_address'].isin(double_ember_token_list)), 'accrued_embers'] *= 2
    df.loc[(df['timestamp'] >= 1714147200) & (df['token_address'].isin(quadriple_ember_token_list)), 'accrued_embers'] *= 4

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

    merged_df = merged_df[['user_address', 'token_address', 'tx_hash', 'timestamp', 'time_difference', 'embers', 'amount_cumulative', 'ember_balance', 'token_cumulative']]

    # Set amount_cumulative values less than 0 to 0 (in-place modification)
    merged_df['amount_cumulative'] = merged_df['amount_cumulative'].clip(lower=0)
    merged_df['ember_balance'] = merged_df['ember_balance'].clip(lower=0)
    

    return merged_df

# # function we will apply to out dataframe to estimate how many embers users have earned since their last event
def simulate_accrued_points(df):
  df['ember_balance'] += (df['embers'] * df['time_difference'] / 86400) * df['amount_cumulative'].fillna(0)

  df = df[['user_address', 'token_address', 'tx_hash', 'timestamp', 'time_difference', 'embers', 'amount_cumulative', 'ember_balance']]
  return df

# # function we will apply to out dataframe to estimate how many embers users have earned since their last event
def simulate_regular_accrued_points(df):
  df['regular_ember_balance'] += (df['embers'] * df['time_difference'] / 86400) * df['amount_cumulative'].fillna(0)

  df = df[['user_address', 'token_address', 'tx_hash', 'timestamp', 'time_difference', 'embers', 'amount_cumulative', 'ember_balance', 'regular_ember_balance', 'token_cumulative']]
  return df

# # function we will apply to out dataframe to estimate how many embers users have earned since their last event
def simulate_multiplier_accrued_points(df):
  df['multiplier_ember_balance'] += (df['embers'] * df['time_difference'] / 86400) * df['amount_cumulative'].fillna(0)

  df = df[['user_address', 'token_address', 'tx_hash', 'timestamp', 'time_difference', 'embers', 'amount_cumulative', 'ember_balance', 'regular_ember_balance', 'multiplier_ember_balance', 'token_cumulative']]
  return df

# # takes in a dataframe with the last known balance and accrued ember amount
# # outputs a dataframe with expected accrued embers since last event
def accrue_latest_embers(df):

    double_ember_token_list = get_double_ember_list()
    quadriple_ember_token_list = get_quadriple_ember_list()

    current_unix = int(time.time())

    ember_start_unix = 1711080000
    point_multiplier_start_unix = 1714147200

    # df['time_difference'] = current_unix - df['timestamp']

    # time within the the regular accrural period
    df['time_difference'] = df['timestamp'] - ember_start_unix

    # if a user's last transaction was before the ember start time
    df.loc[df['time_difference'] < 0, 'time_difference'] = (point_multiplier_start_unix - ember_start_unix)

    # if a user's last transaction was between the start and ending of regular embers
    df.loc[(df['timestamp'] > ember_start_unix) & (df['timestamp'] < point_multiplier_start_unix), 'time_difference'] = point_multiplier_start_unix - df['timestamp']

    df.loc[df['timestamp'] > point_multiplier_start_unix, 'time_difference'] = 0

    # df['time_difference'] = point_multiplier_start_unix - ember_start_unix

    df['regular_ember_balance'] = 0

    df = df.groupby(['user_address','token_address']).apply(simulate_regular_accrued_points)

    # # accrues regular embers to users > ember start time and < multiplier start time
    # baseline sees what people's time difference is
    df['time_difference'] = df['timestamp'] - point_multiplier_start_unix
    # if someone has a < 0 time difference, we just make them current unix timestamp - the start of multipier points :)
    df.loc[df['time_difference'] < 0, 'time_difference'] = current_unix - point_multiplier_start_unix
    df.loc[df['timestamp'] > point_multiplier_start_unix, 'time_difference'] = current_unix - df['timestamp']

    # df['time_difference'] = current_unix - point_multiplier_start_unix

    df['multiplier_ember_balance'] = 0

    df = df.groupby(['user_address','token_address']).apply(simulate_multiplier_accrued_points)

    df.loc[df['token_address'].isin(double_ember_token_list), 'multiplier_ember_balance'] *= 2

    df.loc[df['token_address'].isin(quadriple_ember_token_list), 'multiplier_ember_balance'] *= 4

    df['total_ember_balance'] = df.groupby('user_address')['ember_balance'].transform('sum')
    df['total_ember_balance'] += df.groupby('user_address')['regular_ember_balance'].transform('sum')
    df['total_ember_balance'] += df.groupby('user_address')['multiplier_ember_balance'].transform('sum')

    df = df.loc[df['total_ember_balance'] > 0]

    return df

def set_embers_full(index):
    csv_name = get_lp_config_value('event_csv_name', index)


    df = set_token_flows(csv_name, index)

    print('set_token_flows complete')

    df = set_rolling_balance(df)

    print('set_token_flows complete')

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
        
        i += 1
    
    print('amount_cumulative clean up complete')
    
    df = pd.concat(df_list)

    # makes the lowest accumualtive = 0 (user's can't have negative balances)
    df['amount_cumulative'] = df['amount_cumulative'].clip(lower=0)

    df = get_time_difference(df)

    print('time_difference complete')


    df = df.reset_index(drop=True)

    df = set_realized_embers(df)

    print('set_realized_embers complete')


    df = get_last_tracked_embers(df)

    df = accrue_latest_embers(df)

    df.loc[df['user_address'] == '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF', 'user_address'] = '0x5bC7b531B1a8810c74E53C4b81ceF4F8f911921F'
    # df = df.loc[df['user_address'] != '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF']
    df = df.loc[df['user_address'] != '0x6387c7193B5563DD17d659b9398ACd7b03FF0080']
    df = df.loc[df['user_address'] != '0x0000000000000000000000000000000000000000']
    df = df.loc[df['user_address'] != '0x2dDD3BCA2Fa050532B8d7Fd41fB1449382187dAA']

    df.to_csv('test.csv', index=False)

    return df

# # gets rid of our blacklisted addresses in our dataframe
def drop_blacklisted_addresses(df):
    
    df.loc[df['user_address'] == '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF', 'user_address'] = '0x5bC7b531B1a8810c74E53C4b81ceF4F8f911921F'
    # df = df.loc[df['user_address'] != '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF']
    df = df.loc[df['user_address'] != '0x6387c7193B5563DD17d659b9398ACd7b03FF0080']
    df = df.loc[df['user_address'] != '0x0000000000000000000000000000000000000000']
    df = df.loc[df['user_address'] != '0x2dDD3BCA2Fa050532B8d7Fd41fB1449382187dAA']

    return df

# # updates embers from our database
def set_embers_database(index):

    connection = sqlite3.connect("turtle.db")

    cursor = connection.cursor()

    column_list = ['from_address','to_address','timestamp','token_address', 'token_volume','tx_hash']

    rows = sql.select_specific_columns(cursor, column_list)

    df = sql.get_sql_df(rows, column_list)
    df['token_volume'] = df['token_volume'].astype(float)

    df = set_token_flows(df, cursor, index)
    print('set_token_flows complete')
    
    df = drop_blacklisted_addresses(df)

    df = set_rolling_balance(df)
    print('set_rolling_balances complete')

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
            temp_df['token_cumulative'] = temp_df['amount_cumulative']
            temp_df['amount_cumulative'] = temp_df['amount_cumulative'] * value_usd
            temp_df['embers'] = embers

            df_list.append(temp_df)
        
        i += 1
    
    print('amount_cumulative clean up complete')
    
    df = pd.concat(df_list)

    # makes the lowest accumualtive = 0 (user's can't have negative balances)
    df['amount_cumulative'] = df['amount_cumulative'].clip(lower=0)
    df['token_cumulative'] = df['token_cumulative'].clip(lower=0)

    df = get_time_difference(df)

    print('time_difference complete')

    df = df.reset_index(drop=True)

    df = set_realized_embers(df)
    
    print('set_realized_embers complete')

    df = get_last_tracked_embers(df)
    print('get_last_tracked_embers complete')

    df = accrue_latest_embers(df)
    print('accrue_latest_embers complete')

    # sql.make_new_snapshot_table(cursor, 'snapshot', df)

    # rows = sql.select_star(cursor, 'snapshot')

    # column_list = ['user_address','token_address','tx_hash','timestamp','time_difference','embers','amount_cumulative','ember_balance','total_ember_balance','token_cumulative']
    
    # df = sql.get_sql_df(rows, column_list)

    # user_sum_df = df.groupby('user_address')['amount_cumulative'].sum().reset_index()

    # # makes a 1 item summary of our user with their total_tvl + total_ember_balance
#     user_summary_df = df.groupby('user_address').agg(
#     amount_cumulative=('amount_cumulative', sum),
#     total_ember_balance=('total_ember_balance', 'last')  # Assuming the latest total_embers is desired
# ).reset_index()

    try:
        cloud_storage.df_write_to_cloud_storage(df, 'current_user_tvl_embers.csv', 'cooldowns2')
    except:
        print("Couldn't write to bucket")


    return df

# # returns the tvl and embers for a single user
def set_single_user_stats(index):

    connection = sqlite3.connect("turtle.db")

    cursor = connection.cursor()

    df = cloud_storage.read_from_cloud_storage('current_user_tvl_embers.csv', 'cooldowns2')

    column_list = ['from_address','to_address','timestamp','token_address', 'token_volume','tx_hash']

    rows = sql.select_specific_columns(cursor, column_list)

    df = sql.get_sql_df(rows, column_list)
    df['token_volume'] = df['token_volume'].astype(float)

    df = set_token_flows(df, cursor, index)
    print('set_token_flows complete')
    
    df = drop_blacklisted_addresses(df)

    df = set_rolling_balance(df)
    print('set_rolling_balances complete')

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
            temp_df['token_cumulative'] = temp_df['amount_cumulative']
            temp_df['amount_cumulative'] = temp_df['amount_cumulative'] * value_usd
            temp_df['embers'] = embers

            df_list.append(temp_df)
        
        i += 1
    
    print('amount_cumulative clean up complete')
    
    df = pd.concat(df_list)

    # makes the lowest accumualtive = 0 (user's can't have negative balances)
    df['amount_cumulative'] = df['amount_cumulative'].clip(lower=0)
    df['token_cumulative'] = df['token_cumulative'].clip(lower=0)

    df = get_time_difference(df)

    print('time_difference complete')

    df = df.reset_index(drop=True)

    df = set_realized_embers(df)
    
    print('set_realized_embers complete')

    df = get_last_tracked_embers(df)
    print('get_last_tracked_embers complete')

    df = accrue_latest_embers(df)
    print('accrue_latest_embers complete')

    try:
        cloud_storage.df_write_to_cloud_storage(df, 'current_user_tvl_embers.csv', 'cooldowns2')
    except:
        print("Couldn't write to bucket")


    return df

def filter_after_snapshot(df, embers_snapshot_df):
  """
  Filters rows in 'df' that occur after corresponding entries in 'embers_snapshot_df'

  Args:
      df: The DataFrame containing data to be filtered.
      embers_snapshot_df: The DataFrame containing the snapshot reference.

  Returns:
      A new DataFrame containing filtered rows from 'df'.
  """
  print(df)
  print(embers_snapshot_df)

  filtered_rows = []
  for index, row in df.iterrows():
    user_address = row['user_address']
    token_address = row['token_address']
    timestamp = row['timestamp']

    # Find matching snapshot entry
    snapshot_entry = embers_snapshot_df[(embers_snapshot_df['user_address'] == user_address) & 
                                        (embers_snapshot_df['token_address'] == token_address)]

    # Check if snapshot entry exists and timestamp is after
    if not snapshot_entry.empty and timestamp > snapshot_entry.iloc[0]['timestamp']:
      filtered_rows.append(row.to_dict())

  return pd.DataFrame(filtered_rows)

# # uses our last embers snapshot to build upon
def set_embers_database_smart():
    connection = sqlite3.connect("turtle.db")

    cursor = connection.cursor()

    column_list = ['from_address','to_address','timestamp','token_address', 'token_volume','tx_hash']

    rows = sql.select_specific_columns(cursor, column_list)

    df = sql.get_sql_df(rows, column_list)
    df[['token_volume', 'timestamp']] = df[['token_volume', 'timestamp']].astype(float)

    df = set_token_flows(df, cursor)

    embers_snapshot_df = pd.read_csv('./test/current_user_tvl_embers.csv')

    filtered_df = filter_after_snapshot(df.copy(), embers_snapshot_df.copy())

    return filtered_df

def get_first_timestamp(group):
  # Select one tx_hash (any from the group)
  tx_hash = group['tx_hash'].iloc[0]
  # Get the first timestamp
  first_timestamp = group['timestamp'].min()
  return pd.Series({'user_address': group['user_address'].iloc[0], 'tx_hash': tx_hash, 'first_timestamp': first_timestamp})

def find_median_stats(ember_csv_name, deposit_list, borrow_list, minimum_value):

    df = pd.read_csv(ember_csv_name, usecols=['user_address', 'token_address', 'amount_cumulative'], dtype={'user_address': str, 'token_address': str, 'amount_cumulative': float})
    median_df = df[['user_address', 'token_address', 'amount_cumulative']]

    deposit_df = median_df.loc[median_df['token_address'].isin(deposit_list)]
    borrow_df = median_df.loc[median_df['token_address'].isin(borrow_list)]

    # Group by wallet_address and sum the amount for each token
    total_amounts = deposit_df.groupby('user_address')['amount_cumulative'].sum()

    # Create a DataFrame with wallet_address and total amount
    deposit_df = pd.DataFrame({'user_address': total_amounts.index, 'amount_cumulative': total_amounts.values})

    # Group by wallet_address and sum the amount for each token
    total_amounts = borrow_df.groupby('user_address')['amount_cumulative'].sum()

    # Create a DataFrame with wallet_address and total amount
    borrow_df = pd.DataFrame({'user_address': total_amounts.index, 'amount_cumulative': total_amounts.values})


    deposit_df = deposit_df.loc[deposit_df['amount_cumulative'] >= minimum_value]
    borrow_df = borrow_df.loc[borrow_df['amount_cumulative'] >= minimum_value]

    # median_deposit = deposit_df['amount_cumulative'].median()
    # median_borrow = borrow_df['amount_cumulative'].median()

    print(deposit_df)
    print(borrow_df)

    deposit_df.to_csv('deposit.csv', index=False)
    borrow_df.to_csv('borrow.csv', index=False)
    return

# csv_name = 'ironclad_events.csv'
# snapshot_csv_name = 'snapshot_events.csv'
# ember_csv_name = 'test.csv'
# index = 0
# deposit_list = ['0xe7334Ad0e325139329E747cF2Fc24538dD564987', '0x02CD18c03b5b3f250d2B29C87949CDAB4Ee11488', '0x9c29a8eC901DBec4fFf165cD57D4f9E03D4838f7', '0x272CfCceFbEFBe1518cd87002A8F9dfd8845A6c4',
#                 '0x58254000eE8127288387b04ce70292B56098D55C', '0xe3f709397e87032E61f4248f53Ee5c9a9aBb6440', '0xC17312076F48764d6b4D263eFdd5A30833E311DC', '0x4522DBc3b2cA81809Fa38FEE8C1fb11c78826268']

# borrow_list = ['0xe5415Fa763489C813694D7A79d133F0A7363310C', '0xBcE07537DF8AD5519C1d65e902e10aA48AF83d88', '0x06D38c309d1dC541a23b0025B35d163c25754288', '0x5eEA43129024eeE861481f32c2541b12DDD44c08',
#             '0x05249f9Ba88F7d98fe21a8f3C460f4746689Aea5', '0x083E519E76fe7e68C15A6163279eAAf87E2addAE', '0x3F332f38926b809670b3cac52Df67706856a1555', '0xF8D68E1d22FfC4f09aAA809B21C46560174afE9c']

# minimum_value = 1000

# set_embers_smart()