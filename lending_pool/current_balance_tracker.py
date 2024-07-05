import pandas as pd
from lending_pool import lending_pool_helper as lph
from sql_interfacer import sql
import time
from cloud_storage import cloud_storage

# # will return a dataframe with all users and the tokens they have interacted with on Ironclad
def get_user_token_combos(index):
    table_name = lph.get_lp_config_value('table_name', index)
    query = f"""
    SELECT DISTINCT to_address, token_address
    FROM {table_name}
"""
    column_list = ['to_address', 'token_address']
    
    df = sql.get_custom_query(query, column_list)

    return df

# # creates our balance table if it doesn't already exist
def create_balance_table():
    query = f"""
            CREATE TABLE IF NOT EXISTS current_balance(
                user_address TEXT,
                token_address TEXT,
                current_balance TEXT
                )
            """
    
    sql.create_custom_table(query)

    return

# # will write our balance entries to our balance table
def write_to_balance_table(df):

    query = """
    INSERT INTO current_balance (user_address,token_address,current_balance)
    VALUES (?, ?, ?)
    """

    sql.write_to_custom_table(query, df)

    return

# # will read from our sql database and return the current_balance_table as a dataframe
def get_current_balance_df():
    
    column_list = ['user_address', 'token_address', 'current_balance']

    query = f"""
    SELECT *
    FROM current_balance
    """

    balance_df = sql.get_custom_query(query, column_list)

    balance_df = balance_df.drop_duplicates(subset=['user_address', 'token_address'])

    return balance_df


# # will find the raw amount of tokens that each user has
def find_all_token_balances(df, index):
    rpc_url = lph.get_lp_config_value('rpc_url', index)
    web3 = lph.get_web_3(rpc_url)

    wait_time = lph.get_lp_config_value('wait_time', index)

    wait_time = 0.2

    unique_token_list = df.token_address.unique()
    column_list = ['user_address', 'token_address', 'current_balance']
    table_name = 'current_balance'
    
    # # these lists will be populated with our wallet_token balance informations
    wallet_list = []
    token_list = []
    balance_list = []

    # # makes our balance_table if it doesn't already exist
    create_balance_table()

    i = 0

    # # iterates through each token in our database that has had a transfer
    for token_address in unique_token_list:
        token_contract = lph.get_a_token_contract(web3, token_address)

        # # makes a temporary dataframe of all values for the token address we are iterating through
        temp_token_wallet_df = df.loc[df['token_address'] == token_address]
        temp_unique_wallet_address_list = temp_token_wallet_df.to_address.unique()

        # # iterates through each wallet in the token_df that has had a transfer with that token
        for wallet_address in temp_unique_wallet_address_list:

            value_list = [wallet_address, token_address]
            temp_column_list = column_list[:2]

            values_exist = sql.sql_multiple_values_exist(value_list, temp_column_list, table_name)

            # # if our values already exist, then we will get the current balance and write to the database
            if values_exist == False:
                token_balance = lph.get_balance_of(token_contract, wallet_address)

                wallet_list.append(wallet_address)
                token_list.append(token_address)
                balance_list.append(token_balance)

                time.sleep(wait_time)

                # # will write our entry to our sql database
                temp_write_df = pd.DataFrame()
                temp_write_df['user_address'] = [wallet_address]
                temp_write_df['token_address'] = [token_address]
                temp_write_df['current_balance'] = [token_balance]

                write_to_balance_table(temp_write_df)

            print('Function Calls Remaining: ', str(len(df) - i), str(i) + '/' + str(len(df)))

            i += 1

    balance_df = get_current_balance_df()

    return balance_df


# # adds decimals and pricing and such
def add_token_metadata(index):

    df = get_current_balance_df()

    amount_column = 'current_balance'

    df[amount_column] = df[amount_column].astype(float)

    df = lph.clean_up_df_decimals(df, amount_column, index)

    df = lph.add_df_reserve_address(df, index)

    df = lph.add_df_asset_prices(df, index)

    df['amount_cumulative'] = df['current_balance'] * df['asset_price']

    return df

# # will merge our two dataframes and return our updated snapshot_df
def merge_current_balance_snapshot_df(df, snapshot_df):

    snapshot_df = snapshot_df[['user_address', 'total_embers']]
    df = df[['user_address', 'total_tvl']]

    merged_df = pd.merge(snapshot_df, df, how='left', on='user_address')

    merged_df['total_tvl'] = pd.to_numeric(merged_df['total_tvl'], errors='coerce')
    merged_df['total_embers'] = pd.to_numeric(merged_df['total_embers'], errors='coerce')

    merged_df['total_tvl'] = merged_df['total_tvl'].fillna(0)
    merged_df['total_embers'] = merged_df['total_embers'].fillna(0)
    
    merged_df.loc[merged_df['total_tvl'] < 0, 'total_tvl'] = 0
    merged_df.loc[merged_df['total_embers'] < 0, 'total_embers'] = 0

    merged_df = merged_df.drop_duplicates(subset=['user_address'])

    return merged_df


# # will update our cloud_storage bucket
def update_snapshot_bucket(df):

    # # will turn our current_balance df tvl into total_tvl per user_address
    # # also reduces columns to two and drops any duplicates
    df = lph.make_one_line_tvl(df)
    df = df[['user_address', 'total_tvl']]
    df = df.drop_duplicates(subset=['user_address'])

    # # reads existing tvl_and_embers data from the cloud
    # # and drops any duplicates just to be safe
    snapshot_df = cloud_storage.read_zip_csv_from_cloud_storage('snapshot_user_tvl_embers.csv', 'cooldowns2')
    snapshot_df = snapshot_df.drop_duplicates(subset=['user_address'])

    snapshot_df = merge_current_balance_snapshot_df(df, snapshot_df)

    cloud_storage.df_write_to_cloud_storage_as_zip(snapshot_df, 'snapshot_user_tvl_embers.csv', 'cooldowns2')

    return snapshot_df

# # our aggregate function that will do all of our looping
def loop_through_current_balances(index):
    
    df = get_user_token_combos()

    df = find_all_token_balances(df, index)

    df = add_token_metadata(0)

    df = update_snapshot_bucket(df)

    sql.drop_table('current_balance')

    print(df)