import pandas as pd
from lending_pool import lending_pool_helper as lph
from sql_interfacer import sql
import time

# # will return a dataframe with all users and the tokens they have interacted with on Ironclad
def get_user_token_combos():
    query = """
    SELECT DISTINCT to_address, token_address
    FROM persons
"""
    column_list = ['to_address', 'token_address']
    
    df = sql.get_custom_query(query, column_list)

    return df

# # will find the raw amount of tokens that each user has
def find_all_token_balances(df, index):
    rpc_url = lph.get_lp_config_value('rpc_url', index)
    web3 = lph.get_web_3(rpc_url)

    wait_time = lph.get_lp_config_value('wait_time', index)

    wait_time = 0.1

    unique_token_list = df.token_address.unique()
    
    # # these lists will be populated with our wallet_token balance informations
    wallet_list = []
    token_list = []
    balance_list = []

    i = 0

    # # iterates through each token in our database that has had a transfer
    for token_address in unique_token_list:
        token_contract = lph.get_a_token_contract(web3, token_address)

        # # makes a temporary dataframe of all values for the token address we are iterating through
        temp_token_wallet_df = df.loc[df['token_address'] == token_address]
        temp_unique_wallet_address_list = temp_token_wallet_df.to_address.unique()

        # # iterates through each wallet in the token_df that has had a transfer with that token
        for wallet_address in temp_unique_wallet_address_list:

            token_balance = lph.get_balance_of(token_contract, wallet_address)

            wallet_list.append(wallet_address)
            token_list.append(token_address)
            balance_list.append(token_balance)

            print(wallet_address, token_address, token_balance)

            time.sleep(wait_time)

            print('Function Calls Remaining: ', str(i) + '/' + str(len(df)), str(len(df) - i))
            i += 1

    balance_df = pd.DataFrame()
    
    df['wallet_address'] = wallet_list
    df['token_address'] = token_list
    df['current_balance'] = balance_list

    return balance_df