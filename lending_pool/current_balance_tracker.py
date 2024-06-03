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


# # will find the raw amount of tokens that each user has
def find_all_token_balances(df, index):
    rpc_url = lph.get_lp_config_value('rpc_url', index)
    web3 = lph.get_web_3(rpc_url)

    wait_time = lph.get_lp_config_value('wait_time', index)

    wait_time = 0.1

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


    balance_df = pd.DataFrame()
    
    df['user_address'] = wallet_list
    df['token_address'] = token_list
    df['current_balance'] = balance_list

    # # will delete our current_balance_table to save on memory
    # try:
    #     sql.drop_table('current_balance')
    # except:
    #     print('Current Balance table does not already exist')
    

    return balance_df