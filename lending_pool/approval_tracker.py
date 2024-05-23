from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
import time
import sys
import sqlite3
from sql_interfacer import sql
from lending_pool import lending_pool_helper as lph

# # gets our token transfer events
def get_approval_events(contract, from_block, to_block):
    
    from_block = int(from_block)
    to_block = int(to_block)
    
    events = contract.events.Approval.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # gets our the current approval amount per wallet to their respective leverager
def get_allowance(contract, wallet_address, leverager_address):

    allowance = contract.functions.allowance(wallet_address, leverager_address).call()

    return allowance

# # gets the current balance of a wallet_addresses' token
def get_balance_of(contract, wallet_address):
    
    balance_of = contract.functions.balanceOf(wallet_address).call()

    return balance_of

#makes our dataframe
def make_approval_data(events, web3, from_block, to_block, wait_time, table_name, index):
    
    df = pd.DataFrame()

    owner_list = []
    spender_list = []
    token_address_list = []
    token_amount_list = []
    log_index_list = []
    transaction_index_list = []
    tx_hash_list = []
    block_number_list = []
    
    leverager_address = '0x2dDD3BCA2Fa050532B8d7Fd41fB1449382187dAA'

    user = ''

    wait_time = wait_time/3

    start_time = time.time()

    check_exists_list = []

    # column_list, data_type_list

    db_column_list = ['owner', 'spender', 'token_address', 'value', 'log_index', 'transaction_index', 'tx_hash', 'block_number']
    db_data_type_list = ['TEXT' for x in db_column_list]

    i = 1
    for event in events:

        print('Batch of Events Processed: ', i, '/', len(events))
        # print(event)
        i+=1
        
        spender = event['args']['spender']
        time.sleep(wait_time)

        if spender == leverager_address:
            # exists_list = already_part_of_df(event, wait_time, from_block, to_block, index)
            exists_list = sql.already_part_of_database(event, wait_time, db_column_list, db_data_type_list, table_name)


            tx_hash = exists_list[0]
            log_index = exists_list[1]
            tx_index = exists_list[2]
            token_address = exists_list[3]
            exists = exists_list[4]
            token_amount = -1
            

            if exists == False:
                try:
                    block = web3.eth.get_block(event['blockNumber'])
                    block_number = int(block['number'])
                except:
                    block_number = int(event['blockNumber'])

                time.sleep(wait_time)
                if log_index < 0:
                    log_index = event['logIndex']
                
                time.sleep(wait_time)
                if tx_index < 0:
                    tx_index = event['transactionIndex']

                time.sleep(wait_time)
                if token_amount < 0:
                    token_amount = event['args']['value']
                
                if len(token_address) < 1:
                    token_address = event['address']
                
                log_index = event['logIndex']
                time.sleep(wait_time)

                spender = event['args']['spender']
                time.sleep(wait_time)
                leverager_address = '0x2dDD3BCA2Fa050532B8d7Fd41fB1449382187dAA'

                if token_amount > 0 and spender == leverager_address:
                    
                    spender_list.append(spender)
                    owner = event['args']['owner']
                    owner_list.append(owner)
                    token_address_list.append(token_address)
                    token_amount_list.append(token_amount)
                    log_index_list.append(log_index)
                    transaction_index_list.append(tx_index)
                    tx_hash_list.append(tx_hash)
                    block_number_list.append(block_number)
                    

            else:
                print('Already part of the dataframe')
                # print(event)
                time.sleep(wait_time)
                pass
    
    if len(spender_list) > 0:
        time.sleep(wait_time)

    
        df['owner'] = owner_list
        df['spender'] = spender_list
        df['token_address'] = token_address_list
        df['value'] = token_amount_list
        df['log_index'] = log_index_list
        df['transaction_index'] = transaction_index_list
        df['tx_hash'] = tx_hash_list
        df['block_number'] = block_number_list

    

    # print('User Data Event Looping done in: ', time.time() - start_time)
    return df

def get_current_approvals(column_list, index):

    rpc_url = lph.get_lp_config_value('rpc_url', index)
    web3 = lph.get_web_3(rpc_url)

    leverager_address = lph.get_lp_config_value('leverager_address', index)
    wait_time = lph.get_lp_config_value('wait_time', index)
    wait_time = wait_time / 3

    table_name = 'approvals'
    rows = sql.select_star(table_name)
    df = sql.get_sql_df(rows, column_list)
    
    wallet_address_df = df[['owner']]
    wallet_address_df = wallet_address_df.drop_duplicates(subset=['owner'])

    token_df = df[['token_address']]
    token_df = token_df.drop_duplicates(subset=['token_address'])

    token_list = token_df['token_address'].tolist()

    token_wallet_combo_df = df[['owner', 'token_address']]
    token_wallet_combo_df = token_wallet_combo_df.drop_duplicates(subset=['owner', 'token_address'])

    # # will hold our new dfs
    # # iterate through each token
    # # iterate through each user that has an a historical approval for that token
    df_list = []
    for token_address in token_list:
        token_contract = lph.get_a_token_contract(web3, token_address)

        temp_token_wallet_df = token_wallet_combo_df.loc[token_wallet_combo_df['token_address'] == token_address]
        temp_token_wallet_df = temp_token_wallet_df.drop_duplicates(subset=['owner'])
        temp_wallet_list = temp_token_wallet_df['owner'].tolist()

        for wallet in temp_wallet_list:
            temp_df = pd.DataFrame()
            wallet_list = []
            token_list = []
            approval_amount_list = []
            balance_list = []

            approval_amount = get_allowance(token_contract, wallet, leverager_address)
            time.sleep(wait_time)
            
            if approval_amount > 0:
                balance = get_balance_of(token_contract, wallet)
                time.sleep(wait_time)
                # print(approval_amount)
                wallet_list.append(wallet)
                token_list.append(token_address)
                approval_amount_list.append(approval_amount)
                balance_list.append(balance)

            temp_df['owner'] = wallet_list
            temp_df['token_address'] = token_list
            temp_df['approval_amount'] = approval_amount_list
            temp_df['current_balance'] = balance_list

            if len(temp_df) > 0:
                print(temp_df)
                df_list.append(temp_df)
    
    df = pd.concat(df_list)

    if len(df) > 0:
        df.to_csv('allowance_wallet_tokens.csv', index=False)

    return df


def get_current_balances():
    df = pd.read_csv('allowance_wallet_tokens.csv')


    return