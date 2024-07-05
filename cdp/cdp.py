from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
import time
import sys
import sqlite3
# sys.path.append("..")  # Add the root directory to the search path
from lending_pool import transaction_finder as tf
from lending_pool import balance_and_points as bp
from lending_pool import lending_pool_helper as lph
from sql_interfacer import sql
from cloud_storage import cloud_storage as cs


def insert_bulk_data_into_table(df, table_name):
    # from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number
    
    query = f"""
    INSERT INTO {table_name} (borrower_address, tx_hash, collateral_address, mint_fee, block_number, timestamp)
    VALUES (?, ?, ?, ?, ?, ?)
    """

    sql.write_to_custom_table(query, df)

    return

def insert_bulk_data_into_trove_updated_table(df, table_name):
    # from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number
    
    query = f"""
    INSERT INTO {table_name} (borrower_address, tx_hash, collateral_address, debt_balance, collateral_balance, operation, block_number, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    sql.write_to_custom_table(query, df)

    return

def create_tx_table(table_name, df):
    query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                borrower_address TEXT,
                tx_hash TEXT,
                collateral_address TEXT,
                mint_fee TEXT,
                block_number TEXT,
                timestamp TEXT
                )
            """
    
    # # will only insert data into the sql table if the table doesn't exist

    sql.create_custom_table(query)

    table_length = sql.select_star_count(table_name)[0][0]
    
    # # we will drop our table and insert the data from the cloud of our local database has less entries than the cloud
    if table_length < len(df):
        sql.drop_table(table_name)
        sql.create_custom_table(query)
        insert_bulk_data_into_table(df, table_name)

    return

def create_trove_updated_table(table_name, df):
    query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                borrower_address TEXT,
                tx_hash TEXT,
                collateral_address TEXT,
                debt_balance TEXT,
                collateral_balance TEXT,
                operation TEXT,
                block_number TEXT,
                timestamp TEXT
                )
            """
    
    # # will only insert data into the sql table if the table doesn't exist

    sql.create_custom_table(query)

    table_length = sql.select_star_count(table_name)[0][0]
    
    # # we will drop our table and insert the data from the cloud of our local database has less entries than the cloud
    if table_length < len(df):
        sql.drop_table(table_name)
        sql.create_custom_table(query)
        insert_bulk_data_into_table(df, table_name)

    return

def create_trove_updated_table(table_name, df):
    query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                borrower_address TEXT,
                tx_hash TEXT,
                collateral_address TEXT,
                debt_balance TEXT,
                collateral_balance TEXT,
                operation TEXT,
                block_number TEXT,
                timestamp TEXT
                )
            """
    
    # # will only insert data into the sql table if the table doesn't exist

    sql.create_custom_table(query)

    table_length = sql.select_star_count(table_name)[0][0]
    
    # # we will drop our table and insert the data from the cloud of our local database has less entries than the cloud
    if table_length < len(df):
        sql.drop_table(table_name)
        sql.create_custom_table(query)
        insert_bulk_data_into_trove_updated_table(df, table_name)

    return


#makes our dataframe
def user_data(events, web3, index):
    
    df = pd.DataFrame()

    # column_list = ['borrower_address', 'tx_hash', 'collateral_address', 'mint_fee', 'block_number']

    borrower_address_list = []
    tx_hash_list = []
    collateral_address_list = []
    mint_fee_list = []
    block_list = []
    timestamp_list = []

    # # inputs to our sql function
    column_list = ['borrower_address', 'tx_hash', 'collateral_address', 'mint_fee', 'block_number', 'timestamp']
    data_type_list = ['TEXT' for x in column_list]
    table_name = lph.get_cdp_config_value('table_name', index)

    # reduces wait time by 50%
    wait_time = lph.get_cdp_config_value('wait_time', index)
    wait_time = wait_time/3

    i = 1
    for event in events:

        # print('Batch of Events Processed: ', i, '/', len(events))
        i+=1
            
        # exists_list = already_part_of_df(event, wait_time, from_block, to_block, index)
        exists_list = sql.cdp_fee_already_part_of_database(event, wait_time, column_list, table_name)

        tx_hash = exists_list[0]
        borrower_address = exists_list[1]
        mint_fee = exists_list[2]
        collateral_address = exists_list[3]
        exists = exists_list[4]

        if exists == False:
            try:
                block = web3.eth.get_block(event['blockNumber'])
                block_number = int(block['number'])
            except:
                block_number = int(event['blockNumber'])

            time.sleep(wait_time)
            if mint_fee < 0:
                mint_fee = event['args']['_LUSDFee']
                mint_fee /= 1e18
            
            time.sleep(wait_time)
            if len(borrower_address) < 1:
                borrower_address = event['args']['_borrower']

            time.sleep(wait_time)
            if len(collateral_address) < 1:
                collateral_address = event['args']['_collateral']

            if mint_fee > 0:

                # borrower_address_list = []
                # tx_hash_list = []
                # collateral_address_list = []
                # mint_fee_list = []
                # block_list = []
                # timestamp_list = []

                block_list.append(block_number)
                borrower_address_list.append(borrower_address)
                collateral_address_list.append(collateral_address)
                tx_hash_list.append(tx_hash)
                timestamp_list.append(block['timestamp'])
                # token_address = event['address']
                mint_fee_list.append(mint_fee)

        else:
            # print('Already part of the dataframe')
            # print(event)
            time.sleep(wait_time)
            pass
    
    if len(borrower_address_list) > 0:
        time.sleep(wait_time)

    
        df['borrower_address'] = borrower_address_list
        df['tx_hash'] = tx_hash_list
        df['collateral_address'] = collateral_address_list
        df['mint_fee'] = mint_fee_list
        df['block_number'] = block_list
        df['timestamp'] = timestamp_list
    

    # print('User Data Event Looping done in: ', time.time() - start_time)
    return df

#makes our dataframe
def get_trove_updated_data(events, web3, index):
    
    df = pd.DataFrame()

    # column_list = ['borrower_address', 'tx_hash', 'collateral_address', 'mint_fee', 'block_number']

    borrower_address_list = []
    tx_hash_list = []
    collateral_address_list = []
    debt_balance_list = []
    collateral_balance_list = []
    operation_list = []

    block_list = []
    timestamp_list = []

    # # inputs to our sql function
    column_list = ['borrower_address', 'tx_hash', 'collateral_address', 'debt_balance', 'collateral_balance', 'operation', 'block_number', 'timestamp']
    data_type_list = ['TEXT' for x in column_list]
    table_name = lph.get_cdp_config_value('table_name', index)

    # reduces wait time by 50%
    wait_time = lph.get_cdp_config_value('wait_time', index)
    wait_time = wait_time/3

    i = 1
    for event in events:

        # print('Batch of Events Processed: ', i, '/', len(events))
        i+=1
            
        # exists_list = already_part_of_df(event, wait_time, from_block, to_block, index)
        exists_list = sql.cdp_trove_update_already_part_of_database(event, wait_time, column_list, table_name)

        tx_hash = exists_list[0]
        borrower_address = exists_list[1]
        collateral_address = exists_list[2]
        collateral_balance = exists_list[3]
        debt_balance = exists_list[4]
        exists = exists_list[5]

        if exists == False:
            try:
                block = web3.eth.get_block(event['blockNumber'])
                block_number = int(block['number'])
            except:
                block_number = int(event['blockNumber'])

            time.sleep(wait_time)
            if collateral_balance < 0:
                collateral_balance = event['args']['_coll']
            
            time.sleep(wait_time)
            if debt_balance < 0:
                debt_balance = event['args']['_debt']
            
            time.sleep(wait_time)
            if len(borrower_address) < 1:
                borrower_address = event['args']['_borrower']

            time.sleep(wait_time)
            if len(collateral_address) < 1:
                collateral_address = event['args']['_collateral']

            block_list.append(block_number)
            borrower_address_list.append(borrower_address)
            collateral_address_list.append(collateral_address)
            collateral_balance_list.append(collateral_balance)
            debt_balance_list.append(debt_balance)
            operation = event['args']['operation']
            time.sleep(wait_time)
            operation_list.append(operation)
            tx_hash_list.append(tx_hash)
            timestamp_list.append(block['timestamp'])

        else:
            # print('Already part of the dataframe')
            # print(event)
            time.sleep(wait_time)
            pass
    
    if len(borrower_address_list) > 0:
        time.sleep(wait_time)
    
        df['borrower_address'] = borrower_address_list
        df['tx_hash'] = tx_hash_list
        df['collateral_address'] = collateral_address_list
        df['debt_balance'] = debt_balance_list
        df['collateral_balance'] = collateral_balance_list
        df['operation'] = operation_list
        df['block_number'] = block_list
        df['timestamp'] = timestamp_list
    

    # print('User Data Event Looping done in: ', time.time() - start_time)
    return df

# # runs all our looks
# # updates our csv
def find_all_mint_fee_transactions(index):

    config_df = lph.get_cdp_config_df()
    config_df = config_df.loc[config_df['index'] == index]

    rpc_url = lph.get_cdp_config_value('rpc_url', index)
    
    web3 = tf.get_web_3(rpc_url)

    from_block = lph.get_cdp_config_value('last_block', index)

    latest_block = lph.get_latest_block(web3) 

    interval = lph.get_cdp_config_value('interval', index)

    wait_time = lph.get_cdp_config_value('wait_time', index)
    from_block -= interval

    to_block = from_block + interval

    borrower_operations_address = lph.get_cdp_config_value('borrower_operations_address', index)

    borrower_operations_contract = lph.get_borrower_operations_contract(web3, borrower_operations_address)

    # # reads our last data from our treasury to ensure we don't lose info do to the vm reverting
    cloud_csv_name = lph.get_cdp_config_value('cloud_filename', index)
    cloud_bucket_name = lph.get_cdp_config_value('cloud_bucket_name', index)
    tx_df = cs.read_zip_csv_from_cloud_storage(cloud_csv_name, cloud_bucket_name)
    
    # # drops any stray duplicates
    tx_df.drop_duplicates(subset=['borrower_address', 'tx_hash', 'collateral_address', 'mint_fee'])

    # # inputs to our sql function
    column_list = ['borrower_address', 'tx_hash', 'collateral_address', 'mint_fee', 'block_number', 'timestamp']

    table_name = lph.get_cdp_config_value('table_name', index)

    # # will create our table and only insert data into it from our cloud bucket if the table doesn't exist
    create_tx_table(table_name, tx_df)

    while to_block < latest_block:
        # print(receipt_token_address, ': Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)

        events = lph.get_mint_fee_events(borrower_operations_contract, from_block, to_block)
        
        if len(events) > 0:
            contract_df = user_data(events, web3, index)
            if len(contract_df) > 0:
                sql.write_to_db(contract_df, column_list, table_name)
                # sql.drop_duplicates_from_database(cursor)
                # make_user_data_csv(contract_df, index)
        else:
            time.sleep(wait_time)

        # # will make sure not overwrite other chains' data in the config file
        temp_config_df = lph.get_cdp_config_df()
        temp_config_df.loc[(temp_config_df['index'] == index) and (temp_config_df['tx_type'] == 0), 'last_block'] = to_block
        # config_df['last_block'] = from_block
        temp_config_df.to_csv('./config/cdp_config.csv', index=False)

        from_block += interval
        to_block += interval

        # print(deposit_events)


        if from_block >= latest_block:
            from_block = latest_block - 1
        
        if to_block >= latest_block:
            to_block = latest_block
        
        print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)
    
    contract_df = sql.get_transaction_data_df(table_name, column_list)
    cs.df_write_to_cloud_storage_as_zip(contract_df, cloud_csv_name, cloud_bucket_name)
    
    # bp.set_embers_database(index)

    return

# # runs all our looks
# # updates our csv
def find_all_trove_updated_transactions(index):

    config_df = lph.get_cdp_config_df()
    config_df = config_df.loc[config_df['index'] == index]

    rpc_url = lph.get_cdp_config_value('rpc_url', index)
    
    web3 = tf.get_web_3(rpc_url)

    from_block = lph.get_cdp_config_value('last_block', index)

    latest_block = lph.get_latest_block(web3) 

    interval = lph.get_cdp_config_value('interval', index)

    wait_time = lph.get_cdp_config_value('wait_time', index)
    from_block -= interval

    to_block = from_block + interval

    borrower_operations_address = lph.get_cdp_config_value('borrower_operations_address', index)

    borrower_operations_contract = lph.get_borrower_operations_contract(web3, borrower_operations_address)

    # # reads our last data from our treasury to ensure we don't lose info do to the vm reverting
    cloud_csv_name = lph.get_cdp_config_value('cloud_filename', index)
    cloud_bucket_name = lph.get_cdp_config_value('cloud_bucket_name', index)
    tx_df = cs.read_zip_csv_from_cloud_storage(cloud_csv_name, cloud_bucket_name)
    
    # # drops any stray duplicates
    tx_df.drop_duplicates(subset=['borrower_address', 'tx_hash', 'collateral_address', 'debt_balance', 'collateral_balance', 'operation'])

    # # inputs to our sql function
    column_list = ['borrower_address', 'tx_hash', 'collateral_address', 'debt_balance', 'collateral_balance', 'operation', 'block_number', 'timestamp']

    table_name = lph.get_cdp_config_value('table_name', index)

    # # will create our table and only insert data into it from our cloud bucket if the table doesn't exist
    create_trove_updated_table(table_name, tx_df)

    while to_block < latest_block:
        # print(receipt_token_address, ': Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)

        events = lph.get_trove_updated_events(borrower_operations_contract, from_block, to_block)
        
        if len(events) > 0:
            contract_df = get_trove_updated_data(events, web3, index)
            if len(contract_df) > 0:
                sql.write_to_db(contract_df, column_list, table_name)
                # sql.drop_duplicates_from_database(cursor)
                # make_user_data_csv(contract_df, index)
        else:
            time.sleep(wait_time)

        # # will make sure not overwrite other chains' data in the config file
        temp_config_df = lph.get_cdp_config_df()
        temp_config_df.loc[temp_config_df['index'] == index, 'last_block'] = to_block
        # config_df['last_block'] = from_block
        temp_config_df.to_csv('./config/cdp_config.csv', index=False)

        from_block += interval
        to_block += interval

        if from_block >= latest_block:
            from_block = latest_block - 1
        
        if to_block >= latest_block:
            to_block = latest_block
        
        print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)
    
    contract_df = sql.get_transaction_data_df(table_name)
    cs.df_write_to_cloud_storage_as_zip(contract_df, cloud_csv_name, cloud_bucket_name)
    
    # bp.set_embers_database(index)

    return