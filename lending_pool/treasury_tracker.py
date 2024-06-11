from lending_pool import lending_pool_helper as lph
from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
import time
import sys
import sqlite3
# sys.path.append("..")  # Add the root directory to the search path
from lending_pool import transaction_finder as tf
from sql_interfacer import sql
from lending_pool import balance_and_points as bp
from cloud_storage import cloud_storage as cs

connection = sqlite3.connect("turtle.db")

cursor = connection.cursor()

# # will loop through our events and make the relevant output df
def get_revenue_data(events, web3, index):
    
    df = pd.DataFrame()

    to_address_list = []
    from_address_list = []
    tx_hash_list = []
    timestamp_list = []
    token_address_list = []
    reserve_address_list = []
    token_volume_list = []
    asset_price_list = []
    token_usd_amount_list = []
    log_index_list = []
    tx_index_list = []
    block_list = []

    user = ''

    treasury_address = lph.get_lp_config_value('treasury_address', index)

    # # inputs to our sql function
    column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number']
    data_type_list = ['TEXT' for x in column_list]
    table_name = lph.get_lp_config_value('table_name', index)

    # reduces wait time by 50%
    wait_time = lph.get_lp_config_value('wait_time', index)
    wait_time = wait_time/3

    start_time = time.time()
    i = 1
    for event in events:

        # print('Batch of Events Processed: ', i, '/', len(events))
        i+=1

        to_address = event['args']['to']
        time.sleep(wait_time)

        # # will only add data to the dataframe if tokens are going to the treasury
        if to_address == treasury_address:
            exists_list = sql.already_part_of_database(event, wait_time, column_list, data_type_list, table_name)

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

                if token_amount > 0:
                    
                    block_list.append(block_number)
                    from_address = event['args']['from']
                    time.sleep(wait_time)
                    from_address_list.append(from_address)
                    to_address_list.append(to_address)
                    tx_hash_list.append(tx_hash)
                    timestamp_list.append(block['timestamp'])
                    time.sleep(wait_time)
                    # token_address = event['address']
                    token_address_list.append(token_address)
                    reserve_address = lph.get_token_config_value('underlying_address', token_address, index)
                    reserve_address_list.append(reserve_address)
                    token_volume_list.append(token_amount)
                    log_index_list.append(log_index)
                    tx_index_list.append(tx_index)
                else:
                    # print('Already part of the dataframe')
                    # print(event)
                    time.sleep(wait_time)
                    pass
    
    if len(from_address_list) > 0:
        time.sleep(wait_time)

    
        df['from_address'] = from_address_list
        df['to_address'] = to_address_list
        df['tx_hash'] = tx_hash_list
        df['timestamp'] = timestamp_list
        df['token_address'] = token_address_list
        df['reserve_address'] = reserve_address_list
        df['token_volume'] = token_volume_list
        df = lph.update_batch_pricing(df, web3, index)
        df['log_index'] = log_index_list
        df['transaction_index'] = tx_index_list
        df['block_number'] = block_list
    

    # print('User Data Event Looping done in: ', time.time() - start_time)
    return df        

# # creates our balance table if it doesn't already exist
def create_treasury_table(table_name, df):

    query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                from_address TEXT,
                to_address TEXT,
                tx_hash TEXT,
                timestamp TEXT,
                token_address TEXT,
                reserve_address TEXT,
                token_volume TEXT,
                asset_price TEXT,
                usd_token_amount TEXT,
                log_index TEXT,
                transaction_index TEXT,
                block_number TEXT
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

# # will load our cloud df information into the sql database
def insert_bulk_data_into_table(df, table_name):
    # from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number
    
    query = f"""
    INSERT INTO {table_name} (from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    sql.write_to_custom_table(query, df)

    return

# # will find all revenue transactions
def find_all_revenue_transactions(index):

    config_df = lph.get_lp_config_df()
    config_df = config_df.loc[config_df['index'] == index]

    rpc_url = lph.get_lp_config_value('rpc_url', index)

    web3 = tf.get_web_3(rpc_url)

    from_block = lph.get_from_block(index)

    latest_block = lph.get_latest_block(web3)
    
    token_config_df = lph.get_token_config_df()

    interval = lph.get_lp_config_value('interval', index)

    wait_time = lph.get_lp_config_value('wait_time', index)

    to_block = from_block + interval

    token_config_df = lph.get_token_config_df()
    
    token_config_df = token_config_df.loc[token_config_df['chain_index'] == index]

    column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number']

    table_name = lph.get_lp_config_value('table_name', index)

    a_token_list = lph.get_a_token_list(index)

    # # reads our last data from our treasury to ensure we don't lose info do to the vm reverting
    cloud_csv_name = lph.get_lp_config_value('treasury_filename', index)
    cloud_bucket_name = lph.get_lp_config_value('treasury_bucket_name', index)
    last_treasury_df = cs.read_from_cloud_storage(cloud_csv_name, cloud_bucket_name)
    # # drops any stray duplicates
    last_treasury_df.drop_duplicates(subset=['from_address', 'to_address', 'tx_hash', 'log_index', 'transaction_index'])

    # # will create our table and only insert data into it from our cloud bucket if the table doesn't exist
    create_treasury_table(table_name, last_treasury_df)

    while to_block < latest_block:

        receipt_counter = 0

        while receipt_counter < len(a_token_list):
            receipt_token_address = a_token_list[receipt_counter]

            receipt_contract = lph.get_a_token_contract(web3, receipt_token_address)

            # print(receipt_token_address, ': Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)

            events = lph.get_transfer_events(receipt_contract, from_block, to_block)

            receipt_counter += 1

            if len(events) > 0:
                contract_df = get_revenue_data(events, web3, index)

                if len(contract_df) > 0:
                    time.sleep(wait_time)
                    sql.write_to_db(contract_df, column_list, table_name)

            else:
                time.sleep(wait_time)
        
        config_df.loc[config_df['index'] == index, 'last_block'] = from_block
        # config_df['last_block'] = from_block
        config_df.to_csv('./config/lp_config.csv', index=False)

        from_block += interval
        to_block += interval

        # print(deposit_events)


        if from_block >= latest_block:
            from_block = latest_block - 1
        
        if to_block >= latest_block:
            to_block = latest_block    

        print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)
    
    contract_df = sql.get_transaction_data_df(table_name)
    
    if len(contract_df) > 0:
        cs.df_write_to_cloud_storage(contract_df, cloud_csv_name, cloud_bucket_name)

        try:
            sql.drop_table(table_name)
        except:
            print(table_name, ' failed to drop')

    return

 # will find all revenue transactions
def find_all_user_transactions(index):

    config_df = lph.get_lp_config_df()
    config_df = config_df.loc[config_df['index'] == index]

    rpc_url = lph.get_lp_config_value('rpc_url', index)

    web3 = tf.get_web_3(rpc_url)

    from_block = lph.get_from_block(index)

    latest_block = lph.get_latest_block(web3)
    
    token_config_df = lph.get_token_config_df()

    interval = lph.get_lp_config_value('interval', index)

    wait_time = lph.get_lp_config_value('wait_time', index)

    to_block = from_block + interval

    token_config_df = lph.get_token_config_df()
    
    token_config_df = token_config_df.loc[token_config_df['chain_index'] == index]

    column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number']

    # table_name = lph.get_lp_config_value('table_name', index)

    table_name = 'mersons'

    receipt_token_list = token_config_df['token_address'].tolist()

    # # reads our last data from our treasury to ensure we don't lose info do to the vm reverting
    cloud_csv_name = lph.get_lp_config_value('treasury_filename', index)
    cloud_bucket_name = lph.get_lp_config_value('treasury_bucket_name', index)
    last_treasury_df = cs.read_from_cloud_storage(cloud_csv_name, cloud_bucket_name)
    # # drops any stray duplicates
    last_treasury_df.drop_duplicates(subset=['from_address', 'to_address', 'tx_hash', 'log_index', 'transaction_index'])

    # # will create our table and only insert data into it from our cloud bucket if the table doesn't exist
    create_treasury_table(table_name, last_treasury_df)

    user_list = ['0x7D56e162A044A6B327332D3e6Ce4F68470440373']

    while to_block < latest_block:

        receipt_counter = 0

        while receipt_counter < len(receipt_token_list):
            receipt_token_address = receipt_token_list[receipt_counter]

            receipt_contract = lph.get_a_token_contract(web3, receipt_token_address)

            # print(receipt_token_address, ': Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)

            events = lph.get_transfer_events(receipt_contract, from_block, to_block)

            receipt_counter += 1

            if len(events) > 0:
                contract_df = lph.get_specified_users_transactions(user_list, events, web3, index)

                if len(contract_df) > 0:
                    time.sleep(wait_time)
                    sql.write_to_db(contract_df, column_list, table_name)

            else:
                time.sleep(wait_time)
        
        config_df.loc[config_df['index'] == index, 'last_block'] = from_block
        # config_df['last_block'] = from_block
        config_df.to_csv('./config/lp_config.csv', index=False)

        from_block += interval
        to_block += interval

        # print(deposit_events)


        if from_block >= latest_block:
            from_block = latest_block - 1
        
        if to_block >= latest_block:
            to_block = latest_block    

        print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)
    
    contract_df = sql.get_transaction_data_df(table_name)
    
    # if len(contract_df) > 0:
    #     cs.df_write_to_cloud_storage(contract_df, cloud_csv_name, cloud_bucket_name)

    #     try:
    #         sql.drop_table(table_name)
    #     except:
    #         print(table_name, ' failed to drop')

    return

# # returns a tx_hash list of transactions that classify as revenue transactions
def set_unique_revenue_tx_list(df, index):
    
    treasury_address = lph.get_lp_config_value('treasury_address', index)

    a_token_list = lph.get_a_token_list(index)

    revenue_df = df.loc[df['token_address'].isin(a_token_list)]
    revenue_df = df.loc[df['to_address'] == treasury_address]

    revenue_df = revenue_df.drop_duplicates(subset=['tx_hash'])

    tx_hash_list = revenue_df['tx_hash'].tolist()

    return tx_hash_list

# # will make revenue specific data *
def set_revenue_data(index):

    null_address = '0x0000000000000000000000000000000000000000'

    table_name = lph.get_lp_config_value('table_name', index)
    treasury_filename = lph.get_lp_config_value('treasury_filename', index)
    treasury_bucket_name = lph.get_lp_config_value('treasury_bucket_name', index)

    df = sql.get_transaction_data_df(table_name)
    revenue_df = cs.read_from_cloud_storage(treasury_filename, treasury_bucket_name)
    
    # tx_hash_list = set_unique_revenue_tx_list(df, index)

    merged_df = pd.merge(df, revenue_df, how='inner')

    merged_df = merged_df.loc[df['from_address'] != null_address]

    return merged_df

def find_revenue_user_tx_data(index):

    treasury_filename = lph.get_lp_config_value('treasury_filename', index)
    treasury_bucket_name = lph.get_lp_config_value('treasury_bucket_name', index)

    revenue_df = cs.read_from_cloud_storage(treasury_filename, treasury_bucket_name)

    revenue_df = revenue_df.drop_duplicates(subset=['block_number'])

    block_number_list = revenue_df['block_number'].tolist()

    
    return