import sys
import os
import time
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import lending_pool_helper as lph
from helper_classes import ERC_20, Protocol_Data_Provider
from cloud_storage import cloud_storage as cs
from sql_interfacer import sql

class Lending_Pool(ERC_20.ERC_20, Protocol_Data_Provider.Protocol_Data_Provider):

    def __init__(self, protocol_data_provider_address: str, rpc_url: str, wait_time: float, interval: float, index: str):
        self.protocol_data_provider_address = protocol_data_provider_address
        self.rpc_url = rpc_url
        self.wait_time = wait_time
        self.interval = interval
        self.index = index
        self.web3 = lph.get_web_3(self.rpc_url)
        self.cloud_file_name = index + '_events.zip'
        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.cloud_file_name

    # # will load our cloud df information into the sql database
    def insert_bulk_data_into_table(df, table_name):
        # from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number
        
        query = f"""
        INSERT INTO {table_name} (from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,block_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        sql.write_to_custom_table(query, df)

        return

    def create_lend_table(self, cloud_df):
        
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}(
                from_address TEXT,
                to_address TEXT,
                tx_hash TEXT,
                timestamp TEXT,
                token_address TEXT,
                reserve_address TEXT,
                token_volume TEXT,
                asset_price TEXT,
                usd_token_amount TEXT,
                block_number TEXT
                )
            """
        
        # # will only insert data into the sql table if the table doesn't exist
        sql.create_custom_table(query)
        
        db_df = sql.get_transaction_data_df(self.table_name)

        # # makes a combined local and cloud dataframe and drops any duplicates from the dataframe
        # # drops our old database table
        # # then writes the updated sanitized dataframe to our local database table
        duplicate_column_list = ['tx_hash', 'to_address', 'from_address', 'token_address', 'token_volume']
        sanitized_df = lph.sanitize_database_and_cloud_df(db_df, cloud_df, duplicate_column_list)

        sql.drop_table(self.table_name)
        sql.create_custom_table(query)
        self.insert_bulk_data_into_table(sanitized_df, self.table_name)

        return
    
    # # will return a boolean of whether our event already exists
    def lend_event_already_exists(self, event):
        
        column_list = ['tx_hash', 'to_address', 'from_address', 'token_address', 'token_volume']

        tx_hash = event['transactionHash'].hex()
        to_address = event['args']['to']
        from_address = event['args']['from']
        token_address = event['address']
        token_volume = event['args']['value']

        value_list = [tx_hash, to_address, from_address, token_address, token_volume]

        exists = sql.sql_multiple_values_exist(column_list, value_list, self.table_name)

        return exists
    
    # # will process our events
    def process_events(self, events):

        df = pd.DataFrame()

        to_address_list = []
        from_address_list = []
        tx_hash_list = []
        timestamp_list = []
        token_address_list = []
        reserve_address_list = []
        token_volume_list = []
        log_index_list = []
        tx_index_list = []
        block_list = []
        
        column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','block_number']

        i = 1

        for event in events:
            i += 1

            lend_event_exists = sql.already_part_of_database(event, column_list, self.table_name)

            if lend_event_exists == True:
                
                from_address = event['args']['from']
                to_address = event['args']['to']
                tx_hash = event['transactionHash'].hex()
                token_address = event['address']
                reserve_address = ''
                token_volume = event['args']['value']
                asset_price = ''
                usd_token_amount = ''

                
                try:
                    block = self.web3.eth.get_block(event['blockNumber'])
                    block_number = int(block['number'])
                except:
                    block_number = int(event['blockNumber'])
                
                block_timestamp = block['timestamp']
                


        return
    # # will run all of the lending pool transfer event tracking
    def run_all(self):
        
        from_block = lph.get_from_block(self.index)
        latest_block = lph.get_latest_block(self.web3) 
        interval = self.interval
        wait_time = self.wait_time

        to_block = from_block + interval

        receipt_token_list = self.get_non_stable_receipt_token_list()

        receipt_contract_list = [ERC_20.ERC_20(receipt_address, self.rpc_url) for receipt_address in receipt_token_list]

        cloud_df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)

        cloud_df.drop_duplicates(subset=['tx_hash', 'token_address', 'token_volume', 'from_address', 'to_address'])

        self.create_lend_table(cloud_df)
        
        while to_block < latest_block:
            i = 0

        while i < len(receipt_token_list):

            receipt_contract = receipt_contract_list[i]

            events = self.get_transfer_events(receipt_contract, from_block, to_block)

            i += 1

            if len(events) > 0:
                contract_df = self.process_events(events, self.web3, self.index)

        return