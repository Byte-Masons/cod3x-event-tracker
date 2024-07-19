import sys
import os
import time
import pandas as pd
import sqlite3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import lending_pool_helper as lph
from helper_classes import ERC_20, Protocol_Data_Provider, Sanitize
from cloud_storage import cloud_storage as cs
from sql_interfacer import sql
from revenue_tracking import Transaction_Labeler as tl

class Lending_Pool(ERC_20.ERC_20, Protocol_Data_Provider.Protocol_Data_Provider):

    def __init__(self, protocol_data_provider_address: str, gateway_address: str, treasury_address: str, rpc_url: str, wait_time: float, interval: float, index: str):
        self.protocol_data_provider_address = protocol_data_provider_address
        self.gateway_address = gateway_address
        self.treasury_address = treasury_address
        self.rpc_url = rpc_url
        self.wait_time = wait_time
        self.interval = interval
        self.index = index
        self.web3 = lph.get_web_3(self.rpc_url)
        self.cloud_file_name = index + '.zip'
        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.index

    # # will load our cloud df information into the sql database
    def insert_bulk_data_into_table(self, df, table_name):
        # from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number
        
        query = f"""
        INSERT INTO {table_name} (from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,block_number,event_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                block_number TEXT,
                event_type TEXT
                )
            """

        # # will only insert data into the sql table if the table doesn't exist
        sql.create_custom_table(query)
        
        db_df = sql.get_transaction_data_df(self.table_name)

        # # makes a combined local and cloud dataframe and drops any duplicates from the dataframe
        # # drops our old database table
        # # then writes the updated sanitized dataframe to our local database table
        duplicate_column_list = ['tx_hash', 'to_address', 'from_address', 'token_address', 'token_volume']

        combined_df = pd.concat([db_df, cloud_df])
        combined_df = combined_df.drop_duplicates(subset=duplicate_column_list)

        sanitized_df = combined_df[['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','block_number','event_type']]

        # # extra decimal sanitation
        sanitizer = Sanitize.Sanitize(sanitized_df, self.rpc_url)
        sanitized_df = sanitizer.sanitize_df()

        sql.drop_table(self.table_name)
        sql.create_custom_table(query)
        self.insert_bulk_data_into_table(sanitized_df, self.table_name)

        return
    
    # # will return a boolean of whether our event already exists
    def lend_event_already_exists(self, event):
        
        column_list = ['tx_hash', 'to_address', 'from_address', 'token_address', 'token_volume']

        tx_hash = str(event['transactionHash'].hex())
        to_address = event['args']['to']
        from_address = event['args']['from']
        token_address = event['address']
        token_volume = event['args']['value']

        value_list = [tx_hash, to_address, from_address, token_address, token_volume]

        exists = sql.sql_multiple_values_exist(value_list, column_list, self.table_name)

        return exists
    

    # # will return our batch df with appropriate asset_price and usd_token_amount
    def update_batch_pricing(self, batch_df, reserve_df) -> pd.DataFrame:
        # 'asset_price','usd_token_amount'

        unique_underlying_list = batch_df['reserve_address'].unique()

        batch_pricing_list = self.get_oracle_price(unique_underlying_list)

        i = 0
        for unique_underlying in unique_underlying_list:
            temp_reserve_df = reserve_df.loc[reserve_df['reserve_address'] == unique_underlying]
            temp_decimals = temp_reserve_df['decimals'].tolist()[0]
            
            batch_df.loc[batch_df['reserve_address'] == unique_underlying, 'decimals'] = temp_decimals
            temp_price = batch_pricing_list[i]

            batch_df.loc[batch_df['reserve_address'] == unique_underlying, 'asset_price'] = temp_price

            i += 1
            
        batch_df['usd_token_amount'] = batch_df['token_volume'] / batch_df['decimals'] * batch_df['asset_price']

        # print(batch_df.dtypes)
        # print(batch_df)

        # print(batch_df[['tx_hash', 'reserve_address', 'token_volume', 'asset_price', 'usd_token_amount']])
        
        batch_df = batch_df[['from_address', 'to_address', 'tx_hash', 'timestamp', 'token_address', 'reserve_address', 'token_volume', 'asset_price', 'usd_token_amount']]

        return batch_df

    # # will process our events
    def process_events(self, events, token_reserve_df):

        df = pd.DataFrame()

        to_address_list = []
        from_address_list = []
        tx_hash_list = []
        timestamp_list = []
        token_address_list = []
        reserve_address_list = []
        token_volume_list = []
        block_list = []
        
        column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','block_number']

        i = 1

        for event in events:
            i += 1

            lend_event_exists = self.lend_event_already_exists(event)

            if lend_event_exists == False:
                
                from_address = event['args']['from']
                to_address = event['args']['to']
                tx_hash = event['transactionHash'].hex()
                token_address = event['address']
                reserve_address = token_reserve_df.loc[token_reserve_df['token_address'] == token_address]
                reserve_address = reserve_address['reserve_address'].tolist()[0]
                token_volume = event['args']['value']
                
                try:
                    block = self.web3.eth.get_block(event['blockNumber'])
                    block_number = int(block['number'])
                except:
                    block_number = int(event['blockNumber'])
                
                block_timestamp = block['timestamp']
                
                block_list.append(block_number)
                from_address_list.append(from_address)
                to_address_list.append(to_address)
                tx_hash_list.append(tx_hash)
                timestamp_list.append(block_timestamp)
                token_address_list.append(token_address)
                reserve_address_list.append(reserve_address)
                token_volume_list.append(token_volume)

            time.sleep(self.wait_time)
        
        if len(from_address_list) > 0:

    
            df['from_address'] = from_address_list
            df['to_address'] = to_address_list
            df['tx_hash'] = tx_hash_list
            df['timestamp'] = timestamp_list
            df['token_address'] = token_address_list
            df['reserve_address'] = reserve_address_list
            df['token_volume'] = token_volume_list
            df = self.update_batch_pricing(df, token_reserve_df)
            df['block_number'] = block_list

        return df
    
    # # will run all of the lending pool transfer event tracking
    def run_all_lend_event_tracking(self):
        
        labeler = tl.Transaction_Labeler(self.protocol_data_provider_address, self.rpc_url, self.index, self.gateway_address, self.treasury_address)

        from_block = lph.get_from_block(self.index, self.interval)

        latest_block = lph.get_latest_block(self.web3) 
        interval = self.interval
        wait_time = self.wait_time

        to_block = from_block + interval

        receipt_token_list = self.get_non_stable_receipt_token_list()

        receipt_contract_list = []
        for token_address in receipt_token_list:
            token_contract = self.get_token_contract(token_address)
            receipt_contract_list.append(token_contract)

            time.sleep(wait_time)

        cloud_df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)

        cloud_df.drop_duplicates(subset=['tx_hash', 'token_address', 'token_volume', 'from_address', 'to_address'])

        # # inputs to our sql function
        column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','block_number', 'event_type']

        self.create_lend_table(cloud_df)
        
        token_reserve_df = self.get_token_and_reserve_df(receipt_contract_list, receipt_token_list)

        while to_block < latest_block:
            i = 0

            while i < len(receipt_token_list):

                receipt_contract = receipt_contract_list[i]

                events = self.get_transfer_events(receipt_contract, from_block, to_block)

                if len(events) > 0:
                    contract_df = self.process_events(events, token_reserve_df)

                    if len(contract_df) > 0:
                        contract_df = labeler.label_events(contract_df)

                        sql.write_to_db(contract_df, column_list, self.table_name)
                
                time.sleep(wait_time)
                
                i += 1

            # # will make sure not overwrite other chains' data in the config file
            temp_config_df = lph.get_lp_config_df()
            temp_config_df.loc[temp_config_df['index'] == self.index, 'last_block'] = from_block
            # config_df['last_block'] = from_block
            temp_config_df.to_csv('./config/lp_config.csv', index=False)

            from_block += interval
            to_block += interval

            # print(deposit_events)


            if from_block >= latest_block:
                from_block = latest_block - 1
            
            if to_block >= latest_block:
                to_block = latest_block
            
            print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)
            time.sleep(wait_time)
    
        contract_df = sql.get_transaction_data_df(self.table_name)
        
        # # extra dataframe sanitation
        sanitizer = Sanitize.Sanitize(contract_df, self.rpc_url)
        contract_df = sanitizer.sanitize_df()

        cs.df_write_to_cloud_storage_as_zip(contract_df, self.cloud_file_name, self.cloud_bucket_name)

        return