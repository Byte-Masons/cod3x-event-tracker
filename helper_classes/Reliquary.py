from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
import time
import sys
import os
import sqlite3
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import transaction_finder as tf
from lending_pool import balance_and_points as bp
from lending_pool import lending_pool_helper as lph
from helper_classes import ERC_20
from sql_interfacer import sql
from cloud_storage import cloud_storage as cs


class Reliquary(ERC_20.ERC_20):

    def __init__(self, reliquary_address: str, from_block: int, rpc_url: str, wait_time: float, interval: float, index: str):
        self.reliquary_address = reliquary_address
        self.from_block = from_block
        self.rpc_url = rpc_url
        self.wait_time = wait_time
        self.interval = interval
        self.index = index
        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.index
        self.lend_event_table_name = index + '_lend_events'

        web3 = lph.get_web_3(rpc_url)
        self.web3 = web3

    # # reliquary contract
    def get_child_reliquary_abi():
        contract_abi = [{"inputs":[{"internalType":"address","name":"_rewardToken","type":"address"},{"internalType":"address","name":"_reliquary","type":"address"},{"internalType":"uint8","name":"_poolId","type":"uint8"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[{"internalType":"address","name":"target","type":"address"}],"name":"AddressEmptyCode","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"AddressInsufficientBalance","type":"error"},{"inputs":[],"name":"FailedInnerCall","type":"error"},{"inputs":[],"name":"MathOverflowedMulDiv","type":"error"},{"inputs":[],"name":"RollingRewarder__NOT_OWNER","type":"error"},{"inputs":[],"name":"RollingRewarder__NOT_PARENT","type":"error"},{"inputs":[],"name":"RollingRewarder__ZERO_INPUT","type":"error"},{"inputs":[{"internalType":"address","name":"token","type":"address"}],"name":"SafeERC20FailedOperation","type":"error"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"_newDistributionPeriod","type":"uint256"}],"name":"Fund","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"_newDistributionPeriod","type":"uint256"}],"name":"Issue","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"_relicId","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_rewardAmount","type":"uint256"},{"indexed":False,"internalType":"address","name":"_to","type":"address"}],"name":"LogOnReward","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"_newDistributionPeriod","type":"uint256"}],"name":"UpdateDistributionPeriod","type":"event"},{"inputs":[],"name":"accRewardPerShare","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"distributionPeriod","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"fund","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_seconds","type":"uint256"}],"name":"getRewardAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lastDistributionTime","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lastIssuanceTimestamp","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract ICurves","name":"_curve","type":"address"},{"internalType":"uint256","name":"_relicId","type":"uint256"},{"internalType":"uint256","name":"_depositAmount","type":"uint256"},{"internalType":"uint256","name":"_oldAmount","type":"uint256"},{"internalType":"uint256","name":"_oldLevel","type":"uint256"},{"internalType":"uint256","name":"_newLevel","type":"uint256"}],"name":"onDeposit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ICurves","name":"_curve","type":"address"},{"internalType":"uint256","name":"_fromId","type":"uint256"},{"internalType":"uint256","name":"_toId","type":"uint256"},{"internalType":"uint256","name":"_fromAmount","type":"uint256"},{"internalType":"uint256","name":"_toAmount","type":"uint256"},{"internalType":"uint256","name":"_fromLevel","type":"uint256"},{"internalType":"uint256","name":"_oldToLevel","type":"uint256"},{"internalType":"uint256","name":"_newToLevel","type":"uint256"}],"name":"onMerge","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_relicId","type":"uint256"},{"internalType":"address","name":"_to","type":"address"}],"name":"onReward","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ICurves","name":"_curve","type":"address"},{"internalType":"uint256","name":"_fromId","type":"uint256"},{"internalType":"uint256","name":"_toId","type":"uint256"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"uint256","name":"_oldFromAmount","type":"uint256"},{"internalType":"uint256","name":"_oldToAmount","type":"uint256"},{"internalType":"uint256","name":"_fromLevel","type":"uint256"},{"internalType":"uint256","name":"_oldToLevel","type":"uint256"},{"internalType":"uint256","name":"_newToLevel","type":"uint256"}],"name":"onShift","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ICurves","name":"_curve","type":"address"},{"internalType":"uint256","name":"_fromId","type":"uint256"},{"internalType":"uint256","name":"_newId","type":"uint256"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"uint256","name":"_fromAmount","type":"uint256"},{"internalType":"uint256","name":"_level","type":"uint256"}],"name":"onSplit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ICurves","name":"_curve","type":"address"},{"internalType":"uint256","name":"_relicId","type":"uint256"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"uint256","name":"_oldLevel","type":"uint256"},{"internalType":"uint256","name":"_newLevel","type":"uint256"}],"name":"onUpdate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ICurves","name":"_curve","type":"address"},{"internalType":"uint256","name":"_relicId","type":"uint256"},{"internalType":"uint256","name":"_withdrawalAmount","type":"uint256"},{"internalType":"uint256","name":"_oldAmount","type":"uint256"},{"internalType":"uint256","name":"_oldLevel","type":"uint256"},{"internalType":"uint256","name":"_newLevel","type":"uint256"}],"name":"onWithdraw","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"parent","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_relicId","type":"uint256"}],"name":"pendingToken","outputs":[{"internalType":"uint256","name":"amount_","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_relicId","type":"uint256"}],"name":"pendingTokens","outputs":[{"internalType":"address[]","name":"rewardTokens_","type":"address[]"},{"internalType":"uint256[]","name":"rewardAmounts_","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"poolId","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"reliquary","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"rewardPerSecond","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"rewardToken","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_newDistributionPeriod","type":"uint256"}],"name":"updateDistributionPeriod","outputs":[],"stateMutability":"nonpayable","type":"function"}]
        
        return contract_abi
    
    # # will make our web3 token contract object for our given reliq address
    def get_child_reliquary_contract(self, reliquary_address):
        token_abi = self.get_child_reliquary_abi()
        contract_address = reliquary_address

        web3 = self.web3

        contract = lph.get_contract(contract_address, token_abi, web3)
        time.sleep(self.WAIT_TIME)

        return contract
    
    # # gets the last block we have scanned
    def get_last_block_checked(self, df):

        df['block_number'] = df['block_number'].astype(float)

        last_checked_block_number = df['block_number'].max()

        return last_checked_block_number
    
    # # gets the bare minimum block we want to start our scanning from
    def get_from_block(self, df):
        
        from_block = self.from_block

        last_block_checked = self.get_last_block_checked(df)

        if last_block_checked > from_block:
            from_block = last_block_checked
            from_block = from_block - self.interval

        from_block = int(from_block)
        
        return from_block

    # # returns any events that may have occured in the specified block range
    def get_fund_events(self, from_block, to_block):
        
        from_block = int(from_block)
        to_block = int(to_block)
        
        events = self.contract.events.fund.get_logs(fromBlock=from_block, toBlock=to_block)

        return events

    # # returns the unix timestamp of when the reliqs last distribution will occur and will require a refill
    def def_get_last_distribution(self, contract):
        
        from_block = int(from_block)
        to_block = int(to_block)
        
        last_distribution_unix = contract.functions.lastDistributionTime().call()

        return last_distribution_unix

    # # # will get the mint fee paid when someone mints our cdp stablecoin
    # def get_mint_fee_events(self, from_block, to_block):
        
    #     from_block = int(from_block)
    #     to_block = int(to_block)
        
    #     events = self.contract.events.LUSDBorrowingFeePaid.get_logs(fromBlock=from_block, toBlock=to_block)

    #     return events
    
    # # # makes a boilerplate dummy data df
    # def make_default_df(self):

    #     df = pd.DataFrame()
        
    #     df['borrower_address'] = ['0x0000000000000000000000000000000000000000']
    #     df['tx_hash'] = ['0x0000000000000000000000000000000000000000']
    #     df['timestamp'] = [1776]
    #     df['collateral_address'] = ['0x0000000000000000000000000000000000000000']
    #     df['collateral_amount'] = [0]
    #     df['usd_collateral_amount'] = [0]
    #     df['debt_amount'] = [0]
    #     df['mint_fee'] = [0]
    #     df['block_number'] = [1776]

    #     return df
    
    # # # will load our cloud df information into the sql database
    # def insert_bulk_data_into_table(self, df, table_name):

    #     query = f"""
    #     INSERT INTO {table_name} (borrower_address, tx_hash, timestamp, collateral_address, collateral_amount, usd_collateral_amount, debt_amount, mint_fee, block_number)
    #     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    #     """

    #     sql.write_to_custom_table(query, df)

    #     return
    
    # # # creates our local oToken table
    # def create_cdp_table(self, cloud_df):
    #     query = f"""
    #         CREATE TABLE IF NOT EXISTS {self.table_name}(
    #             borrower_address TEXT,
    #             tx_hash TEXT,
    #             timestamp TEXT,
    #             collateral_address TEXT,
    #             collateral_amount TEXT,
    #             usd_collateral_amount TEXT,
    #             debt_amount TEXT,
    #             mint_fee TEXT,
    #             block_number TEXT
    #             )
    #         """

    #     # # will only insert data into the sql table if the table doesn't exist
    #     sql.create_custom_table(query)
        
    #     db_df = sql.get_cdp_token_data_df(self.table_name)

    #     # # makes a combined local and cloud dataframe and drops any duplicates from the dataframe
    #     # # drops our old database table
    #     # # then writes the updated sanitized dataframe to our local database table
    #     combined_df = pd.concat([db_df, cloud_df])
    #     combined_df = combined_df.drop_duplicates(subset=self.duplicate_column_list)
        
    #     sql.drop_table(self.table_name)
    #     sql.create_custom_table(query)
    #     self.insert_bulk_data_into_table(combined_df, self.table_name)

    #     return combined_df
    
    # # # will make a dataframe of all the tokens in our lend_event table and their decimals
    # def get_asset_decimal_df(self):
    #     lend_event_df = sql.get_transaction_data_df(self.lend_event_table_name)

    #     unique_reserve_address_list = lend_event_df['reserve_address'].unique()

    #     reserve_address_list = []
    #     token_decimal_list = []

    #     for reserve_address in unique_reserve_address_list:
    #         token_contract = ERC_20.ERC_20(reserve_address, self.rpc_url)
    #         token_decimals = int(token_contract.decimals)

    #         reserve_address_list.append(reserve_address)
    #         token_decimal_list.append(token_decimals)
        
    #     df = pd.DataFrame()
    #     df['reserve_address'] = reserve_address_list
    #     df['token_decimal'] = token_decimal_list

    #     return df
    
    # # # will return a dataframe with the closest known asset_price to our BorrowerOperations TroveUpdated
    # def add_collateral_token_pricing_to_df(self, event_df):
    #     lend_df = sql.get_transaction_data_df(self.lend_event_table_name)

    #     lend_df = lend_df[['reserve_address','block_number', 'asset_price']]

    #     unique_reserve_address_list = self.asset_decimal_df['reserve_address'].unique()

    #     # # temporarily will add decimals to our lend_df
    #     for reserve_address in unique_reserve_address_list:
    #         token_decimal = self.asset_decimal_df.loc[self.asset_decimal_df['reserve_address'] == reserve_address]['token_decimal'].max()
    #         lend_df.loc[lend_df['reserve_address'] == reserve_address, 'token_decimal'] = token_decimal

    #     event_df['block_number'] = event_df['block_number'].astype(int)
    #     event_df['collateral_amount'] = event_df['collateral_amount'].astype(float)

    #     lend_df['block_number'] = lend_df['block_number'].astype(int)
    #     lend_df[['asset_price', 'token_decimal']] = lend_df[['asset_price', 'token_decimal']].astype(float)
        
    #     event_df = event_df.sort_values(by='block_number')
    #     lend_df = lend_df.sort_values(by='block_number')

    #     df = pd.merge_asof(event_df, lend_df, on='block_number', direction='nearest')

    #     # df.rename(columns = {'asset_price':'usd_payment_amount'}, inplace = True)
    #     df['usd_collateral_amount'] = df['asset_price'] * df['collateral_amount'] / df['token_decimal']

    #     df = df[['borrower_address', 'tx_hash', 'timestamp', 'collateral_address', 'collateral_amount', 'usd_collateral_amount', 'debt_amount', 'mint_fee', 'block_number']]

    #     return df
    
    # # # will return a boolean on whether our event already exists or not
    # def event_already_exists(self, event):

    #     column_list = self.duplicate_column_list
    #     # ['borrower_address', 'tx_hash', 'collateral_address', 'collateral_amount', 'debt_amount']

    #     borrower_address = event['args']['_borrower']
    #     tx_hash = str(event['transactionHash'].hex())
    #     collateral_address = event['args']['_collateral']
    #     collateral_amount = event['args']['_coll']
    #     debt_amount = event['args']['_debt']

    #     value_list = [borrower_address, tx_hash, collateral_address, collateral_amount, debt_amount]

    #     exists = sql.sql_multiple_values_exist(value_list, column_list, self.table_name)

    #     return exists
    
    # def mint_fee_event_already_exists(self, event):
        
    #     column_list = ['borrower_address', 'tx_hash', 'collateral_address', 'mint_fee']
        
    #     borrower_address = event['args']['_borrower']
    #     tx_hash = event['transactionHash'].hex()
    #     collateral_address = event['args']['_collateral']
    #     mint_fee = event['args']['_LUSDFee']

    #     value_list = [borrower_address, tx_hash, collateral_address, mint_fee]

    #     exists = sql.sql_multiple_values_exist(value_list, column_list, self.table_name)

    #     return exists
    
    # # # will process our events
    # def process_events(self, events):

    #     df = pd.DataFrame()
    #     # ['borrower_address', 'tx_hash', 'timestamp', 'collateral_address', 'collateral_amount', 'usd_collateral_amount', 'debt_amount', 'block_number']
        
    #     borrower_list = []
    #     tx_hash_list = []
    #     timestamp_list = []
    #     collateral_address_list = []
    #     collateral_amount_list = []
    #     usd_collateral_amount_list = []
    #     debt_amount_list = []
    #     block_number_list = []

    #     i = 1

    #     for event in events:
    #         i += 1

    #         event_exists = self.event_already_exists(event)

    #         if event_exists == False:
                
    #             borrower_address = event['args']['_borrower']
    #             tx_hash = str(event['transactionHash'].hex())
    #             collateral_address = event['args']['_collateral']
    #             collateral_amount = event['args']['_coll']
    #             debt_amount = event['args']['_debt']
                
    #             try:
    #                 block = self.web3.eth.get_block(event['blockNumber'])
    #                 block_number = int(block['number'])
    #             except:
    #                 block_number = int(event['blockNumber'])
                
    #             block_timestamp = block['timestamp']
                
    #             block_number_list.append(block_number)
    #             tx_hash_list.append(tx_hash)
    #             timestamp_list.append(block_timestamp)
    #             borrower_list.append(borrower_address)
    #             collateral_address_list.append(collateral_address)
    #             collateral_amount_list.append(collateral_amount)
    #             debt_amount_list.append(debt_amount)


    #         time.sleep(self.wait_time)
        
    #     if len(tx_hash_list) > 0:

    
    #         df['borrower_address'] = borrower_list
    #         df['tx_hash'] = tx_hash_list
    #         df['timestamp'] = timestamp_list
    #         df['collateral_address'] = collateral_address_list
    #         df['collateral_amount'] = collateral_amount_list
    #         df['debt_amount'] = debt_amount_list
    #         df['mint_fee'] = 0
    #         df['block_number'] = block_number_list
    #         df = self.add_collateral_token_pricing_to_df(df)

    #     return df

    # # # will process our events
    # def process_mint_fee_events(self, events):

    #     df = pd.DataFrame()
    #     # ['borrower_address', 'tx_hash', 'timestamp', 'collateral_address', 'collateral_amount', 'usd_collateral_amount', 'debt_amount', 'block_number']
        
    #     borrower_address_list = []
    #     tx_hash_list = []
    #     collateral_address_list = []
    #     mint_fee_list = []
    #     # block_number_list = []
    #     # timestamp_list = []

    #     i = 1

    #     for event in events:
    #         i += 1

    #         event_exists = self.mint_fee_event_already_exists(event)

    #         if event_exists == False:
                
    #             borrower_address = event['args']['_borrower']
    #             tx_hash = str(event['transactionHash'].hex())
    #             collateral_address = event['args']['_collateral']
    #             mint_fee = event['args']['_LUSDFee']
    #             mint_fee /= self.decimals
                
    #             # try:
    #             #     block = self.web3.eth.get_block(event['blockNumber'])
    #             #     block_number = int(block['number'])
    #             # except:
    #             #     block_number = int(event['blockNumber'])
                
    #             # block_timestamp = block['timestamp']
                
    #             # block_number_list.append(block_number)
    #             tx_hash_list.append(tx_hash)
    #             # timestamp_list.append(block_timestamp)
    #             borrower_address_list.append(borrower_address)
    #             collateral_address_list.append(collateral_address)
    #             mint_fee_list.append(mint_fee)


    #         time.sleep(self.wait_time)
        
    #     if len(tx_hash_list) > 0:

    
    #         df['borrower_address'] = borrower_address_list
    #         df['tx_hash'] = tx_hash_list
    #         df['collateral_address'] = collateral_address_list
    #         df['mint_fee'] = mint_fee_list
    #         # df['block_number'] = block_number_list
    #         # df['timestamp'] = timestamp_list

    #     return df
    
    # def combine_trove_and_mint_df(self, trove_df, mint_df):
    #     # Merge the DataFrames on common columns
    #     merged = pd.merge(trove_df, mint_df[['borrower_address', 'collateral_address', 'tx_hash', 'mint_fee']], 
    #                     on=['borrower_address', 'collateral_address', 'tx_hash'], 
    #                     how='left', 
    #                     suffixes=('', '_new'))

    #     # Update the mint_fee column in df1
    #     trove_df['mint_fee'] = merged['mint_fee_new'].fillna(trove_df['mint_fee'])

    #     # If you want to drop the extra column created by the merge
    #     trove_df = trove_df.drop('mint_fee_new', axis=1, errors='ignore')

    #     return trove_df
    
    # # # will track all of our cdp_events
    # def run_all_cdp_tracking(self):

    #     self.asset_decimal_df = self.get_asset_decimal_df()

    #     time.sleep(self.WAIT_TIME)
    
    #     try:
    #         cloud_df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)
    #         cloud_df.drop_duplicates(subset=self.duplicate_column_list)
    #     except:
    #         cloud_df = self.make_default_df()

    #     df = self.create_cdp_table(cloud_df)

    #     from_block = self.get_from_block(df)

    #     latest_block = lph.get_latest_block(self.web3)

    #     interval = self.interval
    #     wait_time = self.wait_time

    #     to_block = from_block + interval

    #     while to_block < latest_block:
            
    #         events = self.get_trove_updated_events(from_block, to_block)
            
    #         time.sleep(wait_time)
    #         mint_events = self.get_mint_fee_events(from_block, to_block)

    #         if len(events) > 0:
    #             trove_df = self.process_events(events)

    #         else:
    #             trove_df = pd.DataFrame()

    #         if len(mint_events) > 0:
    #             mint_df = self.process_mint_fee_events(mint_events)
            
    #         else:
    #             mint_df = pd.DataFrame()

    #         # # if our trove df exists, it means we will write to our database
    #         # # if our mint_df also exists, we will add the mint_fee from mint_df to the default 0 mint_fee of trove_updated
    #         if len(trove_df) > 0:
    #             if len(mint_df) > 0:
    #                 trove_df = self.combine_trove_and_mint_df(trove_df, mint_df)
    #             sql.write_to_db(trove_df, self.column_list, self.table_name)

    #         time.sleep(wait_time)

    #         from_block += interval
    #         to_block += interval

    #         if from_block >= latest_block:
    #             from_block = latest_block - 1
            
    #         if to_block >= latest_block:
    #             to_block = latest_block
            
    #         print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)
    #         time.sleep(wait_time)
        
    #     df = sql.get_cdp_token_data_df(self.table_name)
    #     df = df.drop_duplicates(subset=self.duplicate_column_list)
    #     cs.df_write_to_cloud_storage_as_zip(df, self.cloud_file_name, self.cloud_bucket_name)
        
    #     return
