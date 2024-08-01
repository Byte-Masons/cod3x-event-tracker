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


class CDP(ERC_20.ERC_20):

    def __init__(self, borrower_operations_address: str, from_block: int, rpc_url: str, wait_time: float, interval: float, index: str):
        self.borrower_operations_address = self.borrower_operations_address
        self.from_block = from_block
        self.rpc_url = rpc_url
        self.wait_time = wait_time
        self.interval = interval
        self.index = self.get_event_index(index)
        self.cloud_file_name = self.get_cloud_filename()
        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.index
        self.lend_event_table_name = self.index.split('_lend')[0] + '_lend_events'
        
        self.column_list = ['sender', 'recipient', 'tx_hash', 'timestamp', 'o_token_address', 'payment_token_address', 'o_token_amount', 'payment_token_amount', 'usd_o_token_amount', 'usd_payment_amount', 'block_number']
        self.duplicate_column_list = ['sender', 'tx_hash', 'o_token_amount', 'payment_token_amount']

        web3 = lph.get_web_3(rpc_url)
        self.web3 = web3

        contract = self.get_borrower_operations_contract()
        self.contract = contract

        time.sleep(self.WAIT_TIME)
        
        self.debt_token_address = self.get_debt_token_address()

        time.sleep(self.WAIT_TIME)

        self.token_contract = self.get_token_contract(self.debt_token_address)
        time.sleep(self.WAIT_TIME)

        self.decimals = self.get_token_decimals()
        self.decimals = 10 ** self.decimals

        time.sleep(self.WAIT_TIME)
    
    # # adds onto our index for o_token_events
    def get_event_index(self, index):
        index = index + '_cdp_events'
        
        return index
    
    # # makes our revenue cloud filename
    def get_cloud_filename(self):
        cloud_filename = self.index + '.zip'

        return cloud_filename

    # # gets our LUSD debt token
    def get_debt_token_address(self):
        debt_token_address = self.contract.functions.lusdToken().call()

        return debt_token_address
    
    # # cdp contract
    def get_borrower_operations_abi():

        abi = [{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_activePoolAddress","type":"address"}],"name":"ActivePoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collSurplusPoolAddress","type":"address"}],"name":"CollSurplusPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newCollateralConfigAddress","type":"address"}],"name":"CollateralConfigAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_defaultPoolAddress","type":"address"}],"name":"DefaultPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_gasPoolAddress","type":"address"}],"name":"GasPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_lqtyStakingAddress","type":"address"}],"name":"LQTYStakingAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_LUSDFee","type":"uint256"}],"name":"LUSDBorrowingFeePaid","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_lusdTokenAddress","type":"address"}],"name":"LUSDTokenAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":True,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newPriceFeedAddress","type":"address"}],"name":"PriceFeedAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_sortedTrovesAddress","type":"address"}],"name":"SortedTrovesAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_stabilityPoolAddress","type":"address"}],"name":"StabilityPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"arrayIndex","type":"uint256"}],"name":"TroveCreated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newTroveManagerAddress","type":"address"}],"name":"TroveManagerAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_debt","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_coll","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"stake","type":"uint256"},{"indexed":False,"internalType":"enum BorrowerOperations.BorrowerOperation","name":"operation","type":"uint8"}],"name":"TroveUpdated","type":"event"},{"inputs":[],"name":"BORROWING_FEE_FLOOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DECIMAL_PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"LUSD_GAS_COMPENSATION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_NET_DEBT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"NAME","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERCENT_DIVISOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"_100pct","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"activePool","outputs":[{"internalType":"contract IActivePool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"addColl","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_maxFeePercentage","type":"uint256"},{"internalType":"uint256","name":"_collTopUp","type":"uint256"},{"internalType":"uint256","name":"_collWithdrawal","type":"uint256"},{"internalType":"uint256","name":"_LUSDChange","type":"uint256"},{"internalType":"bool","name":"_isDebtIncrease","type":"bool"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"adjustTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"claimCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"closeTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"collateralConfig","outputs":[{"internalType":"contract ICollateralConfig","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"defaultPool","outputs":[{"internalType":"contract IDefaultPool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_debt","type":"uint256"}],"name":"getCompositeDebt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"getEntireSystemColl","outputs":[{"internalType":"uint256","name":"entireSystemColl","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"getEntireSystemDebt","outputs":[{"internalType":"uint256","name":"entireSystemDebt","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lqtyStaking","outputs":[{"internalType":"contract ILQTYStaking","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lqtyStakingAddress","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lusdToken","outputs":[{"internalType":"contract ILUSDToken","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"moveCollateralGainToTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collAmount","type":"uint256"},{"internalType":"uint256","name":"_maxFeePercentage","type":"uint256"},{"internalType":"uint256","name":"_LUSDAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"openTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"priceFeed","outputs":[{"internalType":"contract IPriceFeed","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_LUSDAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"repayLUSD","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateralConfigAddress","type":"address"},{"internalType":"address","name":"_troveManagerAddress","type":"address"},{"internalType":"address","name":"_activePoolAddress","type":"address"},{"internalType":"address","name":"_defaultPoolAddress","type":"address"},{"internalType":"address","name":"_stabilityPoolAddress","type":"address"},{"internalType":"address","name":"_gasPoolAddress","type":"address"},{"internalType":"address","name":"_collSurplusPoolAddress","type":"address"},{"internalType":"address","name":"_priceFeedAddress","type":"address"},{"internalType":"address","name":"_sortedTrovesAddress","type":"address"},{"internalType":"address","name":"_lusdTokenAddress","type":"address"},{"internalType":"address","name":"_lqtyStakingAddress","type":"address"}],"name":"setAddresses","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"sortedTroves","outputs":[{"internalType":"contract ISortedTroves","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"troveManager","outputs":[{"internalType":"contract ITroveManager","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collWithdrawal","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"withdrawColl","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_maxFeePercentage","type":"uint256"},{"internalType":"uint256","name":"_LUSDAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"withdrawLUSD","outputs":[],"stateMutability":"nonpayable","type":"function"}]
        return abi

    # # will make our web3 token contract object for our given token_address
    def get_borrower_operations_contract(self):
        token_abi = self.get_borrower_operations_abi()
        contract_address = self.borrower_operations_address

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
    def get_borrower_operations_from_block(self, df):
        
        from_block = self.from_block

        last_block_checked = self.get_last_block_checked(df)

        if last_block_checked > from_block:
            from_block = last_block_checked
            from_block = from_block - self.interval

        from_block = int(from_block)
        
        return from_block

    # # returns any events that may have occured in the specified block range
    def get_trove_updated_events(self, from_block, to_block):
        
        from_block = int(from_block)
        to_block = int(to_block)
        
        events = self.contract.events.TroveUpdated.get_logs(fromBlock=from_block, toBlock=to_block)

        return events
    
    # # makes a boilerplate dummy data df
    def make_default_df(self):

        df = pd.DataFrame()
        
        df['borrower_address'] = ['0x0000000000000000000000000000000000000000']
        df['tx_hash'] = ['0x0000000000000000000000000000000000000000']
        df['timestamp'] = [1776]
        df['collateral_address'] = [self.o_token_address]
        df['collateral_amount'] = [self.payment_token_address]
        df['usd_collateral_amount'] = [0]
        df['debt_amount'] = [0]
        df['mint_fee'] = [0]
        df['block_number'] = [1776]

        return df
    
    # # will load our cloud df information into the sql database
    def insert_bulk_data_into_table(self, df, table_name):

        query = f"""
        INSERT INTO {table_name} (sender, recipient, tx_hash, timestamp, o_token_address, payment_token_address, o_token_amount, payment_token_amount, usd_o_token_amount, usd_payment_amount, block_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        sql.write_to_custom_table(query, df)

        return
    
    # # creates our local oToken table
    def create_o_token_table(self, cloud_df):
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}(
                sender TEXT,
                recipient TEXT,
                tx_hash TEXT,
                timestamp TEXT,
                o_token_address TEXT,
                payment_token_address TEXT,
                o_token_amount TEXT,
                payment_token_amount TEXT,
                usd_o_token_amount TEXT,
                usd_payment_amount TEXT,
                block_number TEXT
                )
            """

        # # will only insert data into the sql table if the table doesn't exist
        sql.create_custom_table(query)
        
        db_df = sql.get_o_token_data_df(self.table_name)

        # # makes a combined local and cloud dataframe and drops any duplicates from the dataframe
        # # drops our old database table
        # # then writes the updated sanitized dataframe to our local database table
        combined_df = pd.concat([db_df, cloud_df])
        combined_df = combined_df.drop_duplicates(subset=self.duplicate_column_list)
        
        sql.drop_table(self.table_name)
        sql.create_custom_table(query)
        self.insert_bulk_data_into_table(combined_df, self.table_name)

        return combined_df
    
    # # will return a dataframe with the closest known asset_price to our oToken exercise
    def add_payment_and_o_token_pricing_to_df(self, event_df):
        lend_df = sql.get_transaction_data_df(self.lend_event_table_name)

        lend_df = lend_df.loc[lend_df['reserve_address'] == self.payment_token_address]
        lend_df = lend_df[['block_number', 'asset_price']]

        event_df['block_number'] = event_df['block_number'].astype(int)
        event_df['payment_token_amount'] = event_df['payment_token_amount'].astype(float)

        lend_df['block_number'] = lend_df['block_number'].astype(int)
        lend_df['asset_price'] = lend_df['asset_price'].astype(float)
        
        event_df = event_df.sort_values(by='block_number')
        lend_df = lend_df.sort_values(by='block_number')

        df = pd.merge_asof(event_df, lend_df, on='block_number', direction='nearest')

        # df.rename(columns = {'asset_price':'usd_payment_amount'}, inplace = True)
        df['usd_payment_amount'] = df['asset_price'] * df['payment_token_amount'] / self.decimals
        # # tokens they receive should be about 2x what they paid
        df['usd_o_token_amount'] = df['usd_payment_amount'] * 2

        df = df[['sender', 'recipient', 'tx_hash', 'timestamp', 'o_token_address', 'payment_token_address', 'o_token_amount', 'payment_token_amount', 'usd_o_token_amount', 'usd_payment_amount','block_number']]

        return df
    
    # # will return a boolean on whether our event already exists or not
    def exercise_event_already_exists(self, event):

        # column_list = ['sender', 'tx_hash', 'o_token_amount', 'payment_token_amount']
        column_list = ['sender', 'o_token_amount', 'payment_token_amount']

        # tx_hash = str(event['transactionHash'].hex())
        sender_address = event['args']['sender']
        o_token_amount = event['args']['amount']
        payment_token_amount = event['args']['paymentAmount']

        # value_list = [tx_hash, sender_address, o_token_amount, payment_token_amount]

        value_list = [sender_address, o_token_amount, payment_token_amount]

        exists = sql.sql_multiple_values_exist(value_list, column_list, self.table_name)

        return exists
    
    # # will process our events
    def process_events(self, events):

        df = pd.DataFrame()

        sender_list = []
        recipient_list = []
        tx_hash_list = []
        timestamp_list = []
        o_token_address_list = []
        payment_token_address_list = []
        o_token_amount_list = []
        payment_token_amount_list = []
        usd_o_token_amount_list = []
        usd_payment_amount_list = []
        block_number_list = []

        i = 1

        for event in events:
            i += 1

            event_exists = self.exercise_event_already_exists(event)

            if event_exists == False:
                
                sender_address = event['args']['sender']
                recipient_address = event['args']['recipient']
                tx_hash = event['transactionHash'].hex()
                o_token_amount = event['args']['amount']
                payment_token_amount = event['args']['paymentAmount']
                
                try:
                    block = self.web3.eth.get_block(event['blockNumber'])
                    block_number = int(block['number'])
                except:
                    block_number = int(event['blockNumber'])
                
                block_timestamp = block['timestamp']
                
                block_number_list.append(block_number)
                tx_hash_list.append(tx_hash)
                timestamp_list.append(block_timestamp)
                sender_list.append(sender_address)
                recipient_list.append(recipient_address)
                o_token_amount_list.append(o_token_amount)
                payment_token_amount_list.append(payment_token_amount)

            time.sleep(self.wait_time)
        
        if len(sender_list) > 0:

    
            df['sender'] = sender_list
            df['recipient'] = recipient_list
            df['tx_hash'] = tx_hash_list
            df['timestamp'] = timestamp_list
            df['o_token_address'] = self.o_token_address
            df['payment_token_address'] = self.payment_token_address
            df['o_token_amount'] = o_token_amount_list
            df['payment_token_amount'] = payment_token_amount_list
            df['block_number'] = block_number_list
            df = self.add_payment_and_o_token_pricing_to_df(df)

        return df
    
    # # will track all of our o_token_events
    def run_all_o_token_tracking(self):

        try:
            cloud_df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)
            cloud_df.drop_duplicates(subset=self.duplicate_column_list)
        except:
            cloud_df = self.make_default_o_token_df()

        df = self.create_o_token_table(cloud_df)
        
        from_block = self.get_o_token_from_block(df)

        latest_block = lph.get_latest_block(self.web3)

        interval = self.interval
        wait_time = self.wait_time

        to_block = from_block + interval

        while to_block < latest_block:
            
            events = self.get_exercised_events(from_block, to_block)

            if len(events) > 0:
                df = self.process_events(events)

                if len(df) > 0:
                    sql.write_to_db(df, self.column_list, self.table_name)

            time.sleep(wait_time)

            from_block += interval
            to_block += interval

            if from_block >= latest_block:
                from_block = latest_block - 1
            
            if to_block >= latest_block:
                to_block = latest_block
            
            print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)
            time.sleep(wait_time)
        
        df = sql.get_o_token_data_df(self.table_name)
        df = df.drop_duplicates(subset=self.duplicate_column_list)
        cs.df_write_to_cloud_storage_as_zip(df, self.cloud_file_name, self.cloud_bucket_name)
        
        return