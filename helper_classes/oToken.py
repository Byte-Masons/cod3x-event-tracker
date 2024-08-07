import sys
import os
import time
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import lending_pool_helper as lph
from helper_classes import ERC_20
from sql_interfacer import sql
from cloud_storage import cloud_storage as cs

class oToken(ERC_20.ERC_20):

    def __init__(self, exercise_address: str, from_block: int, rpc_url: str, wait_time: float, interval: float, index: str):
        self.exercise_address = exercise_address
        self.from_block = from_block
        self.rpc_url = rpc_url
        self.wait_time = wait_time
        self.interval = interval
        self.index = self.get_event_index(index)
        self.cloud_file_name = self.get_cloud_filename()
        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.index
        self.lend_event_table_name = index.split('_lend')[0] + '_lend_events'
        
        self.column_list = ['sender', 'recipient', 'tx_hash', 'timestamp', 'o_token_address', 'payment_token_address', 'o_token_amount', 'payment_token_amount', 'usd_o_token_amount', 'usd_payment_amount', 'block_number']
        self.duplicate_column_list = ['sender', 'tx_hash', 'o_token_amount', 'payment_token_amount']

        web3 = lph.get_web_3(rpc_url)
        self.web3 = web3

        contract = self.get_exercise_contract()
        self.exercise_contract = contract

        time.sleep(self.WAIT_TIME)
        
        self.o_token_address = self.get_o_token_address()

        time.sleep(self.WAIT_TIME)

        self.payment_token_address = self.get_payment_token_address()
        time.sleep(self.WAIT_TIME)

        self.token_contract = self.get_token_contract(self.payment_token_address)
        time.sleep(self.WAIT_TIME)

        self.decimals = self.get_token_decimals()
        self.decimals = 10 ** self.decimals

        time.sleep(self.WAIT_TIME)
    
    # # adds onto our index for o_token_events
    def get_event_index(self, index):
        index = index + '_o_token_events'
        
        return index
    
    # # makes our revenue cloud filename
    def get_cloud_filename(self):
        cloud_filename = self.index + '.zip'

        return cloud_filename
    
    # # gets the discount_exercise contract abi
    def get_exercise_abi(self):
        contract_abi = [ { "inputs": [ { "internalType": "contract OptionsToken", "name": "oToken_", "type": "address" }, { "internalType": "address", "name": "owner_", "type": "address" }, { "internalType": "contract IERC20", "name": "paymentToken_", "type": "address" }, { "internalType": "contract IERC20", "name": "underlyingToken_", "type": "address" }, { "internalType": "contract IOracle", "name": "oracle_", "type": "address" }, { "internalType": "uint256", "name": "multiplier_", "type": "uint256" }, { "internalType": "uint256", "name": "instantExitFee_", "type": "uint256" }, { "internalType": "uint256", "name": "minAmountToTriggerSwap_", "type": "uint256" }, { "internalType": "address[]", "name": "feeRecipients_", "type": "address[]" }, { "internalType": "uint256[]", "name": "feeBPS_", "type": "uint256[]" }, { "components": [ { "internalType": "address", "name": "swapper", "type": "address" }, { "internalType": "address", "name": "exchangeAddress", "type": "address" }, { "internalType": "enum ExchangeType", "name": "exchangeTypes", "type": "uint8" }, { "internalType": "uint256", "name": "maxSwapSlippage", "type": "uint256" } ], "internalType": "struct SwapProps", "name": "swapProps_", "type": "tuple" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "inputs": [], "name": "Exercise__AmountOutIsZero", "type": "error" }, { "inputs": [], "name": "Exercise__FeeGreaterThanMax", "type": "error" }, { "inputs": [], "name": "Exercise__InvalidFeeAmounts", "type": "error" }, { "inputs": [], "name": "Exercise__InvalidOracle", "type": "error" }, { "inputs": [], "name": "Exercise__MultiplierOutOfRange", "type": "error" }, { "inputs": [], "name": "Exercise__NotOToken", "type": "error" }, { "inputs": [], "name": "Exercise__PastDeadline", "type": "error" }, { "inputs": [], "name": "Exercise__SlippageTooHigh", "type": "error" }, { "inputs": [], "name": "Exercise__ZapMultiplierIncompatible", "type": "error" }, { "inputs": [], "name": "Exercise__feeArrayLengthMismatch", "type": "error" }, { "inputs": [ { "internalType": "uint256", "name": "exType", "type": "uint256" } ], "name": "SwapHelper__InvalidExchangeType", "type": "error" }, { "inputs": [], "name": "SwapHelper__ParamHasAddressZero", "type": "error" }, { "inputs": [], "name": "SwapHelper__SlippageGreaterThanMax", "type": "error" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Claimed", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address[]", "name": "feeRecipients", "type": "address[]" }, { "indexed": False, "internalType": "uint256[]", "name": "feeBPS", "type": "uint256[]" }, { "indexed": False, "internalType": "uint256", "name": "totalAmount", "type": "uint256" } ], "name": "DistributeFees", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "sender", "type": "address" }, { "indexed": True, "internalType": "address", "name": "recipient", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "paymentAmount", "type": "uint256" } ], "name": "Exercised", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "OwnershipTransferred", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "account", "type": "address" } ], "name": "Paused", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address[]", "name": "feeRecipients", "type": "address[]" }, { "indexed": False, "internalType": "uint256[]", "name": "feeBPS", "type": "uint256[]" } ], "name": "SetFees", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "instantFee", "type": "uint256" } ], "name": "SetInstantFee", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "uint256", "name": "minAmountToTrigger", "type": "uint256" } ], "name": "SetMinAmountToTrigger", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "newMultiplier", "type": "uint256" } ], "name": "SetMultiplier", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "contract IOracle", "name": "newOracle", "type": "address" } ], "name": "SetOracle", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newTreasury", "type": "address" } ], "name": "SetTreasury", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "account", "type": "address" } ], "name": "Unpaused", "type": "event" }, { "inputs": [], "name": "FEE_DENOMINATOR", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "to", "type": "address" } ], "name": "claim", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "", "type": "address" } ], "name": "credit", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "from", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "address", "name": "recipient", "type": "address" }, { "internalType": "bytes", "name": "params", "type": "bytes" } ], "name": "exercise", "outputs": [ { "internalType": "uint256", "name": "paymentAmount", "type": "uint256" }, { "internalType": "address", "name": "", "type": "address" }, { "internalType": "uint256", "name": "", "type": "uint256" }, { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "name": "feeBPS", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "name": "feeRecipients", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "getPaymentAmount", "outputs": [ { "internalType": "uint256", "name": "paymentAmount", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "instantExitFee", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "minAmountToTriggerSwap", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "multiplier", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "oToken", "outputs": [ { "internalType": "contract IOptionsToken", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "oracle", "outputs": [ { "internalType": "contract IOracle", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "owner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "pause", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "paused", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "paymentToken", "outputs": [ { "internalType": "contract IERC20", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "_feeRecipients", "type": "address[]" }, { "internalType": "uint256[]", "name": "_feeBPS", "type": "uint256[]" } ], "name": "setFees", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_instantExitFee", "type": "uint256" } ], "name": "setInstantExitFee", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_minAmountToTriggerSwap", "type": "uint256" } ], "name": "setMinAmountToTriggerSwap", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "multiplier_", "type": "uint256" } ], "name": "setMultiplier", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "contract IOracle", "name": "oracle_", "type": "address" } ], "name": "setOracle", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "components": [ { "internalType": "address", "name": "swapper", "type": "address" }, { "internalType": "address", "name": "exchangeAddress", "type": "address" }, { "internalType": "enum ExchangeType", "name": "exchangeTypes", "type": "uint8" }, { "internalType": "uint256", "name": "maxSwapSlippage", "type": "uint256" } ], "internalType": "struct SwapProps", "name": "_swapProps", "type": "tuple" } ], "name": "setSwapProps", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "swapProps", "outputs": [ { "internalType": "address", "name": "swapper", "type": "address" }, { "internalType": "address", "name": "exchangeAddress", "type": "address" }, { "internalType": "enum ExchangeType", "name": "exchangeTypes", "type": "uint8" }, { "internalType": "uint256", "name": "maxSwapSlippage", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "transferOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "underlyingToken", "outputs": [ { "internalType": "contract IERC20", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "unpause", "outputs": [], "stateMutability": "nonpayable", "type": "function" } ]
        
        return contract_abi

    # # will make our web3 token contract object for our given token_address
    def get_exercise_contract(self):
        token_abi = self.get_exercise_abi()
        contract_address = self.exercise_address

        web3 = self.web3

        contract = lph.get_contract(contract_address, token_abi, web3)
        time.sleep(self.WAIT_TIME)

        return contract
    
    # # gets the o_token_address
    def get_o_token_address(self):
        o_token_address = self.exercise_contract.functions.oToken().call()

        return o_token_address
    
    # # gets the paymentToken used to exercise our option tokens
    def get_payment_token_address(self):
        payment_token_address = self.exercise_contract.functions.paymentToken().call()

        return payment_token_address
    
    # # gets the last block we have scanned
    def get_last_block_checked(self, df):

        df['block_number'] = df['block_number'].astype(float)

        last_checked_block_number = df['block_number'].max()

        return last_checked_block_number
    
    # # gets the bare minimum block we want to start our scanning from
    def get_o_token_from_block(self, df):
        
        from_block = self.from_block

        last_block_checked = self.get_last_block_checked(df)

        if last_block_checked > from_block:
            from_block = last_block_checked
            from_block = from_block - self.interval

        from_block = int(from_block)
        
        return from_block

    # # returns any events that may have occured in the specified block range
    def get_exercised_events(self, from_block, to_block):
        
        from_block = int(from_block)
        to_block = int(to_block)
        
        events = self.exercise_contract.events.Exercised.get_logs(fromBlock=from_block, toBlock=to_block)

        return events
    
    # # makes a boilerplate dummy data df for oTokens
    def make_default_o_token_df(self):

        df = pd.DataFrame()
        
        df['sender'] = ['0x0000000000000000000000000000000000000000']
        df['recipient'] = ['0x0000000000000000000000000000000000000000']
        df['tx_hash'] = ['0x0000000000000000000000000000000000000000']
        df['timestamp'] = [1776]
        df['o_token_address'] = [self.o_token_address]
        df['payment_token_address'] = [self.payment_token_address]
        df['o_token_amount'] = [0]
        df['payment_token_amount'] = [0]
        df['usd_o_token_amount'] = [0]
        df['usd_payment_amount'] = [0]
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
    