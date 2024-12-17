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

ICL_GAUGE_ADDRESS = '0x969904a7e77381a89Ae2BeE4c4C7d1C10e3563F8'

class VeMode_Voting(ERC_20.ERC_20):

    def __init__(self, contract_address: str, from_block: int, rpc_url: str, wait_time: float, interval: float, index: str):
        self.contract_address = contract_address
        self.from_block = from_block
        self.rpc_url = rpc_url
        self.wait_time = wait_time
        self.interval = interval
        self.index = self.get_event_index(index)
        self.cloud_file_name = self.get_cloud_filename()
        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.index

        self.column_list = ['voter_address', 'tx_hash', 'timestamp', 'gauge_address', 'epoch', 'token_id', 'voting_power_cast', 'total_gauge_voting_power', 'total_contract_voting_power', 'block_number']
        self.duplicate_column_list = ['voter_address', 'tx_hash', 'gauge_address', 'epoch', 'token_id', 'voting_power_cast']

        web3 = lph.get_web_3(rpc_url)
        self.web3 = web3

        time.sleep(self.WAIT_TIME)

        self.contract = self.get_contract()

        time.sleep(self.WAIT_TIME)
    
    # # adds onto our index for o_token_events
    def get_event_index(self, index):
        index = index + '_vote_events'
        
        return index
    
    # # makes our revenue cloud filename
    def get_cloud_filename(self):
        cloud_filename = self.index + '.zip'

        return cloud_filename
    
    # # guage_voter contract abi
    def get_abi(self):

        abi = [ { "inputs": [], "stateMutability": "nonpayable", "type": "constructor" }, { "inputs": [ { "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "AlreadyVoted", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "dao", "type": "address" }, { "internalType": "address", "name": "where", "type": "address" }, { "internalType": "address", "name": "who", "type": "address" }, { "internalType": "bytes32", "name": "permissionId", "type": "bytes32" } ], "name": "DaoUnauthorized", "type": "error" }, { "inputs": [], "name": "DoubleVote", "type": "error" }, { "inputs": [], "name": "GaugeActivationUnchanged", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "_pool", "type": "address" } ], "name": "GaugeDoesNotExist", "type": "error" }, { "inputs": [], "name": "GaugeExists", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "_gauge", "type": "address" } ], "name": "GaugeInactive", "type": "error" }, { "inputs": [], "name": "NoVotes", "type": "error" }, { "inputs": [], "name": "NoVotingPower", "type": "error" }, { "inputs": [], "name": "NotApprovedOrOwner", "type": "error" }, { "inputs": [], "name": "NotCurrentlyVoting", "type": "error" }, { "inputs": [], "name": "VotingInactive", "type": "error" }, { "inputs": [], "name": "ZeroGauge", "type": "error" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "previousAdmin", "type": "address" }, { "indexed": False, "internalType": "address", "name": "newAdmin", "type": "address" } ], "name": "AdminChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "beacon", "type": "address" } ], "name": "BeaconUpgraded", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "gauge", "type": "address" } ], "name": "GaugeActivated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "gauge", "type": "address" }, { "indexed": True, "internalType": "address", "name": "creator", "type": "address" }, { "indexed": False, "internalType": "string", "name": "metadataURI", "type": "string" } ], "name": "GaugeCreated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "gauge", "type": "address" } ], "name": "GaugeDeactivated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "gauge", "type": "address" }, { "indexed": False, "internalType": "string", "name": "metadataURI", "type": "string" } ], "name": "GaugeMetadataUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "uint8", "name": "version", "type": "uint8" } ], "name": "Initialized", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "account", "type": "address" } ], "name": "Paused", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "voter", "type": "address" }, { "indexed": True, "internalType": "address", "name": "gauge", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "epoch", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "tokenId", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "votingPowerRemovedFromGauge", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "totalVotingPowerInGauge", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "totalVotingPowerInContract", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "timestamp", "type": "uint256" } ], "name": "Reset", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "account", "type": "address" } ], "name": "Unpaused", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "implementation", "type": "address" } ], "name": "Upgraded", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "voter", "type": "address" }, { "indexed": True, "internalType": "address", "name": "gauge", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "epoch", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "tokenId", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "votingPowerCastForGauge", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "totalVotingPowerInGauge", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "totalVotingPowerInContract", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "timestamp", "type": "uint256" } ], "name": "Voted", "type": "event" }, { "inputs": [], "name": "GAUGE_ADMIN_ROLE", "outputs": [ { "internalType": "bytes32", "name": "", "type": "bytes32" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "UPGRADE_PLUGIN_PERMISSION_ID", "outputs": [ { "internalType": "bytes32", "name": "", "type": "bytes32" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_gauge", "type": "address" } ], "name": "activateGauge", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "clock", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_gauge", "type": "address" }, { "internalType": "string", "name": "_metadataURI", "type": "string" } ], "name": "createGauge", "outputs": [ { "internalType": "address", "name": "gauge", "type": "address" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "dao", "outputs": [ { "internalType": "contract IDAO", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_gauge", "type": "address" } ], "name": "deactivateGauge", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "epochId", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "epochStart", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "epochVoteEnd", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "epochVoteStart", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "escrow", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_gauge", "type": "address" } ], "name": "gaugeExists", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "name": "gaugeList", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "", "type": "address" } ], "name": "gaugeVotes", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "", "type": "address" } ], "name": "gauges", "outputs": [ { "internalType": "bool", "name": "active", "type": "bool" }, { "internalType": "uint256", "name": "created", "type": "uint256" }, { "internalType": "string", "name": "metadataURI", "type": "string" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_tokenId", "type": "uint256" } ], "name": "gaugesVotedFor", "outputs": [ { "internalType": "address[]", "name": "", "type": "address[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getAllGauges", "outputs": [ { "internalType": "address[]", "name": "", "type": "address[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_gauge", "type": "address" } ], "name": "getGauge", "outputs": [ { "components": [ { "internalType": "bool", "name": "active", "type": "bool" }, { "internalType": "uint256", "name": "created", "type": "uint256" }, { "internalType": "string", "name": "metadataURI", "type": "string" } ], "internalType": "struct IGauge.Gauge", "name": "", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "implementation", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_dao", "type": "address" }, { "internalType": "address", "name": "_escrow", "type": "address" }, { "internalType": "bool", "name": "_startPaused", "type": "bool" }, { "internalType": "address", "name": "_clock", "type": "address" } ], "name": "initialize", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_gauge", "type": "address" } ], "name": "isActive", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_tokenId", "type": "uint256" } ], "name": "isVoting", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "pause", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "paused", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "pluginType", "outputs": [ { "internalType": "enum IPlugin.PluginType", "name": "", "type": "uint8" } ], "stateMutability": "pure", "type": "function" }, { "inputs": [], "name": "proxiableUUID", "outputs": [ { "internalType": "bytes32", "name": "", "type": "bytes32" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_tokenId", "type": "uint256" } ], "name": "reset", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "bytes4", "name": "_interfaceId", "type": "bytes4" } ], "name": "supportsInterface", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "totalVotingPowerCast", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "unpause", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_gauge", "type": "address" }, { "internalType": "string", "name": "_metadataURI", "type": "string" } ], "name": "updateGaugeMetadata", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newImplementation", "type": "address" } ], "name": "upgradeTo", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newImplementation", "type": "address" }, { "internalType": "bytes", "name": "data", "type": "bytes" } ], "name": "upgradeToAndCall", "outputs": [], "stateMutability": "payable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_tokenId", "type": "uint256" } ], "name": "usedVotingPower", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_tokenId", "type": "uint256" }, { "components": [ { "internalType": "uint256", "name": "weight", "type": "uint256" }, { "internalType": "address", "name": "gauge", "type": "address" } ], "internalType": "struct IGaugeVote.GaugeVote[]", "name": "_votes", "type": "tuple[]" } ], "name": "vote", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256[]", "name": "_tokenIds", "type": "uint256[]" }, { "components": [ { "internalType": "uint256", "name": "weight", "type": "uint256" }, { "internalType": "address", "name": "gauge", "type": "address" } ], "internalType": "struct IGaugeVote.GaugeVote[]", "name": "_votes", "type": "tuple[]" } ], "name": "voteMultiple", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_tokenId", "type": "uint256" }, { "internalType": "address", "name": "_gauge", "type": "address" } ], "name": "votes", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "votingActive", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" } ]
        
        return abi

    # # will make our web3 token contract object for our given token_address
    def get_contract(self):
        abi = self.get_abi()
        contract_address = self.contract_address

        web3 = self.web3

        contract = lph.get_contract(contract_address, abi, web3)
        time.sleep(self.WAIT_TIME)

        return contract
    
    # # gets the last block we have scanned
    def get_last_block_checked(self, df):

        df['block_number'] = df['block_number'].astype(float)

        last_checked_block_number = df['block_number'].max()

        return last_checked_block_number
    
    # # gets the bare minimum block we want to start our scanning from
    def get_contract_from_block(self, df):
        
        from_block = self.from_block

        last_block_checked = self.get_last_block_checked(df)

        if last_block_checked > from_block:
            from_block = last_block_checked
            from_block = from_block - self.interval

        from_block = int(from_block)
        
        return from_block

    # # returns any events that may have occured in the specified block range
    def get_events(self, from_block, to_block):
        
        from_block = int(from_block)
        to_block = int(to_block)
        
        events = self.contract.events.Voted.get_logs(fromBlock=from_block, toBlock=to_block)

        return events

    # # makes a boilerplate dummy data df
    def make_default_df(self):

        df = pd.DataFrame()
        
        df['voter_address'] = ['0x0000000000000000000000000000000000000000']
        df['tx_hash'] = ['0x0000000000000000000000000000000000000000']
        df['timestamp'] = [1776]
        df['gauge_address'] = ['0x0000000000000000000000000000000000000000']
        df['epoch'] = [0]
        df['token_id'] = [0]
        df['voting_power_cast'] = [0]
        df['total_gauge_voting_power'] = [0]
        df['total_contract_voting_power'] = [0]
        df['block_number'] = [1776]

        return df
    
    # # will load our cloud df information into the sql database
    def insert_bulk_data_into_table(self, df, table_name):

        query = f"""
        INSERT INTO {table_name} (voter_address, tx_hash, timestamp, gauge_address, epoch, token_id, voting_power_cast, total_gauge_voting_power, total_contract_voting_power, block_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        sql.write_to_custom_table(query, df)

        return
    
    # # creates our local oToken table
    def create_event_table(self, cloud_df):
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}(
                voter_address TEXT,
                tx_hash TEXT,
                timestamp TEXT,
                gauge_address TEXT,
                epoch TEXT,
                token_id TEXT, 
                voting_power_cast TEXT,
                total_gauge_voting_power TEXT,
                total_contract_voting_power TEXT,
                block_number TEXT
                )
            """

        # # will only insert data into the sql table if the table doesn't exist
        sql.create_custom_table(query)
        
        db_df = sql.get_vote_df(self.table_name)

        # # makes a combined local and cloud dataframe and drops any duplicates from the dataframe
        # # drops our old database table
        # # then writes the updated sanitized dataframe to our local database table
        combined_df = pd.concat([db_df, cloud_df])
        combined_df = combined_df.drop_duplicates(subset=self.duplicate_column_list)
        
        sql.drop_table(self.table_name)
        sql.create_custom_table(query)
        self.insert_bulk_data_into_table(combined_df, self.table_name)

        return combined_df
    
    # # will return a boolean on whether our event already exists or not
    def event_already_exists(self, event):

        column_list = self.duplicate_column_list

        borrower_address = event['args']['_borrower']
        tx_hash = str(event['transactionHash'].hex())
        collateral_address = event['args']['_collateral']
        collateral_amount = event['args']['_coll']
        debt_amount = event['args']['_debt']

        value_list = [borrower_address, tx_hash, collateral_address, collateral_amount, debt_amount]

        exists = sql.sql_multiple_values_exist(value_list, column_list, self.table_name)

        return exists
    
    # # will process our events
    def process_events(self, events):

        df = pd.DataFrame()
        # self.column_list = ['voter', 'gauge', 'epoch', 'tokenId', 'votingPowerCastForGauge', 'totalVotingPowerInGauge', 'totalVotingPowerInContract', 'timestamp', 'transactionHash', 'blockNumber']
        
        voter_list = []
        gauge_list = []
        epoch_list = []
        token_id_list = []
        voting_power_cast_for_gauge_list = []
        total_voting_power_in_gauge_list = []
        total_voting_power_in_contract_list = []
        timestamp_list = []
        tx_hash_list = []
        block_number_list = []

        i = 1

        for event in events:
            i += 1

            # event_exists = self.event_already_exists(event)
                
            voter_address = event['args']['voter']
            gauge_address = event['args']['gauge']
            epoch = event['args']['epoch']
            token_id = event['args']['tokenId']
            voting_power_cast_for_gauge = event['args']['votingPowerCastForGauge']
            total_voting_power_in_gauge = event['args']['totalVotingPowerInGauge']
            total_voting_power_in_contract = event['args']['totalVotingPowerInContract']
            timestamp = event['args']['timestamp']
            tx_hash = str(event['transactionHash'].hex())

            
            try:
                block = self.web3.eth.get_block(event['blockNumber'])
                block_number = int(block['number'])
            except:
                block_number = int(event['blockNumber'])

            voter_list.append(voter_address)
            gauge_list.append(gauge_address)
            epoch_list.append(epoch)
            token_id_list.append(token_id)
            voting_power_cast_for_gauge_list.append(voting_power_cast_for_gauge)
            total_voting_power_in_gauge_list.append(total_voting_power_in_gauge)
            total_voting_power_in_contract_list.append(total_voting_power_in_contract)
            timestamp_list.append(timestamp)
            tx_hash_list.append(tx_hash)
            block_number_list.append(block_number)


            time.sleep(self.wait_time)
        
        if len(tx_hash_list) > 0:

    
            df['voter_address'] = voter_list
            df['tx_hash'] = tx_hash_list
            df['timestamp'] = timestamp_list
            df['gauge_address'] = gauge_list
            df['epoch'] = epoch_list
            df['token_id'] = token_id_list
            df['voting_power_cast'] = voting_power_cast_for_gauge_list
            df['total_gauge_voting_power'] = total_voting_power_in_gauge_list
            df['total_contract_voting_power'] = total_voting_power_in_contract_list
            df['block_number'] = block_number_list

        return df
    
    # # will track all of our cdp_events
    def run_all_event_tracking(self):
    
        try:
            cloud_df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)
            cloud_df.drop_duplicates(subset=self.duplicate_column_list)
        except:
            cloud_df = self.make_default_df()

        df = self.create_event_table(cloud_df)
        # from_block = self.get_contract_from_block(df)

        # from_block = 14405023

        from_block = 14405023

        latest_block = lph.get_latest_block(self.web3)

        interval = self.interval
        wait_time = self.wait_time

        to_block = from_block + interval

        while to_block < latest_block:
            
            events = self.get_events(from_block, to_block)
            
            time.sleep(wait_time)

            if len(events) > 0:
                print('Events to be processed:', len(events))
                event_df = self.process_events(events)
                print('events processed')

            else:
                event_df = pd.DataFrame()

            # # if our  df exists, it means we will write to our database
            if len(event_df) > 0:
                sql.write_to_db(event_df, self.column_list, self.table_name)

            from_block += interval
            to_block += interval

            if from_block >= latest_block:
                from_block = latest_block - 1
            
            if to_block >= latest_block:
                to_block = latest_block
            
            print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)
            time.sleep(wait_time)
        
        df = sql.get_vote_df(self.table_name)
        df = df.drop_duplicates(subset=self.duplicate_column_list)
        cs.df_write_to_cloud_storage_as_zip(df, self.cloud_file_name, self.cloud_bucket_name)
        
        return

# # returns the absolute latest vote per user
# def get_users_last_vote():
#     df = sql.get_vote_df('ironclad_vote_events')
    
#     df[['timestamp', 'voting_power_cast']] = df[['timestamp', 'voting_power_cast']].astype(float)
#     df['epoch'] = df['epoch'].astype(int)

#     # # voting epoch
#     df = df.loc[df['epoch'] == 1430]

#     # Get indices of rows with max timestamp for each voter_address
#     max_timestamp_idx = df.groupby('voter_address')['timestamp'].idxmax()
    
#     # Return those rows from the original dataframe
#     latest_votes_df = df.loc[max_timestamp_idx]

#     return latest_votes_df

# # will go through every use with a token_id that has ever voted for ICL
# # and will return a df with those who have active votes to the ICL gauge

    def get_contract_user_votes(self):
        try:
            # df = sql.get_vote_df('ironclad_vote_events')

            df = cs.read_zip_csv_from_cloud_storage('ironclad_vote_events.zip', 'cooldowns2')
            df['vote_source'] = 'veMode'
        except:
            print('ironclad_vote_events failed')
            df = pd.DataFrame()
        
        try:
            # df_2 = sql.get_vote_df('ironclad_bpt_vote_events')
            df_2 = cs.read_zip_csv_from_cloud_storage('ironclad_bpt_vote_events.zip', 'cooldowns2')
            df_2['vote_source'] = 'bpt'
        except:
            print('ironclad_bpt_vote_events failed')
            df_2 = pd.DataFrame()

        ve_mode_contract_address = '0x71439Ae82068E19ea90e4F506c74936aE170Cf58'
        ve_bpt_contract_address = '0x2aA8A5C1Af4EA11A1f1F10f3b73cfB30419F77Fb'

        contract_address_list = [ve_mode_contract_address, ve_bpt_contract_address]

        df_list = [df, df_2]

        processed_df_list = []

        i = 0

        while i < len(df_list):

            df = df_list[i]
            contract_address = contract_address_list[i]

            vote_contract = lph.get_contract(contract_address, self.get_abi(), lph.get_web_3(self.rpc_url))

            # # filters our dataframe down to only those who have ever voted for ICL
            df = df.loc[df['gauge_address'] == ICL_GAUGE_ADDRESS]

            # # gets each unique voter who has ever voted for ICL
            unique_user_list = df['voter_address'].unique()

            # # makes an easy to use primary key for our voter_vote_source_token_id combo
            df['primary_key'] = df['voter_address'].astype(str) + '_' + df['vote_source'].astype(str) + '_' + df['token_id'].astype(str)
            
            unique_primary_keys = df['primary_key'].unique()

            voter_address_list = []
            token_id_list = []
            vote_cast_amount_list = []
            total_epoch_gauge_vote_list = []

            z = 1

            for unique_user in unique_user_list:
                # # filter df to our unique user
                user_df = df.loc[df['voter_address'] == unique_user]
                
                # # filter df to user rows that have voted for the ICL gauge
                user_df = user_df.loc[user_df['gauge_address'] == ICL_GAUGE_ADDRESS]

                user_df['token_id'] = user_df['token_id'].astype(int)

                unique_token_id_list = user_df['token_id'].unique()

                for unique_token_id in unique_token_id_list:
                    # debt_token_address = self.contract.functions.lusdToken().call()
                    user_vote_amount = vote_contract.functions.votes(int(unique_token_id), Web3.to_checksum_address(ICL_GAUGE_ADDRESS)).call()
                    print(z, '/' , len(unique_primary_keys), user_vote_amount)

                    if user_vote_amount > 0:
                        voter_address_list.append(unique_user)
                        token_id_list.append(unique_token_id)
                        vote_cast_amount_list.append(user_vote_amount)

                    time.sleep(self.WAIT_TIME)

                    z += 1

            # # total gauge votes
            total_gauge_votes = vote_contract.functions.gaugeVotes(Web3.to_checksum_address(ICL_GAUGE_ADDRESS)).call()
            total_gauge_votes = int(total_gauge_votes)

            processed_df = pd.DataFrame()
            processed_df['voter_address'] = voter_address_list
            processed_df['token_id'] = token_id_list
            processed_df['vote_source'] = df['vote_source'].unique()[0]
            processed_df['vote_cast_amount'] = vote_cast_amount_list
            processed_df['total_gauge_votes'] = total_gauge_votes
            
            processed_df_list.append(processed_df)
            i += 1
        
        processed_df = pd.concat(processed_df_list)

        return processed_df





def get_users_last_vote():
    try:
        # df = sql.get_vote_df('ironclad_vote_events')

        df = cs.read_zip_csv_from_cloud_storage('ironclad_vote_events.zip', 'cooldowns2')
        df['vote_source'] = 'veMode'
    except:
        print('ironclad_vote_events failed')
        df = pd.DataFrame()
    
    try:
        # df_2 = sql.get_vote_df('ironclad_bpt_vote_events')
        df_2 = cs.read_zip_csv_from_cloud_storage('ironclad_bpt_vote_events.zip', 'cooldowns2')
        df_2['vote_source'] = 'bpt'
    except:
        print('ironclad_bpt_vote_events failed')
        df_2 = pd.DataFrame()

    df_list = [df, df_2]

    # # will host the respective dataframes after they have been processed
    processed_df_list = []

    # temp_df = pd.concat(df_list)
    # temp_df.to_csv('test_test.csv', index=False)
    # # finds the last vote for each df source
    for df in df_list:
        if len(df) > 0:
            # Convert datatypes
            df[['timestamp', 'voting_power_cast']] = df[['timestamp', 'voting_power_cast']].astype(float)
            df[['epoch', 'token_id', 'block_number']] = df[['epoch', 'token_id', 'block_number']].astype(int)
            
            # Remove exact duplicates first
            df = df.drop_duplicates(subset=['voter_address', 'tx_hash', 'gauge_address', 'epoch', 'token_id', 'voting_power_cast'])
            
            # # makes a unique key to track votes
            df['primary_key'] = df['voter_address'].astype(str) + '_' + df['vote_source'].astype(str) + '_' + df['token_id'].astype(str)

            primary_key_list = df['primary_key'].unique()

            for primary_key in primary_key_list:
                # # makes a temp dataframe for each token_id
                temp_df = df.loc[df['primary_key'] == primary_key]

                # # finds the last block_number the primary_key voted on
                max_pk_block_number = temp_df['block_number'].max()

                temp_df = temp_df.loc[temp_df['block_number'] == max_pk_block_number]
                
                # # we filter our df down to only contain values with the ICL gauge
                temp_df = temp_df.loc[temp_df['gauge_address'].str.lower() == '0x969904a7e77381a89Ae2BeE4c4C7d1C10e3563F8'.lower()]

                checker_df = df.loc[df['primary_key'] == primary_key]
                check_max_block_number = checker_df['block_number'].max()

                if max_pk_block_number != check_max_block_number:
                    print('block_number_mismatch')
                    print('temp: ', max_pk_block_number, '/', check_max_block_number, ' total_token_id_max')

                # # ensures we only add data if it contains our gauge vote
                if len(temp_df) > 0:
                    
                    # # weights the votes. 80% for veMODe and 20% for MODE bpt
                    if temp_df['vote_source'].iloc[0] == 'veMode':
                        temp_df['voting_power_cast'] *= 1
                    
                    else:
                        temp_df['voting_power_cast'] *= 1

                    # # adds our last_vote rowed df to our list
                    processed_df_list.append(temp_df)
    
    # Combine all processed dataframes
    final_df = pd.concat(processed_df_list)
    
    return final_df

def get_user_ironclad_votes():

    df = get_users_last_vote()

    df = df.drop_duplicates(subset=['voter_address', 'tx_hash', 'token_id', 'voting_power_cast', 'block_number'])

    df = df.loc[df['voter_address'] != '0x969904a7e77381a89Ae2BeE4c4C7d1C10e3563F8']

    # # ICL gauge
    # df = df.loc[df['gauge_address'] == '0x969904a7e77381a89Ae2BeE4c4C7d1C10e3563F8']

    # total_vote_power_cast = df['voting_power_cast'].sum()

    # df['total_epoch_votes'] = total_vote_power_cast

    # df['voter_share_of_total'] = df['voting_power_cast'] / df['total_epoch_votes']

    return df