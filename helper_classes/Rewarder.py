import sys
import os
import time
import pandas as pd
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import lending_pool_helper as lph
from helper_classes import ERC_20
from sql_interfacer import sql
from cloud_storage import cloud_storage as cs

class Rewarder(ERC_20.ERC_20):

    def __init__(self, rpc_url: str, rewarder_address:str, rewarder_type:str, wait_time:float, index: str):
        self.rpc_url = rpc_url
        self.index = index
        self.rewarder_address = rewarder_address
        self.rewarder_type = rewarder_type
        self.wait_time = wait_time

        self.cloud_file_name = 'rewarder.zip'
        self.cloud_bucket_name = 'cooldowns2'

    

        web3 = lph.get_web_3(rpc_url)
        self.web3 = web3
    
    # # adds onto our index for o_token_events
    def get_event_index(self, index):
        index = index + '_o_token_events'
        
        return index
    
    # # makes our revenue cloud filename
    def get_cloud_filename(self):
        cloud_filename = self.index + '.zip'

        return cloud_filename
    
    # # lending pool supply and borrow emissions
    def get_lending_pool_rewarder_abi(self):
        contract_abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"asset","type":"address"},{"indexed":True,"internalType":"address","name":"reward","type":"address"},{"indexed":False,"internalType":"uint256","name":"emission","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"distributionEnd","type":"uint256"}],"name":"AssetConfigUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"asset","type":"address"},{"indexed":True,"internalType":"address","name":"reward","type":"address"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"AssetIndexUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"claimer","type":"address"}],"name":"ClaimerSet","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":True,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"reward","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"RewardsAccrued","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"reward","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"address","name":"claimer","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"RewardsClaimed","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"vault","type":"address"}],"name":"RewardsVaultUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"asset","type":"address"},{"indexed":True,"internalType":"address","name":"reward","type":"address"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"UserIndexUpdated","type":"event"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address","name":"to","type":"address"}],"name":"claimAllRewards","outputs":[{"internalType":"address[]","name":"rewardTokens","type":"address[]"},{"internalType":"uint256[]","name":"claimedAmounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"to","type":"address"}],"name":"claimAllRewardsOnBehalf","outputs":[{"internalType":"address[]","name":"rewardTokens","type":"address[]"},{"internalType":"uint256[]","name":"claimedAmounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"}],"name":"claimAllRewardsToSelf","outputs":[{"internalType":"address[]","name":"rewardTokens","type":"address[]"},{"internalType":"uint256[]","name":"claimedAmounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"name":"claimRewards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"name":"claimRewardsOnBehalf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"reward","type":"address"}],"name":"claimRewardsToSelf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint88","name":"emissionPerSecond","type":"uint88"},{"internalType":"uint256","name":"totalSupply","type":"uint256"},{"internalType":"uint32","name":"distributionEnd","type":"uint32"},{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"internalType":"struct DistributionTypes.RewardsConfigInput[]","name":"config","type":"tuple[]"}],"name":"configureAssets","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address","name":"user","type":"address"}],"name":"getAllUserRewardsBalance","outputs":[{"internalType":"address[]","name":"rewardTokens","type":"address[]"},{"internalType":"uint256[]","name":"unclaimedAmounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getAssetDecimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getClaimer","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"name":"getDistributionEnd","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getRewardTokens","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getRewardsByAsset","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"name":"getRewardsData","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"reward","type":"address"}],"name":"getRewardsVault","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"name":"getUserAssetData","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"name":"getUserRewardsBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"name":"getUserUnclaimedRewardsFromStorage","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"totalSupply","type":"uint256"},{"internalType":"uint256","name":"userBalance","type":"uint256"}],"name":"handleAction","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"caller","type":"address"}],"name":"setClaimer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"reward","type":"address"},{"internalType":"uint32","name":"distributionEnd","type":"uint32"}],"name":"setDistributionEnd","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"vault","type":"address"},{"internalType":"address","name":"reward","type":"address"}],"name":"setRewardsVault","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}]
        return contract_abi

    # # borrow_lend_rewards_vault abi
    def get_rewards_vault_abi(self):

        contract_abi = [ { "inputs": [ { "internalType": "address", "name": "incentivesController", "type": "address" }, { "internalType": "address", "name": "rewardToken", "type": "address" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "previousOwner", "type": "address" }, { "indexed": True, "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "OwnershipTransferred", "type": "event" }, { "inputs": [], "name": "INCENTIVES_CONTROLLER", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "REWARD_TOKEN", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "value", "type": "uint256" } ], "name": "approveIncentivesController", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "emergencyEtherTransfer", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "token", "type": "address" }, { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "emergencyTokenTransfer", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "owner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "renounceOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "transferOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" } ]

        return contract_abi
    
    # Lending Rewarder:
    # Staking Pool Rewarder:
    # Lp Staking Rolling Rewarder:
    # Reliquary oHBR contract: 


    def get_lending_pool_rewarder_contract(self):
        contract = lph.get_contract(self.rewarder_address, self.get_lending_pool_rewarder_abi(), self.web3)

        return contract
    
    # # gets the reward token(s) for this kind of rewarder
    def get_lending_pool_reward_token_addresses(self, contract):
        reward_token_address_list = contract.functions.getRewardTokens().call()
        
        return reward_token_address_list
    
    # # gets the vault contract address
    def get_lending_pool_rewards_vault_address(self, contract, token_address):
        
        reward_token_address_list = contract.functions.getRewardsVault(token_address).call()
        
        return reward_token_address_list
    
    def get_rewarder_config_df(self):
        df = pd.DataFrame()

        protocol_list = ['ironclad']
        rewarder_type_list = ['lending_pool']
        block_explorer_list = ['https://explorer.mode.network/address/']

        df['protocol'] = protocol_list
        df['rewarder_type'] = rewarder_type_list
        df['block_explorer'] = block_explorer_list

        return df
    
    def get_config_df_value(self, column_name, protocol, rewarder_type):

        config_df = self.get_rewarder_config_df()

        config_df = config_df.loc[(config_df['protocol'] == protocol) & (config_df['rewarder_type'] == rewarder_type)]

        value = config_df[column_name].tolist()[0]

        return value

    # # will make our rewarder block explorer link
    def get_contract_block_explorer_link(self, protocol, rewarder_type, rewarder_address):

        link = self.get_config_df_value('block_explorer', protocol, rewarder_type)

        link = link + rewarder_address

        return link

    # # will read our existing rewarder cloud file then will update it with our latest rewarder data
    def update_rewarder_cloud_file(self, df):

        try:
            cloud_df = cs.read_zip_csv_from_cloud_storage(self.cloud_file_name, self.cloud_bucket_name)
        except:
            cloud_df = df
        

        protocol_list = df['protocol'].tolist()
        rewarder_type_list = df['rewarder_type'].tolist()


        for protocol in protocol_list:
            for rewarder_type in rewarder_type_list:
                last_checked_timestamp = df.loc[(df['protocol'] == protocol) & (df['rewarder_type'] == rewarder_type)]['timestamp'].tolist()[0]
                rewarder_balance = df.loc[(df['protocol'] == protocol) & (df['rewarder_type'] == rewarder_type)]['rewarder_balance'].tolist()[0]

                cloud_df.loc[(cloud_df['protocol'] == protocol) & (cloud_df['rewarder_type'] == rewarder_type), 'timestamp'] = last_checked_timestamp
                cloud_df.loc[(cloud_df['protocol'] == protocol) & (cloud_df['rewarder_type'] == rewarder_type), 'rewarder_balance'] = rewarder_balance
        
        return cloud_df

    # # will return the contract associated with our specific rewarder type
    def contract_type_setup(self):

        contract = ''

        if self.rewarder_type == 'lending_pool':
            contract = self.get_lending_pool_rewarder_contract()

        return contract
    
    # # will return a list of our reward_tokens for our specified rewarder contract
    def reward_token_list_type_setup(self, contract):
        
        reward_token_list = []

        if self.rewarder_type == 'lending_pool':
            reward_token_list = self.get_lending_pool_reward_token_addresses(contract)

        return reward_token_list
    
    # # gets the address of our rewards vault
    def get_vault_address(self, contract, reward_token):

        vault_address = ''

        if self.rewarder_type == 'lending_pool':
            vault_address = self.get_lending_pool_rewards_vault_address(contract, reward_token)

        return vault_address
    
    def run_all(self):

        contract = self.contract_type_setup()
        time.sleep(self.wait_time)
        reward_token_list = self.reward_token_list_type_setup(contract)

        time.sleep(self.wait_time)

        vault_address_list = []
        vault_reward_balance_list = []
        link_list = []

        for reward_token in reward_token_list:
            vault_address = self.get_vault_address(contract, reward_token)
            vault_address_list.append(vault_address)
            time.sleep(self.wait_time)
            reward_token_object = lph.get_contract(reward_token, ERC_20.ERC_20.get_erc_20_abi_no_self(), self.web3)
            vault_reward_balance = reward_token_object.functions.balanceOf(vault_address).call()
            time.sleep(self.wait_time)
            vault_token_decimals = reward_token_object.functions.decimals().call()
            time.sleep(self.wait_time)
            vault_token_decimals = 10 ** vault_token_decimals

            vault_reward_balance /= vault_token_decimals
            vault_reward_balance_list.append(vault_reward_balance)

            link = self.get_contract_block_explorer_link(self.index, self.rewarder_type, vault_address)
            link_list.append(link)

        # Get current UTC time
        utc_now = datetime.now(timezone.utc)

        # Format it as a string
        readable_utc = utc_now.strftime("%Y-%m-%d %H:%M:%S UTC")

        df = pd.DataFrame()

        df['protocol'] = [self.index]
        df['rewarder_type'] = [self.rewarder_type]
        df['rewarder_address'] = vault_address_list
        df['rewarder_balance'] = vault_reward_balance_list
        df['timestamp'] = readable_utc
        df['link'] = link_list

        cloud_df = self.update_rewarder_cloud_file(df)
        cs.df_write_to_cloud_storage_as_zip(cloud_df, self.cloud_file_name, self.cloud_bucket_name)
        
        return cloud_df
    