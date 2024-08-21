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

# Lending Rewarder:
# Staking Pool Rewarder:
# Lp Staking Rolling Rewarder:
# Reliquary oHBR contract: 

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
    
    def get_reliquary_abi(self):
        
        contract_abi = [ { "inputs": [ { "internalType": "address", "name": "_rewardToken", "type": "address" }, { "internalType": "uint256", "name": "_emissionRate", "type": "uint256" }, { "internalType": "address", "name": "_gaugeRewardReceiver", "type": "address" }, { "internalType": "address", "name": "_voter", "type": "address" }, { "internalType": "string", "name": "_name", "type": "string" }, { "internalType": "string", "name": "_symbol", "type": "string" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "inputs": [], "name": "AccessControlBadConfirmation", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "account", "type": "address" }, { "internalType": "bytes32", "name": "neededRole", "type": "bytes32" } ], "name": "AccessControlUnauthorizedAccount", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "target", "type": "address" } ], "name": "AddressEmptyCode", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "account", "type": "address" } ], "name": "AddressInsufficientBalance", "type": "error" }, { "inputs": [], "name": "ERC721EnumerableForbiddenBatchMint", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "sender", "type": "address" }, { "internalType": "uint256", "name": "tokenId", "type": "uint256" }, { "internalType": "address", "name": "owner", "type": "address" } ], "name": "ERC721IncorrectOwner", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "operator", "type": "address" }, { "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "ERC721InsufficientApproval", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "approver", "type": "address" } ], "name": "ERC721InvalidApprover", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "operator", "type": "address" } ], "name": "ERC721InvalidOperator", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "owner", "type": "address" } ], "name": "ERC721InvalidOwner", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "receiver", "type": "address" } ], "name": "ERC721InvalidReceiver", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "sender", "type": "address" } ], "name": "ERC721InvalidSender", "type": "error" }, { "inputs": [ { "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "ERC721NonexistentToken", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "owner", "type": "address" }, { "internalType": "uint256", "name": "index", "type": "uint256" } ], "name": "ERC721OutOfBoundsIndex", "type": "error" }, { "inputs": [], "name": "FailedInnerCall", "type": "error" }, { "inputs": [], "name": "MathOverflowedMulDiv", "type": "error" }, { "inputs": [], "name": "ReentrancyGuardReentrantCall", "type": "error" }, { "inputs": [], "name": "Reliquary__BURNING_PRINCIPAL", "type": "error" }, { "inputs": [], "name": "Reliquary__BURNING_REWARDS", "type": "error" }, { "inputs": [], "name": "Reliquary__CURVE_OVERFLOW", "type": "error" }, { "inputs": [], "name": "Reliquary__DUPLICATE_RELIC_IDS", "type": "error" }, { "inputs": [], "name": "Reliquary__GAUGE_NOT_ALIVE", "type": "error" }, { "inputs": [], "name": "Reliquary__MERGING_EMPTY_RELICS", "type": "error" }, { "inputs": [], "name": "Reliquary__MULTIPLIER_AT_LEVEL_ZERO_SHOULD_BE_GT_ZERO", "type": "error" }, { "inputs": [], "name": "Reliquary__NON_EXISTENT_POOL", "type": "error" }, { "inputs": [], "name": "Reliquary__NOT_APPROVED_OR_OWNER", "type": "error" }, { "inputs": [], "name": "Reliquary__NOT_OWNER", "type": "error" }, { "inputs": [], "name": "Reliquary__PARTIAL_WITHDRAWALS_DISABLED", "type": "error" }, { "inputs": [], "name": "Reliquary__PAUSED", "type": "error" }, { "inputs": [], "name": "Reliquary__RELICS_NOT_OF_SAME_POOL", "type": "error" }, { "inputs": [], "name": "Reliquary__REWARD_PRECISION_ISSUE", "type": "error" }, { "inputs": [], "name": "Reliquary__REWARD_TOKEN_AS_POOL_TOKEN", "type": "error" }, { "inputs": [], "name": "Reliquary__TOKEN_NOT_COMPATIBLE", "type": "error" }, { "inputs": [], "name": "Reliquary__ZERO_INPUT", "type": "error" }, { "inputs": [], "name": "Reliquary__ZERO_TOTAL_ALLOC_POINT", "type": "error" }, { "inputs": [ { "internalType": "uint8", "name": "bits", "type": "uint8" }, { "internalType": "uint256", "name": "value", "type": "uint256" } ], "name": "SafeCastOverflowedUintDowncast", "type": "error" }, { "inputs": [ { "internalType": "address", "name": "token", "type": "address" } ], "name": "SafeERC20FailedOperation", "type": "error" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "owner", "type": "address" }, { "indexed": True, "internalType": "address", "name": "approved", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "Approval", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "owner", "type": "address" }, { "indexed": True, "internalType": "address", "name": "operator", "type": "address" }, { "indexed": False, "internalType": "bool", "name": "approved", "type": "bool" } ], "name": "ApprovalForAll", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint8", "name": "pid", "type": "uint8" }, { "indexed": True, "internalType": "address", "name": "to", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "relicId", "type": "uint256" } ], "name": "CreateRelic", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint8", "name": "pid", "type": "uint8" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": True, "internalType": "address", "name": "to", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "relicId", "type": "uint256" } ], "name": "Deposit", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint8", "name": "pid", "type": "uint8" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": True, "internalType": "address", "name": "to", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "relicId", "type": "uint256" } ], "name": "EmergencyWithdraw", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint8", "name": "pid", "type": "uint8" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": True, "internalType": "address", "name": "to", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "relicId", "type": "uint256" } ], "name": "Harvest", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint8", "name": "pid", "type": "uint8" }, { "indexed": False, "internalType": "uint256", "name": "allocPoint", "type": "uint256" }, { "indexed": True, "internalType": "address", "name": "poolToken", "type": "address" }, { "indexed": True, "internalType": "address", "name": "rewarder", "type": "address" }, { "indexed": False, "internalType": "address", "name": "nftDescriptor", "type": "address" }, { "indexed": False, "internalType": "bool", "name": "allowPartialWithdrawals", "type": "bool" } ], "name": "LogPoolAddition", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint8", "name": "pid", "type": "uint8" }, { "indexed": False, "internalType": "uint256", "name": "allocPoint", "type": "uint256" }, { "indexed": True, "internalType": "address", "name": "rewarder", "type": "address" }, { "indexed": False, "internalType": "address", "name": "nftDescriptor", "type": "address" } ], "name": "LogPoolModified", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "emissionRate", "type": "uint256" } ], "name": "LogSetEmissionRate", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "bytes32", "name": "role", "type": "bytes32" }, { "indexed": True, "internalType": "bytes32", "name": "previousAdminRole", "type": "bytes32" }, { "indexed": True, "internalType": "bytes32", "name": "newAdminRole", "type": "bytes32" } ], "name": "RoleAdminChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "bytes32", "name": "role", "type": "bytes32" }, { "indexed": True, "internalType": "address", "name": "account", "type": "address" }, { "indexed": True, "internalType": "address", "name": "sender", "type": "address" } ], "name": "RoleGranted", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "bytes32", "name": "role", "type": "bytes32" }, { "indexed": True, "internalType": "address", "name": "account", "type": "address" }, { "indexed": True, "internalType": "address", "name": "sender", "type": "address" } ], "name": "RoleRevoked", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "fromId", "type": "uint256" }, { "indexed": True, "internalType": "uint256", "name": "toId", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Shift", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "fromId", "type": "uint256" }, { "indexed": True, "internalType": "uint256", "name": "toId", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Split", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "from", "type": "address" }, { "indexed": True, "internalType": "address", "name": "to", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "Transfer", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint8", "name": "pid", "type": "uint8" }, { "indexed": True, "internalType": "uint256", "name": "relicId", "type": "uint256" } ], "name": "Update", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint8", "name": "pid", "type": "uint8" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": True, "internalType": "address", "name": "to", "type": "address" }, { "indexed": True, "internalType": "uint256", "name": "relicId", "type": "uint256" } ], "name": "Withdraw", "type": "event" }, { "inputs": [], "name": "DEFAULT_ADMIN_ROLE", "outputs": [ { "internalType": "bytes32", "name": "", "type": "bytes32" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_allocPoint", "type": "uint256" }, { "internalType": "address", "name": "_poolToken", "type": "address" }, { "internalType": "address", "name": "_rewarder", "type": "address" }, { "internalType": "contract ICurves", "name": "_curve", "type": "address" }, { "internalType": "string", "name": "_name", "type": "string" }, { "internalType": "address", "name": "_nftDescriptor", "type": "address" }, { "internalType": "bool", "name": "_allowPartialWithdrawals", "type": "bool" }, { "internalType": "address", "name": "_to", "type": "address" } ], "name": "addPool", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "approve", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "owner", "type": "address" } ], "name": "balanceOf", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_relicId", "type": "uint256" } ], "name": "burn", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_pid", "type": "uint256" }, { "internalType": "address[]", "name": "_rewardTokens", "type": "address[]" } ], "name": "claimGaugeRewards", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_to", "type": "address" }, { "internalType": "uint8", "name": "_poolId", "type": "uint8" }, { "internalType": "uint256", "name": "_amount", "type": "uint256" } ], "name": "createRelicAndDeposit", "outputs": [ { "internalType": "uint256", "name": "relicId_", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_amount", "type": "uint256" }, { "internalType": "uint256", "name": "_relicId", "type": "uint256" }, { "internalType": "address", "name": "_harvestTo", "type": "address" } ], "name": "deposit", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_pid", "type": "uint256" }, { "internalType": "address[]", "name": "_claimRewardsTokens", "type": "address[]" } ], "name": "disableGauge", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_relicId", "type": "uint256" } ], "name": "emergencyWithdraw", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "emissionRate", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_pid", "type": "uint256" } ], "name": "enableGauge", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "gaugeRewardReceiver", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "getApproved", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint8", "name": "_poolId", "type": "uint8" } ], "name": "getPoolInfo", "outputs": [ { "components": [ { "internalType": "string", "name": "name", "type": "string" }, { "internalType": "uint256", "name": "accRewardPerShare", "type": "uint256" }, { "internalType": "uint256", "name": "totalLpSupplied", "type": "uint256" }, { "internalType": "address", "name": "nftDescriptor", "type": "address" }, { "internalType": "address", "name": "rewarder", "type": "address" }, { "internalType": "address", "name": "gauge", "type": "address" }, { "internalType": "address", "name": "poolToken", "type": "address" }, { "internalType": "uint40", "name": "lastRewardTime", "type": "uint40" }, { "internalType": "bool", "name": "allowPartialWithdrawals", "type": "bool" }, { "internalType": "uint96", "name": "allocPoint", "type": "uint96" }, { "internalType": "contract ICurves", "name": "curve", "type": "address" } ], "internalType": "struct PoolInfo", "name": "pool_", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_relicId", "type": "uint256" } ], "name": "getPositionForId", "outputs": [ { "components": [ { "internalType": "uint256", "name": "rewardDebt", "type": "uint256" }, { "internalType": "uint256", "name": "rewardCredit", "type": "uint256" }, { "internalType": "uint128", "name": "amount", "type": "uint128" }, { "internalType": "uint40", "name": "entry", "type": "uint40" }, { "internalType": "uint40", "name": "level", "type": "uint40" }, { "internalType": "uint8", "name": "poolId", "type": "uint8" } ], "internalType": "struct PositionInfo", "name": "position_", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "role", "type": "bytes32" } ], "name": "getRoleAdmin", "outputs": [ { "internalType": "bytes32", "name": "", "type": "bytes32" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "role", "type": "bytes32" }, { "internalType": "uint256", "name": "index", "type": "uint256" } ], "name": "getRoleMember", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "role", "type": "bytes32" } ], "name": "getRoleMemberCount", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint8", "name": "_poolId", "type": "uint8" } ], "name": "getTotalLpSupplied", "outputs": [ { "internalType": "uint256", "name": "lp_", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "role", "type": "bytes32" }, { "internalType": "address", "name": "account", "type": "address" } ], "name": "grantRole", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "role", "type": "bytes32" }, { "internalType": "address", "name": "account", "type": "address" } ], "name": "hasRole", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "owner", "type": "address" }, { "internalType": "address", "name": "operator", "type": "address" } ], "name": "isApprovedForAll", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_spender", "type": "address" }, { "internalType": "uint256", "name": "_relicId", "type": "uint256" } ], "name": "isApprovedOrOwner", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "massUpdatePools", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint8", "name": "_poolId", "type": "uint8" }, { "internalType": "uint256", "name": "_allocPoint", "type": "uint256" }, { "internalType": "address", "name": "_rewarder", "type": "address" }, { "internalType": "string", "name": "_name", "type": "string" }, { "internalType": "address", "name": "_nftDescriptor", "type": "address" }, { "internalType": "bool", "name": "_overwriteRewarder", "type": "bool" } ], "name": "modifyPool", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "bytes[]", "name": "data", "type": "bytes[]" } ], "name": "multicall", "outputs": [ { "internalType": "bytes[]", "name": "results", "type": "bytes[]" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "name", "outputs": [ { "internalType": "string", "name": "", "type": "string" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "ownerOf", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "pause", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_relicId", "type": "uint256" } ], "name": "pendingReward", "outputs": [ { "internalType": "uint256", "name": "pending_", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "poolLength", "outputs": [ { "internalType": "uint256", "name": "pools_", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "role", "type": "bytes32" }, { "internalType": "address", "name": "callerConfirmation", "type": "address" } ], "name": "renounceRole", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "role", "type": "bytes32" }, { "internalType": "address", "name": "account", "type": "address" } ], "name": "revokeRole", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "rewardToken", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "from", "type": "address" }, { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "safeTransferFrom", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "from", "type": "address" }, { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "uint256", "name": "tokenId", "type": "uint256" }, { "internalType": "bytes", "name": "data", "type": "bytes" } ], "name": "safeTransferFrom", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "operator", "type": "address" }, { "internalType": "bool", "name": "approved", "type": "bool" } ], "name": "setApprovalForAll", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_emissionRate", "type": "uint256" } ], "name": "setEmissionRate", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_gaugeRewardReceiver", "type": "address" } ], "name": "setGaugeReceiver", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_fromId", "type": "uint256" }, { "internalType": "uint256", "name": "_toId", "type": "uint256" }, { "internalType": "uint256", "name": "_amount", "type": "uint256" } ], "name": "shift", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_fromId", "type": "uint256" }, { "internalType": "uint256", "name": "_amount", "type": "uint256" }, { "internalType": "address", "name": "_to", "type": "address" } ], "name": "split", "outputs": [ { "internalType": "uint256", "name": "newId_", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "bytes4", "name": "_interfaceId", "type": "bytes4" } ], "name": "supportsInterface", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "symbol", "outputs": [ { "internalType": "string", "name": "", "type": "string" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "index", "type": "uint256" } ], "name": "tokenByIndex", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "owner", "type": "address" }, { "internalType": "uint256", "name": "index", "type": "uint256" } ], "name": "tokenOfOwnerByIndex", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_relicId", "type": "uint256" } ], "name": "tokenURI", "outputs": [ { "internalType": "string", "name": "", "type": "string" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "totalAllocPoint", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "totalSupply", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "from", "type": "address" }, { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "uint256", "name": "tokenId", "type": "uint256" } ], "name": "transferFrom", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "unpause", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_relicId", "type": "uint256" }, { "internalType": "address", "name": "_harvestTo", "type": "address" } ], "name": "update", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint8", "name": "_poolId", "type": "uint8" } ], "name": "updatePool", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_pid", "type": "uint256" } ], "name": "updatePoolWithGaugeDeposit", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "voter", "outputs": [ { "internalType": "contract IVoter", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_amount", "type": "uint256" }, { "internalType": "uint256", "name": "_relicId", "type": "uint256" }, { "internalType": "address", "name": "_harvestTo", "type": "address" } ], "name": "withdraw", "outputs": [], "stateMutability": "nonpayable", "type": "function" } ]
        
        return contract_abi
    
    # Lending Rewarder:
    # Staking Pool Rewarder:
    # Lp Staking Rolling Rewarder:
    # Reliquary oHBR contract: 


    def get_lending_pool_rewarder_contract(self):
        contract = lph.get_contract(self.rewarder_address, self.get_lending_pool_rewarder_abi(), self.web3)

        return contract
    
    def get_reliquary_contract(self):
        contract = lph.get_contract(self.rewarder_address, self.get_reliquary_abi(), self.web3)

        return contract
    
    # # gets the reward token(s) for this kind of rewarder
    def get_lending_pool_reward_token_addresses(self, contract):
        reward_token_address_list = contract.functions.getRewardTokens().call()
        
        return reward_token_address_list
    
    # # gets the reliquary staking token native token reward
    def get_reliquary_reward_address(self, contract):
        reward_token_address_list = [contract.functions.rewardToken().call()]
        
        return reward_token_address_list

    # # gets the vault contract address
    def get_lending_pool_rewards_vault_address(self, contract, token_address):
        
        reward_token_address_list = contract.functions.getRewardsVault(token_address).call()
        
        return reward_token_address_list
    
    def get_rewarder_config_df(self):
        df = pd.DataFrame()

        protocol_list = ['ironclad']
        block_explorer_list = ['https://explorer.mode.network/address/']

        df['protocol'] = protocol_list
        df['block_explorer'] = block_explorer_list

        return df
    
    def get_config_df_value(self, column_name, protocol):

        config_df = self.get_rewarder_config_df()

        config_df = config_df.loc[config_df['protocol'] == protocol]

        value = config_df[column_name].tolist()[0]

        return value

    # # will make our rewarder block explorer link
    def get_contract_block_explorer_link(self, protocol, rewarder_address):

        link = self.get_config_df_value('block_explorer', protocol)

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

                temp_df = cloud_df.loc[(cloud_df['protocol'] == protocol) & (cloud_df['rewarder_type'] == rewarder_type)]
                
                # # will update a protocol/rewarder_type if the pair already exists
                if len(temp_df) > 0:
                    cloud_df.loc[(cloud_df['protocol'] == protocol) & (cloud_df['rewarder_type'] == rewarder_type), 'timestamp'] = last_checked_timestamp
                    cloud_df.loc[(cloud_df['protocol'] == protocol) & (cloud_df['rewarder_type'] == rewarder_type), 'rewarder_balance'] = rewarder_balance
                
                # # will add our new protocol/rewarder_type combo to our cloud_df if it doesn't already exist
                else:
                    temp_df = df.loc[(df['protocol'] == protocol) & (df['rewarder_type'] == rewarder_type)]
                    cloud_df = pd.concat([cloud_df, temp_df])
                    cloud_df = cloud_df.drop_duplicates(subset=['protocol', 'rewarder_type'], keep='last')
        
        return cloud_df

    # # will return the contract associated with our specific rewarder type
    def contract_type_setup(self):

        contract = ''

        if self.rewarder_type == 'lending_pool':
            contract = self.get_lending_pool_rewarder_contract()

        elif self.rewarder_type == 'reliquary_mrp_token' or self.rewarder_type == 'reliquary_other_token':
            contract = self.get_reliquary_contract()

        return contract
    
    # # will return a list of our reward_tokens for our specified rewarder contract
    def reward_token_list_type_setup(self, contract):
        
        reward_token_list = []

        if self.rewarder_type == 'lending_pool':
            reward_token_list = self.get_lending_pool_reward_token_addresses(contract)
        
        elif self.rewarder_type == 'reliquary_mrp_token' or self.rewarder_type == 'reliquary_other_token':
            reward_token_list = self.get_reliquary_reward_address(contract)

        return reward_token_list
    
    # # gets the address of our rewards vault
    def get_vault_address(self, contract, reward_token):

        vault_address = ''

        if self.rewarder_type == 'lending_pool':
            vault_address = self.get_lending_pool_rewards_vault_address(contract, reward_token)

        elif self.rewarder_type == 'reliquary_mrp_token' or self.rewarder_type == 'reliquary_other_token':
            vault_address = self.rewarder_address
            
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

            link = self.get_contract_block_explorer_link(self.index, vault_address)
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
    