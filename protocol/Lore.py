import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph, User_Balance
from revenue_tracking import cod3x_lend_revenue_tracking, total_revenue, o_token_revenue_tracking, cdp_mint_fee_revenue_tracking
from helper_classes import oToken, Rewarder

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Lore(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0xb17844F6E50f4eE8f8FeC7d9BA200B0E034b8236'
    RPC_URL = 'https://rpc.scroll.io'
    TREASURY_ADDRESS = '0x5F272Ee6348BDE137D9a6c640c42DDcF0dE3D0aA'
    INDEX = 'lore'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 50
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x204f5ccC7b5217B8477C8FA45708144FB0a61831'
    CONTRACT_BLACKLIST = ['0x0000000000000000000000000000000000000000', '0x4cE1A1eC13DBd9084B1A741b036c061b2d58dABf', '0x9df4Ac62F9E435DbCD85E06c990a7f0ea32739a9', '0x5F272Ee6348BDE137D9a6c640c42DDcF0dE3D0aA']
    EXERCISE_ADDRESS = '0x9e864C08564506AfDA9A584B5388907b1dD67FAa'
    FROM_BLOCK = 8584681

    def __init__(self):
        self.protocol_data_provider_address = self.PROTOCOL_DATA_PROVIDER_ADDRESS
        self.gateway_address = self.GATEWAY_ADDRESS
        self.treasury_address = self.TREASURY_ADDRESS
        self.rpc_url = self.RPC_URL
        self.wait_time = self.WAIT_TIME
        self.interval = self.INTERVAL
        self.index = self.get_event_index(self.INDEX)
        self.web3 = lph.get_web_3(self.rpc_url)
        self.cloud_file_name = self.get_cloud_filename()
        self.cloud_bucket_name = self.CLOUD_BUCKET_NAME
        self.table_name = self.index
        self.lend_revenue_object = cod3x_lend_revenue_tracking.cod3x_lend_revenue_tracking(self.PROTOCOL_DATA_PROVIDER_ADDRESS, self.TREASURY_ADDRESS, self.RPC_URL, self.INDEX)
        self.user_balancer = User_Balance.User_Balance('lore', 'lend', '0x80E0Fb6B416E1Ae9bBD02A9bC6A6D10C9E9D51B7', 'weeth', 18, '0x9df4Ac62F9E435DbCD85E06c990a7f0ea32739a9', self.CONTRACT_BLACKLIST)
        self.user_balancer_2 = User_Balance.User_Balance('lore', 'lend', '0xc28A5a35e98bCaC257440A4759B0E7Da3b35Ed69', 'wrseth', 18, '0x9df4Ac62F9E435DbCD85E06c990a7f0ea32739a9', self.CONTRACT_BLACKLIST)
        self.o_token_object = oToken.oToken(self.EXERCISE_ADDRESS, self.FROM_BLOCK, self.RPC_URL, self.WAIT_TIME, self.INTERVAL, self.INDEX)
        self.o_token_revenue_object = o_token_revenue_tracking.o_token_revenue_tracking(self.INDEX)
    
    def get_updated_reward_balances(self):
        lending_pool_rewarder = Rewarder.Rewarder(self.RPC_URL, '0x3E45df33Adf1b81E7B45cA468E8e41496a66c837', 'lending_pool', self.WAIT_TIME, self.INDEX)
        oLore_staking_rewarder = Rewarder.Rewarder(self.RPC_URL, '0xCfcD1A9221434642b221273949361E768431EE13', 'reliquary_mrp_token', self.WAIT_TIME, self.INDEX)
        weth_staking_rewarder = Rewarder.Rewarder(self.RPC_URL, '0xa756519222eC4f81a9DCFE736b8B03a837f366bc', 'reliquary_other_token', self.WAIT_TIME, self.INDEX)
        # stability_pool_rewarder = Rewarder.Rewarder(self.RPC_URL, '0xb42c356CA0d364Bb4130b12221533693AbFD81C8', 'stability_pool', self.WAIT_TIME, self.INDEX)
        discount_exercise_rewarder = Rewarder.Rewarder(self.RPC_URL, '0x9e864C08564506AfDA9A584B5388907b1dD67FAa', 'discount_exercise', self.WAIT_TIME, self.INDEX)

        rewarder_list = [lending_pool_rewarder, oLore_staking_rewarder, weth_staking_rewarder, discount_exercise_rewarder]

        for rewarder in rewarder_list:
            rewarder.run_all()

        return rewarder_list
    
    def run_all_modules(self):

        function_calls = [
        self.run_all_lend_event_tracking,
        self.lend_revenue_object.run_all_lend_revenue,
        self.o_token_object.run_all_o_token_tracking,
        self.o_token_revenue_object.run_all_o_token_revenue,
        total_revenue.run_all,
        # # weETH balance updater
        self.user_balancer.run_all,
        self.user_balancer_2.run_all,
        self.get_updated_reward_balances
        ]

        for func in function_calls:
            try:
                func()
            except Exception as e:
                print(f"Error occurred in {func.__name__}: {str(e)}")

        return 