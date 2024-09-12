import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking, total_revenue, cdp_mint_fee_revenue_tracking
from cdp import CDP
from helper_classes import oToken, Rewarder

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Harbor(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0xFa0AC9b741F0868B2a8C4a6001811a5153019818'
    RPC_URL = 'https://binance.llamarpc.com'
    TREASURY_ADDRESS = '0xEa46EEa22dd001da738735573AD3AD9a02aFF56b'
    INDEX = 'harbor'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 500
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x47a677a5E49B71BE074049EBb358021E6c82b2d7'

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
        # self.lend_revenue_object = cod3x_lend_revenue_tracking.cod3x_lend_revenue_tracking(self.PROTOCOL_DATA_PROVIDER_ADDRESS, self.TREASURY_ADDRESS, self.RPC_URL, self.INDEX)
    
    def get_updated_reward_balances(self):
        lending_pool_rewarder = Rewarder.Rewarder(self.RPC_URL, '0x071c626C75248E4F672bAb8c21c089166F49B615', 'lending_pool', self.WAIT_TIME, self.INDEX)
        oHbr_staking_rewarder = Rewarder.Rewarder(self.RPC_URL, '0xF512283347C174399Cc3E11492ead8b49BD2712e', 'reliquary_mrp_token', self.WAIT_TIME, self.INDEX)
        wbnb_staking_rewarder = Rewarder.Rewarder(self.RPC_URL, '0xC1e5A6a11Cf39C295e1371072529e955d98Ddb55', 'reliquary_other_token', self.WAIT_TIME, self.INDEX)
        # stability_pool_rewarder = Rewarder.Rewarder(self.RPC_URL, '0x22BF901406757D3702105b74fED437358B58cC61', 'stability_pool', self.WAIT_TIME, self.INDEX)
        discount_exercise_rewarder = Rewarder.Rewarder(self.RPC_URL, '0x3Fbf4f9cf73162e4e156972540f53Dabe65c2862', 'discount_exercise', self.WAIT_TIME, self.INDEX)

        rewarder_list = [lending_pool_rewarder, oHbr_staking_rewarder, wbnb_staking_rewarder, discount_exercise_rewarder]

        for rewarder in rewarder_list:
            rewarder.run_all()

        return rewarder_list
    
    def run_all_modules(self):

        function_calls = [
        self.get_updated_reward_balances
        ]

        for func in function_calls:
            try:
                func()
            except Exception as e:
                print(f"Error occurred in {func.__name__}: {str(e)}")

        # self.get_updated_reward_balances()

        return