import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking, total_revenue, o_token_revenue_tracking, cdp_mint_fee_revenue_tracking
from helper_classes import oToken, Rewarder
from cdp import CDP

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Ironclad(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0x29563f73De731Ae555093deb795ba4D1E584e42E'
    RPC_URL = 'https://mainnet.mode.network'
    TREASURY_ADDRESS = '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF'
    INDEX = 'ironclad_2'
    # INDEX = 'ironclad'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 500
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x6387c7193B5563DD17d659b9398ACd7b03FF0080'
    EXERCISE_ADDRESS = '0xcb727532e24dFe22E74D3892b998f5e915676Da8'
    FROM_BLOCK = 10257616
    BORROWER_OPERATIONS_ADDRESS = '0x2d1b857F459ca527991f574A5CB2cfF2763088f2'
    CDP_FROM_BLOCK = 11202864

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
        self.o_token_object = oToken.oToken(self.EXERCISE_ADDRESS, self.FROM_BLOCK, self.RPC_URL, self.WAIT_TIME, self.INTERVAL, self.INDEX)
        self.o_token_revenue_object = o_token_revenue_tracking.o_token_revenue_tracking(self.INDEX)
        self.cdp_object = CDP.CDP(self.BORROWER_OPERATIONS_ADDRESS, self.CDP_FROM_BLOCK, self.RPC_URL, self.WAIT_TIME, self.INTERVAL, self.INDEX)
        self.cdp_revenue_object = cdp_mint_fee_revenue_tracking.cdp_mint_fee_revenue_tracking(self.INDEX)
    

    def get_updated_reward_balances(self):
        lending_pool_rewarder = Rewarder.Rewarder('https://mainnet.mode.network', '0xC043BA54F34C9fb3a0B45d22e2Ef1f171272Bc9D', 'lending_pool', 0.6, 'ironclad')
        oICL_staking_rewarder = Rewarder.Rewarder('https://mainnet.mode.network', '0x1ED3903e792Ff2a3d6A86a9B7930843364bA20E5', 'reliquary_mrp_token', 0.6, 'ironclad')
        weth_staking_rewarder = Rewarder.Rewarder('https://mainnet.mode.network', '0xb08d7643C5fB22fD5B819ca3302b3F89c751ADdf', 'reliquary_other_token', 0.6, 'ironclad')
        stability_pool_rewarder = Rewarder.Rewarder('https://mainnet.mode.network', '0x0490FeCFa551c233264570E80DE5D41273EDD86D', 'stability_pool', 0.6, 'ironclad')

        rewarder_list = [lending_pool_rewarder, oICL_staking_rewarder, weth_staking_rewarder, stability_pool_rewarder]

        for rewarder in rewarder_list:
            rewarder.run_all()

        return rewarder_list
    
    def run_all_modules(self):
        self.run_all_lend_event_tracking()
        self.lend_revenue_object.run_all_lend_revenue()
        self.o_token_object.run_all_o_token_tracking()
        self.o_token_revenue_object.run_all_o_token_revenue()
        self.cdp_object.run_all_cdp_tracking()
        self.cdp_revenue_object.run_all_cdp_revenue()
        total_revenue.run_all()
        self.get_updated_reward_balances()
        return