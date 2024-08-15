import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph, User_Balance
from revenue_tracking import cod3x_lend_revenue_tracking, total_revenue

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Lore(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0xb17844F6E50f4eE8f8FeC7d9BA200B0E034b8236'
    RPC_URL = 'https://rpc.scroll.io'
    TREASURY_ADDRESS = '0x5F272Ee6348BDE137D9a6c640c42DDcF0dE3D0aA'
    INDEX = 'lore'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 500
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x204f5ccC7b5217B8477C8FA45708144FB0a61831'
    CONTRACT_BLACKLIST = ['0x0000000000000000000000000000000000000000', '0x4cE1A1eC13DBd9084B1A741b036c061b2d58dABf', '0x9df4Ac62F9E435DbCD85E06c990a7f0ea32739a9', '0x5F272Ee6348BDE137D9a6c640c42DDcF0dE3D0aA']

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
        self.user_balancer = User_Balance.User_Balance('lore', 'lend', '0x19624e5e4aD3F8de2ab516C67645Bb5B79EcfFE6', 'weeth', 6, '0x9df4Ac62F9E435DbCD85E06c990a7f0ea32739a9', self.CONTRACT_BLACKLIST)

    def run_all_modules(self):
        self.run_all_lend_event_tracking()
        self.lend_revenue_object.run_all_lend_revenue()
        total_revenue.run_all()

        # # weETH balance updater
        self.user_balancer.run_all()

        return