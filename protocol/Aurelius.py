import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking, total_revenue, cdp_mint_fee_revenue_tracking
from cdp import CDP

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Aurelius(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0xedB4f24e4b74a6B1e20e2EAf70806EAC19E1FA54'
    RPC_URL = 'https://rpc.mantle.xyz'
    TREASURY_ADDRESS = '0xCE4975E63b6e737c41C9c0e5aCd248Ef0145B51A'
    INDEX = 'aurelius'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 500
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x039BcB43cE3e5ef9Bf555a30e3b74a7719c46499'
    BORROWER_OPERATIONS_ADDRESS = '0x4Cd23F2C694F991029B85af5575D0B5E70e4A3F1'
    CDP_FROM_BLOCK = 52092639

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
        self.cdp_object = CDP.CDP(self.BORROWER_OPERATIONS_ADDRESS, self.CDP_FROM_BLOCK, self.RPC_URL, self.WAIT_TIME, self.INTERVAL, self.INDEX)
        self.cdp_revenue_object = cdp_mint_fee_revenue_tracking.cdp_mint_fee_revenue_tracking(self.INDEX)

    def run_all_modules(self):
        self.run_all_lend_event_tracking()
        self.lend_revenue_object.run_all_lend_revenue()
        self.cdp_object.run_all_cdp_tracking()
        self.cdp_revenue_object.run_all_cdp_revenue()
        total_revenue.run_all()
        return