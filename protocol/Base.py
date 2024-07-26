import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Base(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0xed984A0E9c12Ee27602314191Fc4487A702bB83f'
    RPC_URL = 'https://mainnet.base.org'
    TREASURY_ADDRESS = '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF'
    INDEX = 'base_lend_events'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 5000
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x29563f73De731Ae555093deb795ba4D1E584e42E'

    def __init__(self):
        self.protocol_data_provider_address = self.PROTOCOL_DATA_PROVIDER_ADDRESS
        self.gateway_address = self.GATEWAY_ADDRESS
        self.treasury_address = self.TREASURY_ADDRESS
        self.rpc_url = self.RPC_URL
        self.wait_time = self.WAIT_TIME
        self.interval = self.INTERVAL
        self.index = self.INDEX
        self.web3 = lph.get_web_3(self.rpc_url)
        self.cloud_file_name = self.index + '.zip'
        self.cloud_bucket_name = self.CLOUD_BUCKET_NAME
        self.table_name = self.index
    
    def run_all_modules(self):
        self.run_all_lend_event_tracking()
        lend_revenue = cod3x_lend_revenue_tracking.cod3x_lend_revenue_tracking(self.PROTOCOL_DATA_PROVIDER_ADDRESS, self.TREASURY_ADDRESS, self.RPC_URL, self.INDEX)
        lend_revenue.run_all_lend_revenue()
        return