import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import Cod3x_Lend_Revenue_Tracking

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Ironclad(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0x29563f73De731Ae555093deb795ba4D1E584e42E'
    RPC_URL = 'https://mainnet.mode.network'
    TREASURY_ADDRESS = '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF'
    INDEX = 'ironclad_lend_events'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 250
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x6387c7193B5563DD17d659b9398ACd7b03FF0080'

    def __init__(self):
        self.protocol_data_provider_address = self.PROTOCOL_DATA_PROVIDER_ADDRESS
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
        lend_revenue = Cod3x_Lend_Revenue_Tracking.Cod3x_Lend_Revenue_Tracking(self.PROTOCOL_DATA_PROVIDER_ADDRESS, self.TREASURY_ADDRESS, self.RPC_URL, self.INDEX)
        lend_revenue.run_all_lend_revenue()
        return