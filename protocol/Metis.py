import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking, total_revenue

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Metis(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0x10615D451a5b91C92ce8538703E7AABA5d5cCC4D'
    RPC_URL = 'https://andromeda.metis.io/?owner=1088'
    TREASURY_ADDRESS = '0xd2abC5d7841d49C40Fd35A1Ec832ee1daCC8D339'
    INDEX = 'metis'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 500
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x4d8d90FAF90405b9743Ce600E98A2Aa8CdF579a0'

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
    
    def run_all_modules(self):

        function_calls = [
        self.run_all_lend_event_tracking,
        self.lend_revenue_object.run_all_lend_revenue,
        total_revenue.run_all
        ]

        for func in function_calls:
            try:
                func()
            except Exception as e:
                print(f"Error occurred in {func.__name__}: {str(e)}")
                
        return