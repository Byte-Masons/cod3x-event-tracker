import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking, total_revenue

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Fantom(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0x9D9f273373aa5759820ea4e59b90AC74c34EA79c'
    RPC_URL = 'https://rpc.ftm.tools'
    TREASURY_ADDRESS = '0x9017Ba3e59cF30A3762Ae45Ed616718F942f734c'
    INDEX = 'fantom_lz'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 5000
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x725c820Ef301C4B66F21A2Be19c655B34082dDBC'

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