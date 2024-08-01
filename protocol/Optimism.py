import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking, cod3x_lend_total_revenue_tracking as cdx_total

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Optimism(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0x9546F673eF71Ff666ae66d01Fd6E7C6Dae5a9995'
    RPC_URL = 'https://optimism.llamarpc.com'
    TREASURY_ADDRESS = '0xC01a7AD7Fb8a085a3cc16be8eaA10302c78a1783'
    INDEX = 'optimism_2'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 2500
    WAIT_TIME = 1.25
    GATEWAY_ADDRESS = '0x6e20E155819f0ee08d1291b0b9889b0e011b8224'

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
        self.run_all_lend_event_tracking()
        self.lend_revenue_object.run_all_lend_revenue()
        cdx_total.run_all()
        return