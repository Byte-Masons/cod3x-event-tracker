import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Arbitrum(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0x96bCFB86F1bFf315c13e00D850e2FAeA93CcD3e7'
    RPC_URL = 'wss://arbitrum-one-rpc.publicnode.com'
    TREASURY_ADDRESS = '0xb17844F6E50f4eE8f8FeC7d9BA200B0E034b8236'
    INDEX = 'arbitrum_lend_events'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 5000
    WAIT_TIME = 0.8

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