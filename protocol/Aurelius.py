import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Aurelius(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0xedB4f24e4b74a6B1e20e2EAf70806EAC19E1FA54'
    RPC_URL = 'https://rpc.mantle.xyz'
    TREASURY_ADDRESS = '0xCE4975E63b6e737c41C9c0e5aCd248Ef0145B51A'
    INDEX = 'aurelius_lend_events'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 500
    WAIT_TIME = 0.6
    GATEWAY_ADDRESS = '0x039BcB43cE3e5ef9Bf555a30e3b74a7719c46499'

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