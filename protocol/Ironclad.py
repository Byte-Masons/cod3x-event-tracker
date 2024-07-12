import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Ironclad(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0x29563f73De731Ae555093deb795ba4D1E584e42E'
    RPC_URL = 'wss://mode.drpc.org'
    TREASURY_ADDRESS = '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF'
    INDEX = 'ironclad_lend_events'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 250
    WAIT_TIME = 0.6

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