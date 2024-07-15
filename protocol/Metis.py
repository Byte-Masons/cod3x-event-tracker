import sys
import os
import time
import pandas as pd
import sqlite3
from lending_pool import Lending_Pool, lending_pool_helper as lph

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Metis(Lending_Pool.Lending_Pool):

    PROTOCOL_DATA_PROVIDER_ADDRESS = '0x10615D451a5b91C92ce8538703E7AABA5d5cCC4D'
    RPC_URL = 'https://metis.drpc.org'
    TREASURY_ADDRESS = '0xd2abC5d7841d49C40Fd35A1Ec832ee1daCC8D339'
    INDEX = 'metis_lend_events'
    CLOUD_BUCKET_NAME = 'cooldowns2'
    INTERVAL = 500
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