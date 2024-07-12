import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Transaction_Labeler(Protocol_Data_Provider.Protocol_Data_Provider):
    
    def __init__(self, protocol_data_provider_address: str, rpc_url: str, df: pd.DataFrame):
        
        self.protocol_data_provider_address = protocol_data_provider_address
        self.rpc_url = rpc_url
        self.web3 = lph.get_web_3(rpc_url)
        self.abi = self.get_protocol_data_provider_abi()
        self.lending_pool_address = self.get_lending_pool_address()
    
    def label_deposit_events(self, df):

        return