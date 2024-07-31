import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class o_token_revenue_tracking():
    
    def __init__(self, index: str):
        
        self.index = index
        self.cloud_file_name = index + '.zip'

        # # makes a more concise name for our cod3x lend revenue file
        revenue_cloud_filename = index.split('_')
        contains_number = lph.number_in_string(index)
        if contains_number == True:
            revenue_cloud_filename = revenue_cloud_filename[0] + '_' + revenue_cloud_filename[1] + '_revenue_' + revenue_cloud_filename[-1] + '.zip'
        else:
            revenue_cloud_filename = revenue_cloud_filename[0] + '_' + revenue_cloud_filename[1] + '_revenue.zip'

        self.revenue_cloud_file_name = revenue_cloud_filename

        self.cloud_bucket_name = 'cooldowns2'
        self.table_name = self.index