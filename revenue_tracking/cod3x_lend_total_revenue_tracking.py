import pandas as pd
from lending_pool import lending_pool_helper as lph
from cloud_storage import cloud_storage as cs
from helper_classes import ERC_20, Protocol_Data_Provider
from sql_interfacer import sql
import sys
import os
import time

protocol_df = lph.get_lp_config_df()

protocol_list = protocol_df['index'].unique()

# # gets a list of our actively tracked protocol deployments
def get_protocol_revenue_filename_list():
    df = lph.get_lp_config_df()
    
    df = df[df['index'].str.contains('lend', case=False, na=False)]

    protocol_list = df['index'].unique()

    # # reformats our list to their revenue file names
    protocol_list = [protocol.split('_events')[0] for protocol in protocol_list]
    protocol_list = [protocol + '_revenue.zip' for protocol in protocol_list]

    return protocol_list

# # returns a list of dataframes for each of our protocols
def get_individual_revenue_df_list(protocol_revenue_list):

    # # just gets the deployments name without any suffixes
    deployment_name_list = [protocol.split('_')[0] for protocol in protocol_revenue_list]

    df_list = []

    i = 0
    while i < len(protocol_revenue_list):
        revenue_cloud_name = protocol_revenue_list[i]
        revenue_deployment_name = deployment_name_list[i]

        df = cs.read_zip_csv_from_cloud_storage(revenue_cloud_name, 'cooldowns2')
        df['deployment'] = revenue_deployment_name

        df_list.append(df)

        time.sleep(0.1)
        i += 1
    
    df = pd.concat(df_list)

    return df