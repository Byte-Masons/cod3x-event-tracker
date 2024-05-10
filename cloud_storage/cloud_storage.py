from flask import Flask, request, jsonify
from web3 import Web3
from web3.middleware import geth_poa_middleware
import pandas as pd
import json
from functools import cache
import threading 
import queue
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
# import gcs_updater
from google.cloud import storage
import google.cloud.storage
import os
import sys
import io
from io import BytesIO

PATH = os.path.join(os.getcwd(), 'fast-web-419215-35d284e06546.json')
STORAGE_CLIENT = storage.Client(PATH)

def read_from_cloud_storage(filename, bucketname):
    storage_client = storage.Client(PATH)
    bucket = storage_client.get_bucket(bucketname)

    df = pd.read_csv(
    io.BytesIO(
                 bucket.blob(blob_name = filename).download_as_string() 
              ) ,
                 encoding='UTF-8',
                 sep=',')
    
    return df

# # writes our dataframe to our desired filename
def df_write_to_cloud_storage(df, filename, bucketname):

    # storage_client = storage.Client(PATH)
    bucket = STORAGE_CLIENT.get_bucket(bucketname)

    csv_string = df.to_csv(index=False)  # Omit index for cleaner output
    blob = bucket.blob(filename)
    blob.upload_from_string(csv_string)

    return