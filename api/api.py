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

app = Flask(__name__)

# # makes our json response for a users total tvl and embers
def get_user_tvl_and_embers(user_address):

    data = []

    df = pd.read_csv('test.csv')

    df['user_address'] = df['user_address'].str.lower()
    user_address = user_address.lower()

    df = df.loc[df['user_address'] == user_address]

    #if we have an address with no transactions
    if len(df) < 1:
        data.append({
           "user_address": user_address,
            "user_tvl": 0,
            "user_total_embers": 0
        })
    
    else:
        total_tvl = df['amount_cumulative'].sum()
        total_embers = df['total_ember_balance'].iloc[0]

        data.append({
           "user_address": user_address,
            "user_tvl": total_tvl,
            "user_total_embers": total_embers
        })

    # Create JSON response
    response = {
        "data": {
            "result": data
        }
    }
    
    return response

@app.route("/user_tvl_and_embers/", methods=["POST"])
def get_api_response():

    data = json.loads(request.data)

    user_address = data['user_address']  # Assuming data is in form format

    response = get_user_tvl_and_embers(user_address)
    
    return jsonify(response), 200

# user_address = '0x0000000040307cA0AEb9AdaF516fc7028528FB58'
# token_address = '0xe5415Fa763489C813694D7A79d133F0A7363310C'

# user_token_balance = get_user_token_balance(user_address, token_address)
# print('User Address: ', user_address)
# print('Token Address: ', token_address)
# print('Token Balance: ', user_token_balance)


if __name__ =='__main__':
    app.run()