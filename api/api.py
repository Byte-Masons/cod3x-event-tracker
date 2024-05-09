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

def get_user_token_balance(user_address, token_address):
    df = pd.read_csv('./test/test.csv')

    df = df.loc[df['user_address'] == user_address]
    df = df.loc[df['token_address'] == token_address]

    token_balance_list = df['amount_cumulative'].tolist()

    user_token_balance = token_balance_list[0]

    return user_token_balance

#reads from csv for quest4
@app.route("/user_token_balance/", methods=["POST"])
def get_api_response():

    user_address = '0x0000000040307cA0AEb9AdaF516fc7028528FB58'
    token_address = '0xe5415Fa763489C813694D7A79d133F0A7363310C'
    response = get_user_token_balance(user_address, token_address)
    
    return jsonify(response), 200

# user_address = '0x0000000040307cA0AEb9AdaF516fc7028528FB58'
# token_address = '0xe5415Fa763489C813694D7A79d133F0A7363310C'

# user_token_balance = get_user_token_balance(user_address, token_address)
# print('User Address: ', user_address)
# print('Token Address: ', token_address)
# print('Token Balance: ', user_token_balance)


if __name__ =='__main__':
    app.run()