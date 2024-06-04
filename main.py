import pandas as pd
from lending_pool import lp_tracker
from lending_pool import balance_and_points as bp
from sql_interfacer import sql
from cloud_storage import cloud_storage as cs
# from api import api
from flask import Flask, request, jsonify
import json
from web3 import Web3
import time
import queue
from concurrent.futures import ThreadPoolExecutor
import threading
import sqlite3
from lending_pool import approval_tracker
from lending_pool import lending_pool_helper as lph
from lending_pool import current_balance_tracker as cbt

app = Flask(__name__)

# # makes our json response for a users total tvl and embers
def get_user_tvl_and_embers(user_address):

    data = []
    index = 0
    
    start_time = time.time()
    df = cs.read_from_cloud_storage('current_user_tvl_embers.csv', 'cooldowns2')
    print('Finished Reading Data in: ' + str(time.time() - start_time))

    user_address = Web3.to_checksum_address(user_address)
    df = df[(df['to_address'].str.contains(user_address)) | (df['from_address'].str.contains(user_address))]
    
    if len(df) > 0:
        df = df.drop_duplicates(subset=['tx_hash','log_index', 'transaction_index', 'token_address', 'token_volume'], keep='last')
        df = bp.set_single_user_stats(df, user_address, index)
    #if we have an address with no transactions
    if len(df) < 1:
        data.append({
           "user_address": user_address,
            "user_tvl": 0,
            "user_total_embers": 0
        })
    
    else:
        total_tvl = df['amount_cumulative'].sum()
        total_embers = df['total_ember_balance'].median()
        # # downrates embers a bit
        total_embers = total_embers * 0.65

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

# # makes our nested response
def make_nested_response(df):

    data = []

    #if we have an address with no transactions
    if len(df) < 1:
        temp_df = pd.DataFrame()
        data.append({
           "wallet_address": 'N/A',
        })

    else:
        # wallet_addresses = ", ".join(df["to_address"].tolist())
        # data.append({"wallet_address": wallet_addresses})
        temp_df = df[['to_address']]
        # Process data
        for i in range(temp_df.shape[0]):
            row = temp_df.iloc[i]
            data.append({
                "wallet_address": str(row['to_address']),
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
    
    # Threads
    with ThreadPoolExecutor() as executor:
        future = executor.submit(get_user_tvl_and_embers, user_address)
    
    response = future.result()

    return jsonify(response), 200

@app.route("/batch_users_tvl_and_embers/", methods=["POST"])
def get_batch_users_tvl_and_embers_response():

    data = request.get_json()

    # Check if data is present and a list
    if not data or not isinstance(data, list):
        return jsonify({"error": "Invalid request format. Please provide a list of user addresses."}), 400
    
    else:
        user_addresses = data

    response_list = []
    for user_address in user_addresses:
        # Threads (optional)
        with ThreadPoolExecutor() as executor:
            future = executor.submit(get_user_tvl_and_embers, user_address)
            response_list.append(future.result())  # Append individual responses

    return jsonify(response_list), 200

# # will be an endpoint we can ping to try to keep our website online for quicker response times
@app.route("/keep_online/", methods=["GET"])
def get_filler_response():

    user_address = '0x2dDD3BCA2Fa050532B8d7Fd41fB1449382187dAA'

    # Threads
    with ThreadPoolExecutor() as executor:
        future = executor.submit(get_user_tvl_and_embers, user_address)
    
    response = future.result()

    return jsonify(response), 200

# # will be an endpoint we can ping to try to keep our website online for quicker response times
@app.route("/get_all_users/", methods=["GET"])
def get_all_users():

    df = cs.read_from_cloud_storage('current_user_tvl_embers.csv', 'cooldowns2')
    df = bp.set_unique_users_no_database(df)
    df = df[['to_address', 'log_index']]

    # Threads
    with ThreadPoolExecutor() as executor:
        future = executor.submit(make_nested_response, df)
    
    response = future.result()

    return jsonify(response), 200

# df = cbt.get_user_token_combos()

# df = cbt.find_all_token_balances(df, 0)


# df = cbt.add_token_metadata(0)

# df = sql.get_transaction_data_df('persons')

df = bp.set_embers_database_v2(0)

print(df)

# rows = sql.select_star('current_balance')

# print(rows)