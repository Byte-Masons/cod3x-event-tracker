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
from lending_pool import treasury_tracker as tt

# def run_all():
#     index_list = [0]

#     for index in index_list:
#         try:
#             cbt.loop_through_current_balances(index)
#         except:
#             pass
        
#         try:
#             lp_tracker.run_all(index_list)
#         except:
#             time.sleep(60)
    
#     run_all()

# run_all()

def run_all_treasury():
    index_list = [0]

    for index in index_list:
        try:
            tt.find_all_revenue_transactions(index)
        except:
            pass
    
    print('Run it back')
    time.sleep(250)

    run_all_treasury()

run_all_treasury()