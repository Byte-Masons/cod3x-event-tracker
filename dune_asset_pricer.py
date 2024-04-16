from requests import get, post
import pandas as pd
import time
import discord
import os
import asyncio
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

API_KEY = ''

HEADER = {"x-dune-api-key": API_KEY}

BASE_URL = "https://api.dune.com/api/v1/"

PATH = os.path.join(os.getcwd(), 'ethos-redemption-bot-d4d4be30b664.json')
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
STORAGE_CLIENT = storage.Client(PATH)

# # gets our needed query_id
def get_query_id(index):

    query_id_list = ["3549527"]

    query_id = query_id_list[index]

    return query_id

def make_api_url(module, action, ID):
  """
    We shall use this function to generate a URL to call the API.
    """

  url = BASE_URL + module + "/" + ID + "/" + action

  return url


def execute_query(query_id, engine="medium"):
  """
    Takes in the query ID and engine size.
    Specifying the engine size will change how quickly your query runs. 
    The default is "medium" which spends 10 credits, while "large" spends 20 credits.
    Calls the API to execute the query.
    Returns the execution ID of the instance which is executing the query.
    """

  url = make_api_url("query", "execute", query_id)
  params = {
      "performance": engine,
  }
  response = post(url, headers=HEADER, params=params)
  #print(response)
  execution_id = response.json()['execution_id']

  return execution_id


def get_query_status(execution_id):
  """
    Takes in an execution ID.
    Fetches the status of query execution using the API
    Returns the status response object
    """

  url = make_api_url("execution", "status", execution_id)
  response = get(url, headers=HEADER)

  return response


def get_query_results(execution_id):
  """
    Takes in an execution ID.
    Fetches the results returned from the query using the API
    Returns the results response object
    """

  url = make_api_url("execution", "results", execution_id)
  response = get(url, headers=HEADER)

  return response


def cancel_query_execution(execution_id):
  """
    Takes in an execution ID.
    Cancels the ongoing execution of the query.
    Returns the response object.
    """

  url = make_api_url("execution", "cancel", execution_id)
  response = get(url, headers=HEADER)

  return response


#loops through our query untill it is completed
async def get_populated_results(response, execution_id):
  state = response.json()['state']

  while state != 'QUERY_STATE_COMPLETED':
    print('Waiting on Query Completion: ' + state)
    await asyncio.sleep(15)
    #gets our updated response

    response = get_query_results(execution_id)
    state = response.json()['state']

    #adds some time if our query needs time to wait before executing
    if state == 'QUERY_STATE_PENDING':
      await asyncio.sleep(120)
      state = response.json(['state'])
    #if our query has an issue then we cancel the query. Sleep. and we run everything again
    if state != 'QUERY_STATE_COMPLETED' and state != 'QUERY_STATE_EXECUTING':
      cancel_query_execution(execution_id)
      print('Query cancelled and trying again later')

    if state == 'QUERY_STATE_COMPLETED':
      print(state)
      break

  data = pd.DataFrame(response.json()['result']['rows'])

  return data

# # reads a csv from our GCP
# # filename = name of the file we want
def read_from_cloud_storage(filename):
    
    storage_client = storage.Client(PATH)
    bucket = storage_client.get_bucket('cooldowns')

    df = pd.read_csv(
    io.BytesIO(
                 bucket.blob(blob_name = filename).download_as_string() 
              ) ,
                 encoding='UTF-8',
                 sep=',')
    # 1st read setup
    if filename == 'liquidations.csv':
      # try to read user_borrowed column and if doesn't exist make it and set it to 0
      try:
          df = df[['Last_Liquidation_Sent','Current_Liquidation_Number','Network','current_time']]
      except:
        df = df
    
    # debt_repaid_per_trove_owner_usd,ern_debt_repaid,number_of_redeemed_tokens,profit_or_loss_usd,redeemed_collateral_per_trove_owner_usd,redeemed_token,redemption_fee_percent,redemption_fee_tokens,redemption_fee_usd,redemption_number,timestamp,transaction,trove_owner,trove_owner_minimal,trove_owner_super_minimal,tx_hash_minimal,total_aggregate_profit_or_loss,total_aggregate_collateral_redeemed,total_aggregate_debt_repaid,total_aggregate_ern_repaid,total_aggregate_redemption_fee,configuration,config_redemption_number

    # other read setup
    elif filename == 'test.csv':
      try:
          df = df[['debt_repaid_per_trove_owner_usd', 'ern_debt_repaid', 'number_of_redeemed_tokens', 'profit_or_loss_usd', 'redeemed_collateral_per_trove_owner_usd', 'redeemed_token', 'redemption_fee_percent', 'redemption_fee_tokens', 'redemption_fee_usd', 'redemption_number', 'timestamp', 'transaction', 'trove_owner', 'trove_owner_minimal', 'trove_owner_super_minimal', 'tx_hash_minimal', 'total_aggregate_profit_or_loss', 'total_aggregate_collateral_redeemed', 'total_aggregate_debt_repaid', 'total_aggregate_ern_repaid', 'total_aggregate_redemption_fee', 'configuration', 'config_redemption_number']]
      except:
          print('test csv failed')
    
    return df

def df_write_to_cloud_storage(df, filename):

    # storage_client = storage.Client(PATH)
    bucket = STORAGE_CLIENT.get_bucket('cooldowns')

    csv_string = df.to_csv(index=False)  # Omit index for cleaner output
    # print(csv_string)
    blob = bucket.blob(filename)
    blob.upload_from_string(csv_string)
    # print('')

    return

#takes in a query_id and gets our query results in a dataframe
async def query_extractor(query_id):

  #gets our execution ID
  execution_id = execute_query(query_id, "medium")

  #makes our dataframe when our data is ready
  response = get_query_status(execution_id)

  response = get_query_results(execution_id)

  data = await get_populated_results(response, execution_id)

  if len(data) < 1:
    #
    data = pd.DataFrame()
    data['minute'] = ['-1']
    data['contract_address'] = [0]
    data['symbol'] = [0]
    data['price'] = [0]

  #sorts our values for our cumulative sum of liquidation profits over days
  data = data.sort_values(by='minute', ascending=True)
  data = data.reset_index(drop=True)

  return data


index = 0

query_id = get_query_id(index)
data = asyncio.run(query_extractor(query_id))
print(data)