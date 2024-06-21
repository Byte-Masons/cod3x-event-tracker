import pandas as pd
from cloud_storage import cloud_storage as cs
from lending_pool import lending_pool_helper as lph
from revenue_tracking import cod3x_lend_revenue_tracking as cod3x
import csv
from notion_client import Client
from time import sleep
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_DATABASE_ID = os.getenv('NOTION_DATABASE_ID')

headers = {
    "Authorization": "Bearer " + NOTION_API_KEY,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

def get_pages(num_pages=None):
    """
    If num_pages is None, get all pages, otherwise just the defined number.
    """
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"

    get_all = num_pages is None
    page_size = 100 if get_all else num_pages

    payload = {"page_size": page_size}
    response = requests.post(url, json=payload, headers=headers)

    data = response.json()

    # Comment this out to dump all data to a file
    # import json
    # with open('db.json', 'w', encoding='utf8') as f:
    #    json.dump(data, f, ensure_ascii=False, indent=4)

    results = data["results"]
    while data["has_more"] and get_all:
        payload = {"page_size": page_size, "start_cursor": data["next_cursor"]}
        url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
        response = requests.post(url, json=payload, headers=headers)
        data = response.json()
        results.extend(data["results"])

    return results

def update_page(page_id: str, data: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"

    payload = {"properties": data}

    res = requests.patch(url, json=payload, headers=headers)
    return res

# index = 2
# cloud_filename = cod3x.get_revenue_by_day_cloud_name(index)
# cloud_bucket_name = lph.get_lp_config_value('cloud_bucket_name', index)
# df = cs.read_from_cloud_storage('metis', cloud_bucket_name)
# df = df[:1]
# df['day'] = pd.to_datetime(df['day'], format='%Y-%m-%d')
# df[['total_rolling_balance', 'daily_revenue']] = df[['total_rolling_balance', 'daily_revenue']].astype(float)

# treasury_address = df['treasury_address'].tolist()[0]
# day = df['day'].tolist()[0]
# balance = df['total_rolling_balance'].tolist()[0]
# daily_revenue = df['daily_revenue'].tolist()[0]

# data = {
#     "treasury_address": {"title": [{"text": {"content": treasury_address}}]},
#     "day": {"date": {"start": day, "end": None}},
#     "balance": {"number": balance},
#     "daily_revenue": {"number": daily_revenue}
# }

def create_database_entry(data: dict):
    create_url = "https://api.notion.com/v1/pages"

    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": data}

    res = requests.post(create_url, headers=headers, json=payload)
    # print(res.status_code)
    return res

# create_database_entry(data)


# # Initialize the Notion client
# notion = Client(auth=NOTION_TOKEN)

# # ID of the database you want to add to
# database_id = DATABASE_ID


# index = 2

# # Your DataFrame
# cloud_filename = cod3x.get_revenue_by_day_cloud_name(index)
# cloud_bucket_name = lph.get_lp_config_value('cloud_bucket_name', index)
# df = cs.read_from_cloud_storage('metis', cloud_bucket_name)

# # df = pd.DataFrame({
# #     'Name': ['John', 'Jane', 'Bob'],
# #     'Age': [30, 25, 35],
# #     'City': ['New York', 'London', 'Paris']
# # })

# # Function to convert DataFrame value to Notion property
# def to_notion_property(value):
#     if pd.isna(value):
#         return None
#     elif isinstance(value, (int, float)):
#         return {"number": value}
#     elif isinstance(value, str):
#         return {"rich_text": [{"text": {"content": value}}]}
#     else:
#         return {"rich_text": [{"text": {"content": str(value)}}]}

# # Iterate through DataFrame rows
# for _, row in df.iterrows():
#     properties = {}
#     for column, value in row.items():
#         if column == df.columns[0]:  # Assume first column is the Name/Title
#             properties[column] = {"title": [{"text": {"content": str(value)}}]}
#         else:
#             properties[column] = to_notion_property(value)
    
#     # Create a new page (row) in the Notion database
#     notion.pages.create(
#         parent={"database_id": database_id},
#         properties=properties
#     )
    
#     # Sleep to respect rate limits
#     sleep(0.5)

# print("DataFrame uploaded to Notion successfully!")