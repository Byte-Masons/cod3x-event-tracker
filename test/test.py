import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

df_list = []
for user_balance in user_balance_list:
    df = user_balance.run_all()
    df_list.append(df)

df = pd.concat(df_list)

df.to_csv('test_test.csv', index=False)

# *****
df = pd.read_csv('test_test.csv')
# Convert Unix timestamp to datetime
df['utc_datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)

# Format the datetime to mm/dd/yyyy
df['formatted_date'] = df['utc_datetime'].dt.strftime('%m/%d/%Y')

# If you want to keep only the formatted date, you can drop the intermediate column
df = df.drop('utc_datetime', axis=1)

# Sort the DataFrame by user, formatted_date, and max_block in descending order
df_sorted = df.sort_values(['user', 'formatted_date', 'max_block'], ascending=[True, True, False])

# Group by user and formatted_date, and keep the first row of each group
df_result = df_sorted.groupby(['user', 'formatted_date']).first().reset_index()

# If you want to keep only specific columns, you can do so like this:
columns_to_keep = ['user', 'token_address', 'formatted_date', 'effective_balance', 'token_volume', 'max_block', 'event_type']
df = df_result[columns_to_keep]

asset_list = ['0xe7334Ad0e325139329E747cF2Fc24538dD564987', '0x02CD18c03b5b3f250d2B29C87949CDAB4Ee11488', '0x9c29a8eC901DBec4fFf165cD57D4f9E03D4838f7', '0x272CfCceFbEFBe1518cd87002A8F9dfd8845A6c4', '0x58254000eE8127288387b04ce70292B56098D55C', '0x4522DBc3b2cA81809Fa38FEE8C1fb11c78826268', '0xC17312076F48764d6b4D263eFdd5A30833E311DC', '0xe3f709397e87032E61f4248f53Ee5c9a9aBb6440', '0x0F4f2805a6d15dC534d43635314444181A0e82CD']
price_list = [1, 1, 2726.77, 2772.22, 2853.21, 2853.21, 63786.31, 2783.49, 0.01342]
decimal_list = [1e6, 1e6, 1e18, 1e18, 1e18, 1e18, 1e8, 1e18, 1e18]

price_df = pd.DataFrame()
price_df['asset'] = asset_list
price_df['price'] = price_list
price_df['decimal'] = decimal_list

for asset in asset_list:
    price = price_df['price'].tolist()[0]
    decimals = price_df['decimal'].tolist()[0]

    df.loc[df['token_address'] == asset, 'price'] = price
    df.loc[df['token_address'] == asset, 'decimals'] = decimals

df['token_volume'] = df['token_volume'].astype(float)

df['tx_amount_usd'] = df['token_volume'] / df['decimals'] * df['price']

df.loc[df['event_type'].isin(['withdraw']), 'tx_amount_usd'] = df['token_volume'] / df['decimals'] * df['price']

turtle_user_df = pd.read_csv('turtle_users.csv')
turtle_user_list = turtle_user_df['wallet_address'].unique()

cutoff_date = datetime(2024, 5, 20)
df['formatted_date'] = pd.to_datetime(df['formatted_date'])

df.loc[(df['user'].isin(turtle_user_list)) & (df['formatted_date'] >= cutoff_date), 'user_type'] = 'turtle'

# New line to label 'regular' users
df.loc[~df['user'].isin(turtle_user_list), 'user_type'] = 'regular'

df = df.sort_values(by='formatted_date', ascending=True)

print(df)

df = df.loc[df['formatted_date'] >= cutoff_date]
# Calculate cumulative sum while preserving the DataFrame structure
df = df.groupby(['formatted_date', 'user_type'])['tx_amount_usd'].sum().reset_index()

df = df.sort_values(by='formatted_date', ascending=True)

# Calculate cumulative sum per user_type
df['cumsum_per_user_type'] = df.groupby('user_type')['tx_amount_usd'].cumsum()

print(df)

# df.loc[df['effective_balance_usd'] < 0, 'effective_balance_usd'] = 0

print(df.loc[df['user_type'] == 'turtle'])

df = df.loc[df['user_type'] == 'regular']

df.to_csv('turtle_test.csv', index=False)
# *****