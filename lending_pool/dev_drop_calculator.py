from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import time
import datetime

csv_name = '2500_lrt_supplier.csv'
# df = pd.read_csv(csv_name, usecols=['user_address', 'wallet_total_twap_balance'])
df = pd.read_csv(csv_name, usecols=['user_address', 'wallet_token_total_points'])

try:
    df['wallet_token_total_points'] = df['wallet_total_twap_balance']
except:
    pass

reward_total = 2500
total_category_points = df['wallet_token_total_points'].sum()

df['share_of_points'] = df['wallet_token_total_points'] / total_category_points

# Calculate the desired distribution (approximate due to rounding)
df['amount_distributed'] = df['share_of_points'] * reward_total

# Round the distribution to whole numbers (casting to int)
df['amount_distributed'] = df['amount_distributed'].astype(int)

df = df.loc[df['amount_distributed'] > 0]

# Calculate remaining amount (if total cannot be distributed perfectly)
remaining_amount = reward_total - df['amount_distributed'].sum()

row_counter = 0

while remaining_amount >= 0:
    print('Remaining Amount: ', remaining_amount)
    df.iloc[row_counter:row_counter+1, df.columns.get_loc('amount_distributed')] += 1
    row_counter += 1
    remaining_amount -= 1
    # time.sleep(0.1)'


    if row_counter > len(df):
       row_counter = 0

print(reward_total)
print(df['amount_distributed'].sum())
print(df)
df.to_csv('script_output.csv', index=False)