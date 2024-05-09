import pandas as pd
from lending_pool import lp_tracker
from lending_pool import balance_and_points as bp
from cloud_storage import cloud_storage as cs

# df = pd.read_csv('test/test.csv')

# cs.df_write_to_cloud_storage(df, 'current_user_tvl_embers.csv', 'ironclad-bucket')

df = cs.read_from_cloud_storage('current_user_tvl_embers.csv', 'ironclad-bucket')
# index_list = [0]


# df = bp.set_embers_database()

print(df)

# for index in index_list:
#     lp_tracker.find_all_lp_transactions(index)