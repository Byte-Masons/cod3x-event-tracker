import pandas as pd
from lending_pool import lp_tracker
from lending_pool import balance_and_points as bp
from sql_interfacer import sql
# from cloud_storage import cloud_storage as cs


df = sql.get_transaction_data_df('persons')
print(df)

# df = bp.set_embers_database(0)

# print(df)

# index_list = [0]

# for index in index_list:
#     lp_tracker.run_all(index_list)