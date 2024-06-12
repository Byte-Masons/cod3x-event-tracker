import sqlite3
import pandas as pd
import time

connection = sqlite3.connect("turtle.db")

cursor = connection.cursor()

# from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number

def make_table(cursor, table_name):
    cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                from_address TEXT,
                to_address TEXT,
                tx_hash TEXT,
                timestamp TEXT,
                token_address TEXT,
                reserve_address TEXT,
                token_volume TEXT,
                asset_price TEXT,
                usd_token_amount TEXT,
                log_index TEXT,
                transaction_index TEXT,
                block_number TEXT         
                )
            """)
    
    return

# # will make a table given a list of columns and their datatype
def make_specific_table(cursor, column_list, data_type_list, table_name):
    
    # insert_command_text = ''

    # i = 0

    # while i < len(column_list):
    #     column_name = column_list[i]
    #     data_type = data_type_list[i]

    #     insert_command_text += '"'
    #     insert_command_text += column_name
    #     insert_command_text += '"'
    #     insert_command_text += ' '
    #     insert_command_text += data_type

    #     if i != len(column_list) - 1:
    #         insert_command_text += ','
        
    #     i += 1

    # Create comma-separated column definitions with quotes
    column_definitions = []
    for i in range(len(column_list)):
        column_definitions.append(f"{column_list[i]} {data_type_list[i]}")

    # Join column definitions with commas
    insert_command_text = ",".join(column_definitions)

    # Execute the CREATE TABLE statement with parameter substitution
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            {insert_command_text}
        )
    """)
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    table_exists = cursor.fetchone() is not None

    return

# # makes a table in our database for strictly our snapshot data point
def make_snapshot_table(cursor):
    # user_address,token_address,tx_hash,timestamp,time_difference,embers,amount_cumulative,ember_balance,total_ember_balance
    cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS snapshot(
                user_address TEXT,
                token_address TEXT,
                tx_hash TEXT,
                timestamp TEXT,
                time_difference TEXT,
                embers TEXT,
                amount_cumulative TEXT,
                ember_balance TEXT,
                total_ember_balance TEXT,
                token_cumulative TEXT
                )
            """)
    
    connection.commit()
    
    return


# # given a create query, will create our table in our database
def create_custom_table(query):
    cursor = connection.cursor()
    
    cursor.execute(query)

    connection.commit()

    return

# # will write to our custom table
def write_to_custom_table(query, df):
    cursor = connection.cursor()

    df = df.astype(str)
    
    # Get DataFrame as a list of tuples
    data_tuples = df.to_records(index=False)  # Avoids inserting the index as a column

    cursor.executemany(query, data_tuples)

    connection.commit()

    return

# # inserts our snapshot dataframe data into our snapshot database
def insert_data_into_snapshot_table(cursor, snapshot_df):

    snapshot_df = snapshot_df[['user_address','token_address','tx_hash','timestamp','time_difference','embers','amount_cumulative','ember_balance','total_ember_balance','token_cumulative']]
    snapshot_df = snapshot_df.astype(str)

    # Get DataFrame as a list of tuples
    data_tuples = snapshot_df.to_records(index=False)  # Avoids inserting the index as a column

    cursor.executemany("""
        INSERT INTO snapshot (user_address,token_address,tx_hash,timestamp,time_difference,embers,amount_cumulative,ember_balance,total_ember_balance,token_cumulative)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data_tuples)

    connection.commit()

    return

def make_dummy_data(cursor):
    cursor.execute("""
            INSERT INTO persons VALUES
            ('Mean', 'Dean', 26),
            ('Clean', 'Mean', 25),
            ('Mean', 'Mean', 27)
            """)
    return

def select_star_count(table_name):
    
    cursor = connection.cursor()

    cursor.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
            """)

    rows = cursor.fetchall()

    return rows

def select_star(table_name):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT *
        FROM {table_name}
        """)
    
    rows = cursor.fetchall()

    return rows

# # selects specific columns from our database
def select_specific_columns(cursor, column_list, table_name):
    
    columns_string = ', '.join(column_list)

    query = f"SELECT DISTINCT {columns_string} FROM {table_name}"

    cursor.execute(query)

    rows = cursor.fetchall()

    return rows

# # lets a user define a query that is executed and returned
def get_user_query(query):

    cursor.execute(query)

    rows = cursor.fetchall()

    return rows

def get_sql_df(rows, column_list):
    df = pd.DataFrame(rows, columns=column_list)
    
    return df
# # tries to select rows from our database that are greater than the timestamp we provide
def select_rows_greater_than_timestamp(cursor, timestamp):
    
    timestamp_float = float(timestamp)

    cursor.execute(f"""
            SELECT *
            FROM persons
            WHERE {timestamp_float} >= CAST(timestamp as FLOAT)
            """)

    rows = cursor.fetchall()

    return rows


# # tries to drop duplicate entries from our database
def drop_duplicates_from_database(cursor):
    
    # # creates temp table of only distinct values from our 
    cursor.execute("""
            DELETE FROM persons
            WHERE ROWID NOT IN (
            SELECT MIN(ROWID)
            FROM persons
            GROUP BY tx_hash, transaction_index, log_index, token_address
            );
            """)
    
    connection.commit()

    return

def write_to_db(df, column_list, table_name):

    cursor = connection.cursor()

    old_length = select_star_count(table_name)
    old_length = old_length[0]
    old_length = int(old_length[0])

    df = df.astype(str)

    # Get DataFrame as a list of tuples
    data_tuples = df.to_records(index=False)  # Avoids inserting the index as a column

    value_string = ''
    column_string = ''

    i = 0

    while i < len(column_list):
        value_string += '?'
        column_string += column_list[i]

        if i != len(column_list) - 1:
            value_string += ','
            column_string += ','
        
        i += 1

    cursor.executemany(f"""
        INSERT INTO {table_name} ({column_string})
        VALUES ({value_string})
    """, data_tuples)

    connection.commit()

    new_length = select_star_count(table_name)
    new_length = new_length[0]
    new_length = int(new_length[0])

    print('Event Database Updated. Old Length: ', old_length, ' New Length: ', new_length, ' Events Added: ', new_length - old_length)


def test_write_loop(cursor):
    i = 0

    while i < 10:
        make_dummy_data(cursor)
        i += 1
        # Commit changes to the database
        connection.commit()
        print("Data inserted successfully!")
        i += 1

# # columns and datatypes **
# # sees if our given value exists in our database
def sql_value_exists(value, column_name, column_list, table_name):
    
    cursor = connection.cursor()

    df = pd.DataFrame()

    exists = False
    # Sample DataFrame (replace with your actual DataFrame)
    # df = pd.DataFrame({'from_address': ['0x0000000000000000000000000000000000000000']})

    # Get values to check (can be a list or Series)
    values_to_check = [value]

    # Construct the query with placeholders for values
    query = f"""
    SELECT *
    FROM {table_name}
    WHERE {column_name} IN ({', '.join('?' * len(values_to_check))})
    LIMIT 1
    """

    # Convert values to a tuple for execution (important!)
    values_tuple = tuple(values_to_check)

    # Execute the query with the values
    cursor.execute(query, values_tuple)

    # Fetch results (optional)
    results = cursor.fetchall()
    
    # Analyze results:
    if results:
        # print("Matching values found in the database.")
        exists = True
        # You can process the results further (e.g., print specific columns)
    else:
        exists = False
        # print("No matching values found in the database.")
    

    if exists == True:
        df = pd.DataFrame(results, columns=column_list)


    return df

# # will see if all of our values exist in the sql table
def sql_multiple_values_exist(value_list, column_list, table_name):

    cursor = connection.cursor()

    exists = False

    # # casts all of values to str for the purpose of querying our fully str database
    value_list = [str(item)[2:] for item in value_list]
    column_list = [str(item) for item in column_list]

    i = 0

    # where_and_statement_string = f" WHERE {column_list[i]} LIKE '%{value_list[i]}%'"

    # # constructs our WHERE and AND statements
    while i < len(value_list):
        if i == 0:
            where_and_statement_string = f"WHERE {column_list[i]} LIKE '%{value_list[i]}%'"
        else:
            where_and_statement_string += f" AND {column_list[i]} LIKE '%{value_list[i]}%'"

        i += 1

    query = f"""SELECT * FROM {table_name} {where_and_statement_string} LIMIT 1"""

    cursor.execute(query)

    # Fetch results (optional)
    results = cursor.fetchall()
    
    # Analyze results:
    if results:
        # print("Matching values found in the database.")
        exists = True
        # You can process the results further (e.g., print specific columns)
    else:
        exists = False
        # print("No matching values found in the database.")

    return exists

# # generalized exists function that will help us reduce rpc calls
def value_exists(df, input_value, column_name):

    input_value = str(input_value)

    if (df[column_name] == input_value).any():
        df = df.loc[df[column_name] == input_value]
    
    else:
        df = pd.DataFrame()
    
    return df

def already_part_of_database(event, wait_time, column_list, table_name):
    
    # # will make a table if our table doesn't already exist
    # make_specific_table(cursor, column_list, data_type_list, table_name)

    all_exist = False
    temp_exists = False
    wait_time = wait_time / 3

    tx_hash = ''
    token_amount = -1
    token_address = ''
    from_address = ''
    to_address = ''


    tx_hash = event['transactionHash'].hex()

    df = sql_value_exists(tx_hash, 'tx_hash', column_list, table_name)

    value_list = []
    column_list = []

    value_list.append(tx_hash)
    column_list.append('tx_hash')
    time.sleep(wait_time)

    if len(df) > 0:
        token_amount = event['args']['value']
        time.sleep(wait_time)

        value_list.append(token_amount)
        column_list.append('token_volume')

        temp_exists = sql_multiple_values_exist(value_list, column_list, table_name)
        
        # df = value_exists(df, tx_index, 'transaction_index')
        if temp_exists == True:
            token_address = event['address']
            time.sleep(wait_time)

            value_list.append(token_address)
            column_list.append('token_address')

            temp_exists = sql_multiple_values_exist(value_list, column_list, table_name)

            if temp_exists == True:
            
                from_address = event['args']['from']
                time.sleep(wait_time)

                value_list.append(from_address)
                column_list.append('from_address')

                temp_exists = sql_multiple_values_exist(value_list, column_list, table_name)

            if temp_exists == True:

                to_address = event['args']['to']
                time.sleep(wait_time)

                value_list.append(to_address)
                column_list.append('to_address')
                
                temp_exists = sql_multiple_values_exist(value_list, column_list, table_name)

    
    # sets our all_exists variable to whether we have found these 3 values before or not
    if temp_exists == True:
        all_exist = True

    response_list = [tx_hash, token_amount, token_address, from_address, to_address, all_exist]

    return response_list

# # returns only rows of data that occured since the last snapshot
def select_next_batch_of_ember_accumulators(cursor, column_list):
    
    columns_string = ', '.join(column_list)

    query = f"SELECT DISTINCT {columns_string} FROM persons"

    cursor.execute(query)

    rows = cursor.fetchall()

    return rows

# # drops our specified table
def drop_table(table_name):
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE {table_name}")
    print('table_dropped')
    
    connection.commit()

    return

def make_new_snapshot_table(cursor, table_name, snapshot_df):

    try:
        drop_table(cursor, table_name)
    except:
        print('No table to drop')
    
    try:
        make_snapshot_table(cursor)
    except:
        print('Table already exists')

    insert_data_into_snapshot_table(cursor, snapshot_df)

    return

# # makes a transaction df table
def make_new_table(cursor, table_name, df):
    
    try:
        drop_table(cursor, table_name)
    except:
        print('No table to drop')
    
    try:
        make_table(cursor, table_name)
    except:
        print('Table already exists')

    insert_data_into_snapshot_table(cursor, df)

    return

# t2.to_address, t2.token_address, t2.timestamp
# from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number
def get_post_snapshot_data(cursor, snapshot_table_name, all_data_table_name):
    query = f"""
        SELECT t2.from_address, t2.to_address,t2.tx_hash,t2.timestamp,t2.token_address,t2.reserve_address,t2.token_volume,t2.asset_price,t2.usd_token_amount,t2.log_index,t2.transaction_index,t2.block_number
        FROM {all_data_table_name} AS t2
        INNER JOIN (
        SELECT user_address, token_address, MAX(timestamp) AS latest_timestamp
        FROM {snapshot_table_name}
        GROUP BY user_address, token_address
        ) AS latest_snapshots
        ON t2.to_address = latest_snapshots.user_address 
        AND t2.token_address = latest_snapshots.token_address
        AND t2.timestamp > latest_snapshots.latest_timestamp;
        """
    
    cursor.execute(query)

    rows = cursor.fetchall()
    
    column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number']

    df = get_sql_df(rows, column_list)

    return rows

# # makes a dataframe of our transaction table data
def get_transaction_data_df(all_data_table_name):
    
    connection = sqlite3.connect("turtle.db")

    cursor = connection.cursor()

    query = f"""
        SELECT *
        FROM {all_data_table_name}
    """

    cursor.execute(query)

    rows = cursor.fetchall()
    
    column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number']

    df = get_sql_df(rows, column_list)
    

    return df

# # takes in a custom query and returns the df associated with it
def get_custom_query(query, column_list):

    cursor = connection.cursor()

    cursor.execute(query)
    
    rows = cursor.fetchall()

    df = get_sql_df(rows, column_list)

    return df 