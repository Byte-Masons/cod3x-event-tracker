import sqlite3
import pandas as pd
import time

connection = sqlite3.connect("turtle.db")

cursor = connection.cursor()

# from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number

def make_table(cursor):
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS persons(
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

def make_dummy_data(cursor):
    cursor.execute("""
            INSERT INTO persons VALUES
            ('Mean', 'Dean', 26),
            ('Clean', 'Mean', 25),
            ('Mean', 'Mean', 27)
            """)
    return

def select_star(cursor):
    cursor.execute("""
            SELECT CAST(timestamp as FLOAT) as float_stamp
            FROM persons
            """)

    rows = cursor.fetchall()

    return rows

# # selects specific columns from our database
def select_specific_columns(cursor, column_list):
    
    columns_string = ', '.join(column_list)

    query = f"SELECT DISTINCT {columns_string} FROM persons"

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

def write_to_db(cursor, df):

    old_length = select_star(cursor)
    old_length = old_length[0]
    old_length = int(old_length[0])

    df = df.astype(str)

    # Get DataFrame as a list of tuples
    data_tuples = df.to_records(index=False)  # Avoids inserting the index as a column


    cursor.executemany("""
        INSERT INTO persons (from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data_tuples)

    connection.commit()

    new_length = select_star(cursor)
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

# # sees if our given value exists in our database
def sql_value_exists(cursor, value, column_name):
    
    df = pd.DataFrame()

    exists = False
    # Sample DataFrame (replace with your actual DataFrame)
    # df = pd.DataFrame({'from_address': ['0x0000000000000000000000000000000000000000']})

    # Get values to check (can be a list or Series)
    values_to_check = [value]

    # Construct the query with placeholders for values
    query = f"""
    SELECT *
    FROM persons
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
        df = pd.DataFrame(results, columns=['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number'])


    return df

# # generalized exists function that will help us reduce rpc calls
def value_exists(df, input_value, column_name):

    input_value = str(input_value)

    if (df[column_name] == input_value).any():
        df = df.loc[df[column_name] == input_value]
    
    else:
        df = pd.DataFrame()
    
    return df

def already_part_of_database(cursor, event, wait_time):

    all_exist = False
    tx_hash = ''
    log_index = -1
    tx_index = -1
    token_amount = -1
    wait_time = wait_time / 3
    token_address = ''

    tx_hash = event['transactionHash'].hex()

    df = sql_value_exists(cursor, tx_hash, 'tx_hash')

    time.sleep(wait_time)

    if len(df) > 0:
        tx_index = event['transactionIndex']
        time.sleep(wait_time)
        df = value_exists(df, tx_index, 'transaction_index')
        if len(df) > 0:
            log_index = event['logIndex']

            df = value_exists(df, log_index, 'log_index')

            if len(df) > 0:
                token_address = event['address']

                df = value_exists(df, token_address, 'token_address')
    
    # sets our all_exists variable to whether we have found these 3 values before or not
    if len(df) > 0:
        all_exist = True

    response_list = [tx_hash, log_index, tx_index, token_amount, token_address, all_exist]

    return response_list

# rows = select_star(cursor)

# rows = select_rows_greater_than_timestamp(cursor, 1709544909)

# column_list = ['from_address','to_address','timestamp','token_address', 'token_volume','tx_hash']

# rows = select_specific_columns(cursor, column_list)
# print(rows)

# value = '0x0000000000000000000000000000000000000000'
# column_name = 'from_address'
# make_table(cursor)
# write_to_db(cursor)
# select_star(cursor)
# sql_value_exists(cursor,value, column_name)

# connection.commit()
# # # Close the connection
# connection.close()

# print("Data inserted successfully!")