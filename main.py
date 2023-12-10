from dataExtract import dataExtract
from data_mapping_with_transfer import data_mapping_with_transfer

if __name__ == '__main__':
    extract = dataExtract()

    sql = {"host": "localhost", "user": "root", "password": "Password123#@!","database":"Phonepe"}
    sql_conn = data_mapping_with_transfer(sql)
    connect = sql_conn.connect_db()
    create_table_ddl = sql_conn.execute_ddl(connect['cursor'], connect['conn'])

    # Define the root directory path
    agg_transaction_directory = 'pulse-master/data/aggregated/transaction/country/india/'
    agg_trans_df = extract.process_agg_transaction_files(agg_transaction_directory)
    insert_agg_trans = sql_conn.insert_agg_transform(connect['cursor'], connect['conn'], agg_trans_df)
    agg_transform = sql_conn.select_from_AggregatedTransforms(connect['cursor'], connect['conn'])
    print(agg_transform.info())
    print(agg_transform.head())

    agg_user_directory = 'pulse-master/data/aggregated/user/country/india/'
    agg_user_df = extract.process_agg_user_files(agg_user_directory)
    insert_agg_user = sql_conn.insert_agg_user(connect['cursor'], connect['conn'], agg_user_df)
    agg_user = sql_conn.select_from_AggregatedUsers(connect['cursor'], connect['conn'])
    print(agg_user.info())
    print(agg_user.head())

    map_transaction_directory = 'pulse-master/data/map/transaction/hover/country/india/'
    map_trans_df = extract.process_map_transaction_files(map_transaction_directory)
    insert_map_trans = sql_conn.insert_map_transform(connect['cursor'], connect['conn'], map_trans_df)
    map_transform = sql_conn.select_from_MappedTransforms(connect['cursor'], connect['conn'])
    print(map_transform.info())
    print(map_transform.head())

    map_user_directory = 'pulse-master/data/map/user/hover/country/india/'
    map_user_df = extract.process_map_user_files(map_user_directory)
    insert_map_user = sql_conn.insert_map_user(connect['cursor'], connect['conn'], map_user_df)
    map_user = sql_conn.select_from_MappedUsers(connect['cursor'], connect['conn'])
    print(map_user.info())
    print(map_user.head())

    top_transaction_directory = 'pulse-master/data/top/transaction/country/india/'
    top_trans_df = extract.process_top_transaction_files(top_transaction_directory)
    insert_top_trans = sql_conn.insert_top_transform(connect['cursor'], connect['conn'], top_trans_df)
    top_transform = sql_conn.select_from_TopTransforms(connect['cursor'], connect['conn'])
    print(top_transform.info())
    print(top_transform.head())

    top_user_directory = 'pulse-master/data/top/user/country/india'
    top_user_df = extract.process_top_user_files(top_user_directory)
    insert_top_user = sql_conn.insert_top_users(connect['cursor'], connect['conn'], top_user_df)
    top_user = sql_conn.select_from_TopUsers(connect['cursor'], connect['conn'])
    print(top_user.info())
    print(top_user.head())


    