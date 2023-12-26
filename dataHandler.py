from dataExtract import dataExtract
from data_mapping_with_transfer import data_mapping_with_transfer

class dataHandler:
    #Constructor
    def __init__(self):
        pass
    
    #Extract data from the respective directory and insert it to the table
    def callToDB(self, sql_conn, connect):
        try:
            extract = dataExtract()
            agg_transaction_directory = 'pulse-master/data/aggregated/transaction/country/india/'
            agg_trans_df = extract.process_agg_transaction_files(agg_transaction_directory)
            insert_agg_trans = sql_conn.insert_agg_transform(connect['cursor'], connect['conn'], agg_trans_df)
            agg_user_directory = 'pulse-master/data/aggregated/user/country/india/'
            agg_user_df = extract.process_agg_user_files(agg_user_directory)
            insert_agg_user = sql_conn.insert_agg_user(connect['cursor'], connect['conn'], agg_user_df)
            map_transaction_directory = 'pulse-master/data/map/transaction/hover/country/india/'
            map_trans_df = extract.process_map_transaction_files(map_transaction_directory)
            insert_map_trans = sql_conn.insert_map_transform(connect['cursor'], connect['conn'], map_trans_df)
            map_user_directory = 'pulse-master/data/map/user/hover/country/india/'
            map_user_df = extract.process_map_user_files(map_user_directory)
            insert_map_user = sql_conn.insert_map_user(connect['cursor'], connect['conn'], map_user_df)
            top_transaction_directory = 'pulse-master/data/top/transaction/country/india/'
            top_trans_df = extract.process_top_transaction_files(top_transaction_directory)
            insert_top_trans = sql_conn.insert_top_transform(connect['cursor'], connect['conn'], top_trans_df)
            top_user_directory = 'pulse-master/data/top/user/country/india'
            top_user_df = extract.process_top_user_files(top_user_directory)
            insert_top_user = sql_conn.insert_top_users(connect['cursor'], connect['conn'], top_user_df)
            return "Data imported successfully"
        except Exception as e:
            print(f"Error processing callToDB: {e}")