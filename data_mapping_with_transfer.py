import mysql.connector
import datetime
import pandas as pd

#Transfer the data to mysql a relational db

class data_mapping_with_transfer:
    def __init__(self, sql):
        self.host = sql["host"]
        self.user = sql["user"]
        self.password = sql["password"]
        self.database = sql["database"]

    def connect_db(self):
        try:
            db_conn = mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=self.database,auth_plugin='mysql_native_password')
            cursor_db=db_conn.cursor()
            return({"cursor": cursor_db, "conn": db_conn})
        except Exception as e:
            return ("Error connecting to MySQL database: "+str(e))

    def execute_ddl(self, cursor_db, db_conn):
        try:
            agg_transform = '''CREATE TABLE IF NOT EXISTS AggregatedTransforms (FromTimestamp DATETIME, ToTimestamp DATETIME, TypeOfPaymentCategory VARCHAR(200), NoOfTransactions BIGINT, TotalValue FLOAT, Quarter INT, State VARCHAR(255), Year INT)'''
            agg_user = '''CREATE TABLE IF NOT EXISTS AggregatedUsers (TotalRegisteredUsers BIGINT, NoOfAppOpensByUsers BIGINT, DeviceBrand VARCHAR(200), RegisteredUsersByBrand BIGINT, PercentageOfShareByDevice FLOAT, Quarter INT, State VARCHAR(255), YEAR INT)'''
            map_transform = '''CREATE TABLE IF NOT EXISTS MappedTransforms (StateOrDistrictName VARCHAR(255), NoOfTransactions BIGINT, TotalTransactionValue FLOAT, Quarter INT, State VARCHAR(255), YEAR INT)'''
            map_user = '''CREATE TABLE IF NOT EXISTS MappedUsers (StateOrDistrictName VARCHAR(255), TotalRegisteredUsers BIGINT, NoOfAppOpensByUsers BIGINT, Quarter INT, State VARCHAR(255), YEAR INT)'''
            top_transform = '''CREATE TABLE IF NOT EXISTS TopTransforms (Entity VARCHAR(50), StateOrDistrictorPincodeValue VARCHAR(255), NoOfTransactions BIGINT, TotalTransactionValue FLOAT, Quarter INT, State VARCHAR(255), YEAR INT)'''
            top_user = '''CREATE TABLE IF NOT EXISTS TopUsers (Entity VARCHAR(50), StateOrDistrictorPincodeValue VARCHAR(255), TotalRegisteredUsers BIGINT, Quarter INT, State VARCHAR(255), YEAR INT)'''
            cursor_db.execute(agg_transform)
            cursor_db.execute(agg_user)
            cursor_db.execute(map_transform)
            cursor_db.execute(map_user)
            cursor_db.execute(top_transform)
            cursor_db.execute(top_user)
            db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Tables created")
        except Exception as e:
            db_conn.rollback()
            return("Error in execute_ddl "+str(e))

    def close_connection(self, cursor_db, db_conn):
        db_conn.commit()
        cursor_db.close()
        db_conn.close()

    def insert_agg_transform(self, cursor_db, db_conn, agg_trans_df):
        try:
            for index, row in agg_trans_df.iterrows():
                from_timestamp = row['from'].to_pydatetime()
                to_timestamp = row['to'].to_pydatetime()
                insert_agg_transform_query = '''INSERT INTO AggregatedTransforms (FromTimestamp, ToTimestamp, TypeOfPaymentCategory, NoOfTransactions, TotalValue, Quarter, State, Year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
                cursor_db.execute(insert_agg_transform_query, (from_timestamp, to_timestamp, row['transaction'], row['count'], f"{row['amount']:.15f}", row['fileName'], row['stateFolder'], row['folder']))
                db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Aggregated Transform Data Added")
        except Exception as e:
            db_conn.rollback()
            return("Error in insert_agg_transform : "+str(e))

    def insert_agg_user(self, cursor_db, db_conn, agg_users_df):
        try:
            for index, row in agg_users_df.iterrows():
                insert_agg_user_query = '''INSERT INTO AggregatedUsers (TotalRegisteredUsers, NoOfAppOpensByUsers, DeviceBrand, RegisteredUsersByBrand, PercentageOfShareByDevice, Quarter, State, YEAR) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
                cursor_db.execute(insert_agg_user_query, (row['registeredUsers'], row['appOpens'], row['brand'], row['count'], row['percentage'], row['fileName'], row['stateFolder'], row['folder']))
                db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Aggregated Users Data Added")
        except Exception as e:
            db_conn.rollback()
            return("Error in insert_agg_user : "+str(e))

    def insert_map_transform(self, cursor_db, db_conn, map_trans_df):
        try:
            for index, row in map_trans_df.iterrows():
                insert_map_transform_query = '''INSERT INTO MappedTransforms (StateOrDistrictName, NoOfTransactions, TotalTransactionValue, Quarter, State, YEAR) VALUES (%s, %s, %s, %s, %s, %s);'''
                cursor_db.execute(insert_map_transform_query, (row['name'], row['count'], f"{row['amount']:.15f}", row['fileName'], row['stateFolder'], row['folder']))
                db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Mapped Transformation Data Added")
        except Exception as e:
            db_conn.rollback()
            return("Error in insert_map_transform : "+str(e))

    def insert_map_user(self, cursor_db, db_conn, map_user_df):
        try:
            for index, row in map_user_df.iterrows():
                insert_map_user_query = '''INSERT INTO MappedUsers (StateOrDistrictName, TotalRegisteredUsers, NoOfAppOpensByUsers, Quarter, State, YEAR) VALUES (%s, %s, %s, %s, %s, %s);'''
                cursor_db.execute(insert_map_user_query, (row['stateName'], row['registeredUsers'], row['appOpens'], row['fileName'], row['stateFolder'], row['folder']))
                db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Mapped Users Data Added")
        except Exception as e:
            db_conn.rollback()
            return("Error in insert_map_user : "+str(e))

    def insert_top_transform(self, cursor_db, db_conn, top_trans_df):
        try:
            for index, row in top_trans_df.iterrows():
                insert_top_transform_query = '''INSERT INTO TopTransforms (Entity, StateOrDistrictorPincodeValue, NoOfTransactions, TotalTransactionValue, Quarter, State, YEAR) VALUES (%s, %s, %s, %s, %s, %s, %s);'''
                cursor_db.execute(insert_top_transform_query, (row['entity'], row['entityName'], row['count'], f"{row['amount']:.15f}", row['fileName'], row['stateFolder'], row['folder']))
                db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Top Transformation Data Added")
        except Exception as e:
            db_conn.rollback()
            return("Error in insert_map_transform : "+str(e))

    def insert_top_users(self, cursor_db, db_conn, top_user_df):
        try:
            for index, row in top_user_df.iterrows():
                insert_top_user_query = '''INSERT INTO TopUsers (Entity, StateOrDistrictorPincodeValue, TotalRegisteredUsers, Quarter, State, YEAR) VALUES (%s, %s, %s, %s, %s, %s);'''
                cursor_db.execute(insert_top_user_query, (row['entityType'], row['entityName'], row['registeredUsers'], row['fileName'], row['stateFolder'], row['folder']))
                db_conn.commit()
            #self.close_connection(cursor_db, db_conn)
            return("Top User Data Added")
        except Exception as e:
            db_conn.rollback()
            return("Error in insert_top_users : "+str(e))

    def select_from_AggregatedTransforms(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM AggregatedTransforms'''
            agg_transform = pd.read_sql_query(select_query, db_conn)
            return agg_transform
        except Exception as e:
            return("Error in fetching select_from_AggregatedTransforms : "+str(e))

    def select_from_AggregatedUsers(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM AggregatedUsers'''
            agg_user = pd.read_sql_query(select_query, db_conn)
            return agg_user
        except Exception as e:
            return("Error in fetching select_from_table : "+str(e))

    def select_from_MappedTransforms(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM MappedTransforms'''
            map_transform = pd.read_sql_query(select_query, db_conn)
            return map_transform
        except Exception as e:
            return("Error in fetching select_from_MappedTransforms : "+str(e))

    def select_from_MappedUsers(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM MappedUsers'''
            map_user = pd.read_sql_query(select_query, db_conn)
            return map_user
        except Exception as e:
            return("Error in fetching select_from_MappedUsers : "+str(e))

    def select_from_TopTransforms(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM TopTransforms'''
            top_transform = pd.read_sql_query(select_query, db_conn)
            return top_transform
        except Exception as e:
            return("Error in fetching select_from_TopTransforms : "+str(e))

    def select_from_TopUsers(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM TopUsers'''
            top_user = pd.read_sql_query(select_query, db_conn)
            return top_user
        except Exception as e:
            return("Error in fetching select_from_TopUsers : "+str(e))