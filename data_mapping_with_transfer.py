import mysql.connector
import datetime
import pandas as pd

#Transfer the data to mysql a relational db as an input / streamlit as a output

class data_mapping_with_transfer:
    #Constructor
    def __init__(self, sql):
        self.host = sql["host"]
        self.user = sql["user"]
        self.password = sql["password"]
        self.database = sql["database"]

    #Connection to database
    def connect_db(self):
        try:
            db_conn = mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=self.database,auth_plugin='mysql_native_password')
            cursor_db=db_conn.cursor()
            return({"cursor": cursor_db, "conn": db_conn})
        except Exception as e:
            return ("Error connecting to MySQL database: "+str(e))

    #Queries for processing
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
            #self.close_connection(cursor_db, db_conn)
            return agg_transform
        except Exception as e:
            return("Error in fetching select_from_AggregatedTransforms : "+str(e))

    def select_from_AggregatedUsers(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM AggregatedUsers'''
            agg_user = pd.read_sql_query(select_query, db_conn)
            #self.close_connection(cursor_db, db_conn)
            return agg_user
        except Exception as e:
            return("Error in fetching select_from_table : "+str(e))

    def select_from_MappedTransforms(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM MappedTransforms'''
            map_transform = pd.read_sql_query(select_query, db_conn)
            #self.close_connection(cursor_db, db_conn)
            return map_transform
        except Exception as e:
            return("Error in fetching select_from_MappedTransforms : "+str(e))

    def select_from_MappedUsers(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM MappedUsers'''
            map_user = pd.read_sql_query(select_query, db_conn)
            #self.close_connection(cursor_db, db_conn)
            return map_user
        except Exception as e:
            return("Error in fetching select_from_MappedUsers : "+str(e))

    def select_from_TopTransforms(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM TopTransforms'''
            top_transform = pd.read_sql_query(select_query, db_conn)
            #self.close_connection(cursor_db, db_conn)
            return top_transform
        except Exception as e:
            return("Error in fetching select_from_TopTransforms : "+str(e))

    def select_from_TopUsers(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT * FROM TopUsers'''
            top_user = pd.read_sql_query(select_query, db_conn)
            #self.close_connection(cursor_db, db_conn)
            return top_user
        except Exception as e:
            return("Error in fetching select_from_TopUsers : "+str(e))

    def distinctTypeOfPaymentCategories(self, cursor_db, db_conn):
        try:
            select_query = '''SELECT DISTINCT (TypeOfPaymentCategory) FROM AggregatedTransforms'''
            distinctCategory = pd.read_sql_query(select_query, db_conn)
            #self.close_connection(cursor_db, db_conn)
            return distinctCategory['TypeOfPaymentCategory']
        except Exception as e:
            return("Error in fetching distinctTypeOfPaymentCategories : "+str(e))

    def sum_of_AggregatedTransforms(self, cursor_db, db_conn, year, quarter, TypeOfPaymentCategory):
        try:
            #print(TypeOfPaymentCategory)
            payment_categories = [category.strip() for category in TypeOfPaymentCategory.split(',')]
        
            # Create placeholders for each value in payment_categories
            placeholders = ', '.join(['%s' for _ in payment_categories])
            select_query = f'''SELECT SUM(NoOfTransactions) AS SumOfTransactions FROM AggregatedTransforms WHERE Year=%s and Quarter=%s and TypeOfPaymentCategory in ({placeholders}) AND State IS NULL'''
            params = [year, quarter]
            params.extend(payment_categories)

            cursor_db.execute(select_query, params)
            #print(select_query)
            sum_of_transactions = cursor_db.fetchone()
            #self.close_connection(cursor_db, db_conn)
            return sum_of_transactions[0]
        except Exception as e:
            return("Error in fetching sum_of_AggregatedTransforms : "+str(e))

    def total_payment_value(self, cursor_db, db_conn, year, quarter, TypeOfPaymentCategory):
        try:
            #print(TypeOfPaymentCategory)
            payment_categories = [category.strip() for category in TypeOfPaymentCategory.split(',')]
        
            # Create placeholders for each value in payment_categories
            placeholders = ', '.join(['%s' for _ in payment_categories])
            select_query = f'''SELECT SUM(TotalValue) AS TotalPaymentValue FROM AggregatedTransforms WHERE Year=%s and Quarter=%s and TypeOfPaymentCategory in ({placeholders}) and State is null'''
            params = [year, quarter]
            params.extend(payment_categories)

            cursor_db.execute(select_query, params)
            #print(select_query)
            total_payment_value = cursor_db.fetchone()
            #self.close_connection(cursor_db, db_conn)
            return total_payment_value[0]
        except Exception as e:
            return("Error in fetching total_payment_value : "+str(e))

    def average_transaction_value(self, cursor_db, db_conn, year, quarter, TypeOfPaymentCategory):
        try:
            #print(TypeOfPaymentCategory)
            payment_categories = [category.strip() for category in TypeOfPaymentCategory.split(',')]
        
            # Create placeholders for each value in payment_categories
            placeholders = ', '.join(['%s' for _ in payment_categories])
            select_query = f'''SELECT (SUM(TotalValue)/SUM(NoOfTransactions)) AS AvgTransactionValue FROM AggregatedTransforms WHERE Year=%s and Quarter=%s and TypeOfPaymentCategory in ({placeholders}) and State is null'''
            params = [year, quarter]
            params.extend(payment_categories)

            cursor_db.execute(select_query, params)
            #print(select_query)
            average_transaction_value = cursor_db.fetchone()
            #print(average_transaction_value)
            #self.close_connection(cursor_db, db_conn)
            return average_transaction_value[0]
        except Exception as e:
            return("Error in fetching average_transaction_value : "+str(e))
    
    def get_payment_value_by_category(self, cursor_db, db_conn, year, quarter, TypeOfPaymentCategory):
        try:
            payment_categories = [category.strip() for category in TypeOfPaymentCategory.split(',')]
        
            # Create placeholders for each value in payment_categories
            placeholders = ', '.join(['%s' for _ in payment_categories])
            select_query = f'''SELECT TypeOfPaymentCategory, NoOfTransactions FROM AggregatedTransforms WHERE Year=%s and Quarter=%s and TypeOfPaymentCategory in ({placeholders}) and State is null'''
            params = [year, quarter]
            params.extend(payment_categories)

            cursor_db.execute(select_query, params)
            #print(select_query)
            get_payment_value_by_category = pd.DataFrame(cursor_db.fetchall(), columns = ['Category', 'No. of Transaction'])
            #print(get_payment_value_by_category)
            #self.close_connection(cursor_db, db_conn)
            return get_payment_value_by_category
        except Exception as e:
            return("Error in fetching get_payment_value_by_category : "+str(e))
    
    def get_transaction_value_by_states(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = """
                SELECT StateOrDistrictName, NoOfTransactions AS Transactions                  
                FROM MappedTransforms                  
                WHERE Year=%s AND Quarter=%s and StateOrDistrictName not like '%district'                
                ORDER BY NoOfTransactions DESC                  
                LIMIT 10;
            """

            cursor_db.execute(select_query, (year, quarter))
            get_transaction_value_by_states = pd.DataFrame(cursor_db.fetchall(), columns=['State', 'Transactions'])
            return get_transaction_value_by_states
        except Exception as e:
            return "Error in fetching get_transaction_value_by_states : " + str(e)

    def get_transaction_value_by_districts(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = '''
                SELECT REPLACE(StateOrDistrictName, 'district', ''), NoOfTransactions AS Transactions                  
                FROM MappedTransforms                  
                WHERE Year=%s AND Quarter=%s and StateOrDistrictName like '%district'                
                ORDER BY NoOfTransactions DESC                  
                LIMIT 10;
            '''

            cursor_db.execute(select_query, (year, quarter))
            get_transaction_value_by_districts = pd.DataFrame(cursor_db.fetchall(), columns=['Districts', 'Transactions'])
            return get_transaction_value_by_districts
        except Exception as e:
            return "Error in fetching get_transaction_value_by_districts : " + str(e)

    def get_transaction_value_by_pincodes(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = '''
                SELECT DISTINCT StateOrDistrictorPincodeValue, NoOfTransactions AS Transactions 
                FROM TopTransforms 
                WHERE Entity='pincodes' AND Year=%s AND Quarter=%s 
                ORDER BY NoOfTransactions DESC 
                LIMIT 10;
            '''

            cursor_db.execute(select_query, (year, quarter))
            get_transaction_value_by_pincodes = pd.DataFrame(cursor_db.fetchall(), columns=['Pincodes', 'Transactions'])
            return get_transaction_value_by_pincodes
        except Exception as e:
            return "Error in fetching get_transaction_value_by_pincodes : " + str(e)

    def sum_of_AggregatedUsers(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = f'''SELECT DISTINCT TotalRegisteredUsers AS TotalRegisteredUsers FROM AggregatedUsers WHERE Year=%s and Quarter=%s and State is NULL'''
            params = [year, quarter]

            cursor_db.execute(select_query, params)
            #print(select_query)
            sum_of_AggregatedUsers = cursor_db.fetchone()
            return sum_of_AggregatedUsers[0]
        except Exception as e:
            return("Error in fetching sum_of_AggregatedUsers : "+str(e))

    def sum_of_AppOpenUsers(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = f'''SELECT NoOfAppOpensByUsers AS NoOfAppOpensByUsers FROM AggregatedUsers WHERE Year=%s AND Quarter=%s AND State is NULL'''
            params = [year, quarter]

            cursor_db.execute(select_query, params)
            #print(select_query)
            sum_of_AppOpenUsers = cursor_db.fetchone()
            return sum_of_AppOpenUsers[0]
        except Exception as e:
            return("Error in fetching sum_of_AppOpenUsers : "+str(e))

    def get_user_value_by_states(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = '''
                SELECT StateOrDistrictorPincodeValue, SUM(TotalRegisteredUsers) AS TotalRegisteredUsers
                FROM TopUsers
                WHERE Entity='states' AND Year=%s AND Quarter=%s
                GROUP BY StateOrDistrictorPincodeValue
                ORDER BY SUM(TotalRegisteredUsers) DESC
                LIMIT 10
            '''

            cursor_db.execute(select_query, (year, quarter))
            get_transaction_value_by_pincodes = pd.DataFrame(cursor_db.fetchall(), columns=['State', 'Count of users'])
            return get_transaction_value_by_pincodes
        except Exception as e:
            return "Error in fetching get_user_value_by_states : " + str(e)

    def get_user_value_by_districts(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = """
                SELECT REPLACE(StateOrDistrictName, 'district', ''), SUM(TotalRegisteredUsers) 
                FROM MappedUsers WHERE Year=%s AND Quarter = %s AND StateOrDistrictName like '%district'
                GROUP BY StateOrDistrictName 
                ORDER BY SUM(TotalRegisteredUsers) DESC 
                LIMIT 10;
            """

            cursor_db.execute(select_query, (year, quarter))
            get_user_value_by_districts = pd.DataFrame(cursor_db.fetchall(), columns=['Districts', 'Count of users'])
            #self.close_connection(cursor_db, db_conn)
            return get_user_value_by_districts
        except Exception as e:
            return "Error in fetching get_user_value_by_districts : " + str(e)

    def get_user_value_by_pincodes(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = """
                    SELECT StateOrDistrictorPincodeValue, TotalRegisteredUsers 
                    FROM TopUsers 
                    WHERE Year=%s AND Quarter = %s AND Entity='pincodes' AND State is NULL;
                """

            cursor_db.execute(select_query, (year, quarter))
            get_user_value_by_pincodes = pd.DataFrame(cursor_db.fetchall(), columns=['Pincodes', 'Count of users'])
            #self.close_connection(cursor_db, db_conn)
            return get_user_value_by_pincodes
        except Exception as e:
            return "Error in fetching get_user_value_by_pincodes : " + str(e) 

    def get_states(self, cursor_db, db_conn):
        try:
            select_query = """
                SELECT StateOrDistrictName FROM MappedTransforms 
                WHERE StateOrDistrictName NOT LIKE '%district' 
                GROUP BY StateOrDistrictName 
                ORDER BY StateOrDistrictName;
            """

            cursor_db.execute(select_query)
            get_states = pd.DataFrame(cursor_db.fetchall(), columns=['States'])
            return get_states['States']
        except Exception as e:
            return "Error in fetching get_states : " + str(e)

    def get_transaction_by_states_plot(self, cursor_db, db_conn, year, quarter, states):
        try:
            states = [state.strip() for state in states.split(',')]
            placeholders = ', '.join(['%s' for _ in states])
            select_query = f'''
                SELECT StateOrDistrictName, SUM(NoOfTransactions) AS NoOfTransactions, SUM(TotalTransactionValue) AS TotalTransactionValue 
                FROM MappedTransforms 
                WHERE Year=%s AND Quarter=%s AND StateOrDistrictName NOT LIKE '%district' AND StateOrDistrictName in ({placeholders})
                GROUP BY StateOrDistrictName 
                ORDER BY StateOrDistrictName;
            '''
            params = [year, quarter]
            params.extend(states)
            cursor_db.execute(select_query, params)
            get_transaction_by_states_plot = pd.DataFrame(cursor_db.fetchall(), columns=['State', 'NoOfTransactions', 'TotalTransactionValue'])
            return get_transaction_by_states_plot
        except Exception as e:
            return "Error in fetching get_transaction_by_states_plot : " + str(e)
    
    def get_users_by_states_plot(self, cursor_db, db_conn, year, quarter, states):
        try:
            states = [state.strip() for state in states.split(',')]
            placeholders = ', '.join(['%s' for _ in states])
            select_query = f'''
                SELECT StateOrDistrictName, SUM(TotalRegisteredUsers) AS TotalRegisteredUsers, SUM(NoOfAppOpensByUsers) AS NoOfAppOpensByUsers
                FROM MappedUsers 
                WHERE Year=%s AND Quarter=%s AND StateOrDistrictName NOT LIKE '%district' AND StateOrDistrictName in ({placeholders})
                GROUP BY StateOrDistrictName 
                ORDER BY StateOrDistrictName;
            '''
            params = [year, quarter]
            params.extend(states)
            cursor_db.execute(select_query, params)
            get_users_by_states_plot = pd.DataFrame(cursor_db.fetchall(), columns=['State', 'TotalRegisteredUsers', 'NoOfAppOpensByUsers'])
            return get_users_by_states_plot
        except Exception as e:
            return "Error in fetching get_users_by_states_plot : " + str(e)
    
    def get_top_transaction_by_states(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = """
                SELECT StateOrDistrictName, NoOfTransactions, TotalTransactionValue               
                FROM MappedTransforms                  
                WHERE Year=%s AND Quarter=%s and StateOrDistrictName not like '%district'                
                ORDER BY NoOfTransactions DESC                  
                LIMIT 10;
            """

            cursor_db.execute(select_query, (year, quarter))
            get_top_transaction_by_states = pd.DataFrame(cursor_db.fetchall(), columns=['State', 'NoOfTransactions', 'TotalTransactionValue'])
            return get_top_transaction_by_states
        except Exception as e:
            return "Error in fetching get_top_transaction_by_states : " + str(e)
    
    def get_top_transaction_by_districts(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = """
                SELECT REPLACE(StateOrDistrictName, 'district', ''), NoOfTransactions, TotalTransactionValue                   
                FROM MappedTransforms                  
                WHERE Year=%s AND Quarter=%s and StateOrDistrictName like '%district'                
                ORDER BY NoOfTransactions DESC                  
                LIMIT 10;
            """

            cursor_db.execute(select_query, (year, quarter))
            get_top_transaction_by_districts = pd.DataFrame(cursor_db.fetchall(), columns=['Districts', 'NoOfTransactions', 'TotalTransactionValue'])
            return get_top_transaction_by_districts
        except Exception as e:
            return "Error in fetching get_top_transaction_by_districts : " + str(e)

    def get_top_transaction_by_pincodes(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = '''
                SELECT DISTINCT StateOrDistrictorPincodeValue, NoOfTransactions, TotalTransactionValue
                FROM TopTransforms 
                WHERE Entity='pincodes' AND Year=%s AND Quarter=%s 
                ORDER BY NoOfTransactions DESC 
                LIMIT 10;
            '''

            cursor_db.execute(select_query, (year, quarter))
            get_top_transaction_by_pincodes = pd.DataFrame(cursor_db.fetchall(), columns=['Pincodes', 'NoOfTransactions', 'TotalTransactionValue'])
            return get_top_transaction_by_pincodes
        except Exception as e:
            return "Error in fetching get_top_transaction_by_pincodes : " + str(e)

    def get_user_value_by_states(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = '''
                SELECT StateOrDistrictorPincodeValue, SUM(TotalRegisteredUsers) AS TotalRegisteredUsers
                FROM TopUsers
                WHERE Entity='states' AND Year=%s AND Quarter=%s
                GROUP BY StateOrDistrictorPincodeValue
                ORDER BY SUM(TotalRegisteredUsers) DESC
                LIMIT 10
            '''

            cursor_db.execute(select_query, (year, quarter))
            get_transaction_value_by_pincodes = pd.DataFrame(cursor_db.fetchall(), columns=['State', 'Count of users'])
            return get_transaction_value_by_pincodes
        except Exception as e:
            return "Error in fetching get_user_value_by_states : " + str(e)

    def get_user_value_by_districts(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = """
                SELECT REPLACE(StateOrDistrictName, 'district', ''), SUM(TotalRegisteredUsers) 
                FROM MappedUsers WHERE Year=%s AND Quarter = %s AND StateOrDistrictName like '%district'
                GROUP BY StateOrDistrictName 
                ORDER BY SUM(TotalRegisteredUsers) DESC 
                LIMIT 10;
            """

            cursor_db.execute(select_query, (year, quarter))
            get_user_value_by_districts = pd.DataFrame(cursor_db.fetchall(), columns=['Districts', 'Count of users'])
            #self.close_connection(cursor_db, db_conn)
            return get_user_value_by_districts
        except Exception as e:
            return "Error in fetching get_user_value_by_districts : " + str(e)

    def get_user_value_by_pincodes(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = """
                    SELECT StateOrDistrictorPincodeValue, TotalRegisteredUsers 
                    FROM TopUsers 
                    WHERE Year=%s AND Quarter = %s AND Entity='pincodes' AND State is NULL;
                """

            cursor_db.execute(select_query, (year, quarter))
            get_user_value_by_pincodes = pd.DataFrame(cursor_db.fetchall(), columns=['Pincodes', 'Count of users'])
            #self.close_connection(cursor_db, db_conn)
            return get_user_value_by_pincodes
        except Exception as e:
            return "Error in fetching get_user_value_by_pincodes : " + str(e) 

    def get_user_value_by_devices(self, cursor_db, db_conn, year, quarter):
        try:
            select_query = """
                    SELECT DeviceBrand, SUM(RegisteredUsersByBrand) 
                    FROM AggregatedUsers WHERE Year=%s AND Quarter=%s 
                    GROUP BY DeviceBrand 
                    ORDER BY SUM(RegisteredUsersByBrand) DESC 
                    LIMIT 10;
                """

            cursor_db.execute(select_query, (year, quarter))
            get_user_value_by_devices = pd.DataFrame(cursor_db.fetchall(), columns=['DeviceBrand', 'Registered users'])
            #self.close_connection(cursor_db, db_conn)
            return get_user_value_by_devices
        except Exception as e:
            return "Error in fetching get_user_value_by_devices : " + str(e) 
