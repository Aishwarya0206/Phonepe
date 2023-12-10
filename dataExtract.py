import os
import json
import datetime
import pandas as pd

class dataExtract:
    def __init__(self):
        pass

    def process_agg_transaction_files(self, directory):
        combined_data = pd.DataFrame()  # Initialize an empty DataFrame
        # Iterate through each file/folder in the directory
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            # If it's a directory, recursively call the function
            if os.path.isdir(item_path):
                subdir_data = self.process_agg_transaction_files(item_path)
                combined_data = pd.concat([combined_data, subdir_data], ignore_index=True)
            elif item.endswith('.json'):  # If it's a JSON file
                # Read and process the JSON file
                with open(item_path, 'r') as file:
                    try:
                        data = json.load(file)
                        transaction_data = data['data']['transactionData']
                        for entry in transaction_data:
                            payment_instruments = entry['paymentInstruments']
                            for payment in payment_instruments:
                                entity = {
                                    'from': datetime.datetime.fromtimestamp(data['data']['from']/1000),
                                    'to': datetime.datetime.fromtimestamp(data['data']['to']/1000),
                                    'transaction': entry['name'],
                                    'count': payment['count'],
                                    'amount': payment['amount'],
                                    'fileName': int(item.split('.')[0]),
                                    'stateFolder': None if(item_path.split('/')[-3] == 'india') else item_path.split('/')[-3].replace('-', ' ').capitalize(),
                                    'folder': int(item_path.split('/')[-2])
                                }
                                df = pd.DataFrame([entity])
                                combined_data = pd.concat([combined_data, df], ignore_index=True)  # Concatenate DataFrames
                        
                    except Exception as e:
                        print(f"Error processing file {item_path}: {e}")
        
        return combined_data

    def process_agg_user_files(self, directory):
        combined_data = pd.DataFrame()  # Initialize an empty DataFrame
        # Iterate through each file/folder in the directory
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            # If it's a directory, recursively call the function
            if os.path.isdir(item_path):
                subdir_data = self.process_agg_user_files(item_path)
                combined_data = pd.concat([combined_data, subdir_data], ignore_index=True)
            elif item.endswith('.json'):  # If it's a JSON file
                # Read and process the JSON file
                with open(item_path, 'r') as file:
                    try:
                        data = json.load(file)
                        user_data = data['data']['usersByDevice'] #if(data['data']['usersByDevice'] != 'null') else data['data']
                        #print(user_data)
                        if(user_data is not None):
                            for entry in user_data:
                                entity = {
                                        'registeredUsers': data['data']['aggregated']['registeredUsers'],
                                        'appOpens': data['data']['aggregated']['appOpens'],
                                        'brand': entry['brand'] if 'brand' in entry else None,
                                        'count': entry['count'] if 'count' in entry else None,
                                        'percentage': entry['percentage'] if 'percentage' in entry else None,
                                        'fileName': int(item.split('.')[0]),
                                        'stateFolder': None if(item_path.split('/')[-3] == 'india') else item_path.split('/')[-3].replace('-', ' ').capitalize(),
                                        'folder': int(item_path.split('/')[-2])
                                    }
                                
                                df = pd.DataFrame([entity])
                                combined_data = pd.concat([combined_data, df], ignore_index=True)
                        # else:
                        #     entity = {
                        #         'registeredUsers': data['data']['aggregated']['registeredUsers'],
                        #         'appOpens': data['data']['aggregated']['appOpens'],
                        #         'brand': None,
                        #         'count': None,
                        #         'percentage': None,
                        #         'fileName': int(item.split('.')[0]),
                        #         'stateFolder': None if item_path.split('/')[-3] == 'india' else item_path.split('/')[-3],
                        #         'folder': int(item_path.split('/')[-2])
                        #         }
                        #     df = pd.DataFrame([entity])
                        #     combined_data = pd.concat([combined_data, df], ignore_index=True)  # Concatenate DataFrames
                            
                        
                    except Exception as e:
                        print(f"Error processing file {item_path}: {e}")
        
        return combined_data

    def process_map_transaction_files(self, directory):
        combined_data = pd.DataFrame()  # Initialize an empty DataFrame
        # Iterate through each file/folder in the directory
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            # If it's a directory, recursively call the function
            if os.path.isdir(item_path):
                subdir_data = self.process_map_transaction_files(item_path)
                combined_data = pd.concat([combined_data, subdir_data], ignore_index=True)
            elif item.endswith('.json'):  # If it's a JSON file
                # Read and process the JSON file
                with open(item_path, 'r') as file:
                    try:
                        data = json.load(file)
                        hover_data = data['data']['hoverDataList']
                        for entry in hover_data:
                            metric_entry = entry['metric']
                            for metric_ent in metric_entry:
                                entity = {
                                    'name': entry['name'].capitalize(),
                                    'count': metric_ent['count'],
                                    'amount': metric_ent['amount'],
                                    'fileName': int(item.split('.')[0]),
                                    'stateFolder': None if(item_path.split('/')[-3] == 'india') else item_path.split('/')[-3].replace('-', ' ').capitalize(),
                                    'folder': int(item_path.split('/')[-2])
                                }
                                df = pd.DataFrame([entity])
                                combined_data = pd.concat([combined_data, df], ignore_index=True)  # Concatenate DataFrames
                        
                    except Exception as e:
                        print(f"Error processing file {item_path}: {e}")
        
        return combined_data

    def process_map_user_files(self, directory):
        combined_data = pd.DataFrame()  # Initialize an empty DataFrame
        # Iterate through each file/folder in the directory
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            # If it's a directory, recursively call the function
            if os.path.isdir(item_path):
                subdir_data = self.process_map_user_files(item_path)
                combined_data = pd.concat([combined_data, subdir_data], ignore_index=True)
            elif item.endswith('.json'):  # If it's a JSON file
                # Read and process the JSON file
                with open(item_path, 'r') as file:
                    try:
                        data = json.load(file)
                        hover_data = data['data']['hoverData']
                        
                        rows = []
                        for state, info in hover_data.items():
                            df = pd.DataFrame()  
                            df['stateName'] = state.capitalize(),
                            df['registeredUsers'] = info['registeredUsers'],
                            df['appOpens'] = info['appOpens'],
                            df['fileName'] = int(item.split('.')[0]),
                            df['stateFolder'] = None if(item_path.split('/')[-3] == 'india') else item_path.split('/')[-3].replace('-', ' ').capitalize(),
                            df['folder'] = int(item_path.split('/')[-2])
                            combined_data = pd.concat([combined_data, df], ignore_index=True) 
                    
                    except Exception as e:
                        print(f"Error processing JSON data: {e}")
        return combined_data

    def process_top_transaction_files(self, directory):
        combined_data = pd.DataFrame()  # Initialize an empty DataFrame
        # Iterate through each file/folder in the directory
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            # If it's a directory, recursively call the function
            if os.path.isdir(item_path):
                subdir_data = self.process_top_transaction_files(item_path)
                combined_data = pd.concat([combined_data, subdir_data], ignore_index=True)
            elif item.endswith('.json'):  # If it's a JSON file
                # Read and process the JSON file
                with open(item_path, 'r') as file:
                    try:
                        data = json.load(file)
                        top_states = data['data']
                        for key, values in top_states.items():
                            if isinstance(values, list):
                                for entry in values:
                                    metric = entry['metric']
                                    entity = {
                                        'entity': key,
                                        'entityName': entry['entityName'].capitalize(),
                                        'count': metric['count'],
                                        'amount': metric['amount'],
                                        'fileName': int(item.split('.')[0]),
                                        'stateFolder': None if item_path.split('/')[-3] == 'india' else item_path.split('/')[-3].replace('-', ' ').capitalize(),
                                        'folder': int(item_path.split('/')[-2])
                                    }
                                    df = pd.DataFrame([entity])
                                    combined_data = pd.concat([combined_data, df], ignore_index=True)# Concatenate DataFrames
                            # else:
                            #     entity =  {
                            #             'entity': key,
                            #             'entityName': None,
                            #             'count': None,
                            #             'amount': None,
                            #             'fileName': int(item.split('.')[0]),
                            #             'stateFolder': None if item_path.split('/')[-3] == 'india' else item_path.split('/')[-3],
                            #             'folder': int(item_path.split('/')[-2])
                            #         }
                            #     df = pd.DataFrame([entity])
                            #     combined_data = pd.concat([combined_data, df], ignore_index=True)# Concatenate DataFrames
                    except Exception as e:
                        print(f"Error processing file {item_path}: {e}")
        return combined_data

    def process_top_user_files(self, directory):
        combined_data = pd.DataFrame()  # Initialize an empty DataFrame
        # Iterate through each file/folder in the directory
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            # If it's a directory, recursively call the function
            if os.path.isdir(item_path):
                subdir_data = self.process_top_user_files(item_path)
                combined_data = pd.concat([combined_data, subdir_data], ignore_index=True)
            elif item.endswith('.json'):  # If it's a JSON file
                # Read and process the JSON file
                with open(item_path, 'r') as file:
                    try:
                        data = json.load(file)
                        top_states = data['data']
                        for key, values in top_states.items():
                            if isinstance(values, list):
                                for entry in values:
                                    entity = {
                                        'entityType': key,
                                        'entityName': entry['name'].capitalize(),
                                        'registeredUsers': entry['registeredUsers'],
                                        'fileName': int(item.split('.')[0]),
                                        'stateFolder': None if item_path.split('/')[-3] == 'india' else item_path.split('/')[-3].replace('-', ' ').capitalize(),
                                        'folder': int(item_path.split('/')[-2])
                                    }
                                    df = pd.DataFrame([entity])
                                    combined_data = pd.concat([combined_data, df], ignore_index=True)# Concatenate DataFrames
                            # else:
                            #     entity = {
                            #             'entityType': key,
                            #             'entityName': None,
                            #             'registeredUsers': None,
                            #             'fileName': int(item.split('.')[0]),
                            #             'stateFolder': None if item_path.split('/')[-3] == 'india' else item_path.split('/')[-3],
                            #             'folder': int(item_path.split('/')[-2])
                            #         }
                            #     df = pd.DataFrame([entity])
                            #     combined_data = pd.concat([combined_data, df], ignore_index=True)# Concatenate DataFrames
                    except Exception as e:
                        print(f"Error processing file {item_path}: {e}")
        return combined_data