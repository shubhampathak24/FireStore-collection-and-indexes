import yaml
import os,sys
from google.cloud import firestore
import time
import logging
from pathlib import Path
from datetime import datetime
from google.cloud import firestore_admin_v1
from google.cloud.firestore_admin_v1.types import Index
from google.cloud.firestore_admin_v1.services.firestore_admin.client import FirestoreAdminClient
from google.cloud import storage
from google.api_core import client_options


def read_yml_file(path_to_file):
    with open(path_to_file, "r") as stream:
        try:
            sf_schema_object_yaml = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return sf_schema_object_yaml
    
def check_collection(client,collection_name):
    try:
        collections = client.collections()
        list_collection = []
        for collection in collections:
            list_collection.append(collection.id)
        
        if collection_name in list_collection:
            print(f'Collection {collection_name} already exists.')
            logging.info(f'Collection {collection_name} already exists.')
            return True
        else:
            print(f'Collection {collection_name} doesn"t exists.')
            logging.info(f'Collection {collection_name} doesn"t exists.')
            return False
    except Exception as e:
        print(f"Failed with exception {e}")
        logging.error(f"Failed with exception {e}")


def check_index(fsAdmin_client,proj_id,firestore_collection_name):
    indxs = fsAdmin_client.list_indexes(parent = f'projects/{proj_id}/databases/(default)/collectionGroups/{firestore_collection_name}')
    for obj in indxs:
        collection_name = obj.name.split('/')[5]
        if len(collection_name) == len(firestore_collection_name):
            if obj.name.find(firestore_collection_name) != -1:
                return obj.name, True
            else:
                return obj.name, False

def upload_log(blob_log_path,Logfile_name,full_path_to_logfile):
    try:
        bucket = storage_client.get_bucket(log_bucket)
        file_log_path = blob_log_path + "/" + Logfile_name
        bucket.blob(file_log_path).upload_from_filename(full_path_to_logfile)
        
    except Exception as e:
        print(e)
        logging.info(e)
        sys.exit(1)  
               

def main():
    try:
        client = firestore.Client(project = proj_id)
        options = client_options.ClientOptions(quota_project_id=proj_id)
        fsAdmin_client = FirestoreAdminClient(client_options = options)
        firestore_yml_full_path = 'Path for YML file' # YML file full path
        firestore_yml = read_yml_file(firestore_yml_full_path)
        firestore_collection_list = []
        for collection in firestore_yml['Firestore_Collection']:
            firestore_collection_dict = {}
            for obj,obj_details in collection.items():
                firestore_collection_dict['firestore_collection_name'] = obj
                for key,value in obj_details.items():
                    firestore_collection_dict[key] = value
            firestore_collection_list.append(firestore_collection_dict)
        for collection_dict in firestore_collection_list:
            if not(check_collection(client,collection_dict['firestore_collection_name'])):
                if collection_dict['create_index'] == 'Y':
                    #Below snippet can be used to check if index already exists and delete it.
                    obj_name, ind_exists = check_index(fsAdmin_client,proj_id,collection_dict["firestore_collection_name"])
                    if ind_exists:
                        try:
                            print(f'Index {collection_dict["firestore_collection_name"]} already exists.')
                            fsAdmin_client.delete_index(name = obj_name)
                            print(f'Index {collection_dict["firestore_collection_name"]} deleted.')
                            time.sleep(10)
                        except Exception as e:
                            print(f"Failed with exception {e}")
                            logging.error(f"Failed with exception {e}")

                
                    index_fields = collection_dict['indexes'].split(',')
                    index = {}
                    for ind in index_fields:
                        index[ind.strip()] = ''
                    try:
                        client.collection(collection_dict['firestore_collection_name']).document().set(index)
                        print(f'{collection_dict["firestore_collection_name"]} is created with default document and index keys.')
                        logging.info(f'{collection_dict["firestore_collection_name"]} is created with default document and index keys.')
                    except Exception as e:
                        print(f"Failed with exception {e}")
                        logging.error(f"Failed with exception {e}")
                    parent_used = f'projects/{proj_id}/databases/(default)/collectionGroups/{collection_dict["firestore_collection_name"]}'
                    fld = []
                    try:
                        for key, value in collection_dict['index_order'].items():
                            if value.upper() == 'ASCENDING':
                                fld.append(Index.IndexField(field_path = key, order = Index.IndexField.Order.ASCENDING))
                            
                            elif value.upper() == 'DESCENDING':
                                fld.append(Index.IndexField(field_path = key, order = Index.IndexField.Order.DESCENDING))
                                
                            else:
                                fld.append(Index.IndexField(field_path = key, order = Index.IndexField.Order.ORDER_UNSPECIFIED))
                        req_index = Index(query_scope = collection_dict['query_scope'], fields = fld)
                        create_index_operation = fsAdmin_client.create_index(parent = parent_used, index = req_index)
                        print(f'Index {collection_dict["firestore_collection_name"]} has been created with keys and order : {collection_dict["index_order"]}')
                        logging.info(f'Index {collection_dict["firestore_collection_name"]} has been created with keys and order : {collection_dict["index_order"]}')
                        run_completed = False
                        while( not(run_completed) ):
                            time.sleep(60)
                            run_completed = create_index_operation.running()
                    except Exception as e:
                        print(f"Failed with exception {e}")
                        logging.error(f"Failed with exception {e}")
                else:
                    try:
                        client.collection(collection_dict['firestore_collection_name']).document().set({})
                        print(f'{collection_dict["firestore_collection_name"]} is created with emtpy document as "create_index" is not set as "Y"')
                        logging.info(f'{collection_dict["firestore_collection_name"]} is created with emtpy document as "create_index" is not set as "Y"')
                    except Exception as e:
                        print(f"Failed with exception {e}")
                        logging.error(f"Failed with exception {e}")
            
            else:
                print(f'Collection {collection_dict["firestore_collection_name"]} already present in FireStore.')
                logging.info(f'Collection {collection_dict["firestore_collection_name"]} already present in FireStore.')
                        
    except Exception as e:
        print(f"Firestore collection creating failed with error: {e}")
        logging.error(f"Firestore collection creating failed with error: {e}")


if __name__ == "__main__":
    Logfile_name = 'firestore_collection_log_'+datetime.now().strftime("%Y%m%d%H%M%S")+'.log'
    full_path_to_logfile = os.path.join( os.getcwd(), Logfile_name )
    logging.basicConfig(
        filename=Logfile_name,
        format='%(asctime)s %(filename)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        filemode='w'
    )
    logging.info(f"**********Started executing the script ************")
    
    proj_id = 'This variable should conatins the project ID'#Project ID should be kept here.
    print('**** GCP Storage Setup ****')
    logging.info('********* GCP Storage Setup ***********')
    try:
        storage_client = storage.Client(project=proj_id)
        print( "INFO: storage client created" )
    except Exception as e:
        print( e )
        exit(1)
    print("GCP Storage connection successful")
    logging.info("GCP Storage connection successful")
    log_bucket = 'GCS logging bucket name' # GCS bucket name should be kept here for logging.
    blob_log_path = "firestore_collection" # Folder path to store logs.
    file_log_path = blob_log_path + "/" + Logfile_name
    log_file_with_blob_path = f'gs://{log_bucket}/' + str(file_log_path)
    
    main()
    logging.info(f"**********Completed executing the script ************")
    upload_log(blob_log_path,Logfile_name,full_path_to_logfile)
    print( "Blob Log File with Path: " + log_file_with_blob_path)
