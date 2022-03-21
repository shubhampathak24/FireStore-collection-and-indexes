import yaml
import os,sys
from google.cloud import firestore
import time
from pathlib import Path
from datetime import datetime
from google.cloud import firestore_admin_v1
from google.cloud.firestore_admin_v1.types import Index
from google.cloud.firestore_admin_v1.services.firestore_admin.client import FirestoreAdminClient
from google.cloud import storage
from google.api_core import client_options

    
def check_collection(client,collection_name):
    collections = client.collections()
    list_collection = []
    for collection in collections:
        list_collection.append(collection.id)

    if collection_name in list_collection:
        print(f'Collection {collection_name} already exists.')
        return True
    else:
        print(f'Collection {collection_name} doesn"t exists.')
        return False


def check_index(fsAdmin_client,proj_id,collection_name):
    indxs = fsAdmin_client.list_indexes(parent = f'projects/{proj_id}/databases/(default)/collectionGroups/{collection_name}')
    for obj in indxs:
        collection_name = obj.name.split('/')[5]
        if len(collection_name) == len(collection_name):
            if obj.name.find(collection_name) != -1:
                return obj.name, True
            else:
                return obj.name, False

               

client = firestore.Client(project = proj_id)
options = client_options.ClientOptions(quota_project_id=proj_id)
fsAdmin_client = FirestoreAdminClient(client_options = options)

#collection_dict contains your variable entries to create a firestore collection.
collection_dict = {'collection_name' : 'collection_1', 'indexes' : 'field1,field2', 'create_index' : 'Y', 'index_order' : {'field1' : 'ASCENDING', 'field2' : 'DESCENDING'}}, 'query_scope' : 'COLLECTION'}

if not(check_collection(client,collection_dict['collection_name'])):
    if collection_dict['create_index'] == 'Y':
        #Below snippet can be used to check if index already exists and delete it.
        obj_name, ind_exists = check_index(fsAdmin_client,proj_id,collection_dict["collection_name"])
        if ind_exists:
            try:
                print(f'Index {collection_dict["collection_name"]} already exists.')
                fsAdmin_client.delete_index(name = obj_name)
                print(f'Index {collection_dict["collection_name"]} deleted.')
                time.sleep(10)
            except Exception as e:
                print(f"Failed with exception {e}")


        index_fields = collection_dict['indexes'].split(',')
        index = {}
        for ind in index_fields:
            index[ind.strip()] = ''
        try:
            client.collection(collection_dict['collection_name']).document().set(index)
            print(f'{collection_dict["collection_name"]} is created with default document and index keys.')
        except Exception as e:
            print(f"Failed with exception {e}")
        parent_used = f'projects/{proj_id}/databases/(default)/collectionGroups/{collection_dict["collection_name"]}'
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
            print(f'Index {collection_dict["collection_name"]} has been created with keys and order : {collection_dict["index_order"]}')
            run_completed = False
            while( not(run_completed) ):
                time.sleep(60)
                run_completed = create_index_operation.running()
        except Exception as e:
            print(f"Failed with exception {e}")
    else:
        try:
            client.collection(collection_dict['collection_name']).document().set({})
            print(f'{collection_dict["collection_name"]} is created with emtpy document as "create_index" is not set as "Y"')
        except Exception as e:
            print(f"Failed with exception {e}")

else:
    print(f'Collection {collection_dict["collection_name"]} already present in FireStore.')

