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

               

client = firestore.Client(project = proj_id)
options = client_options.ClientOptions(quota_project_id=proj_id)
fsAdmin_client = FirestoreAdminClient(client_options = options)

#collection_dict contains your variable entries to create a firestore collection.
collection_dict = {'collection_name' : 'collection_1', 'indexes' : 'field1,field2', 'index_order' : {'field1' : 'ASCENDING', 'field2' : 'DESCENDING'}}, 'query_scope' : 'COLLECTION'}

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

