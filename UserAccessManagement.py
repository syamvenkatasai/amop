import json
import requests
import traceback
import os
import sqlalchemy
import pandas as pd
import psutil
from datetime import datetime, timedelta
from db_utils import DB
from db_utils import DB
from flask import Flask, request, jsonify
from time import time as tt
from sqlalchemy.orm import sessionmaker
from hashlib import sha256
from elasticsearch_utils import elasticsearch_search
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from logging_utils import Logging
from app import app
from datetime import datetime

logging = Logging(name='UserAccessManagement')

db_config = {
    'host': os.environ['HOST_IP'],
    'port': os.environ['LOCAL_DB_PORT'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD']
}

def http_transport(encoded_span):
    body = encoded_span
    requests.post(
        'http://servicebridge:80/zipkin',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )

def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes

#Audit Framework and Error Management Framework
def insert_data_dict_into_Table(database,table,data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    database = DB(database, **db_config)
    database.insert_dict(data, table)
    return True


group_access_db="need to connnect to database"


def delete_user(data, group_access_db):

    try:
        try:
            user_name = data['username']
        except:
            traceback.print_exc()
            message = "id not present in request data."
            return {"flag": False, "message" : message}
        
        
        Users_directory_query = f"DELETE FROM `Users_directory` WHERE `username` = '{user_name}'" 
        result = group_access_db.execute(Users_directory_query)
        if not result:
            message = f"Something went wrong while deleting the user {user_name}  from Users_directory"
            return {"flag": False, "message" : message}

        message = f"Successfully deleted {user_name}"
        return {"flag": True, "message" : message}
    
    except Exception as e:
        logging.error(F"Something went wrong and error is {e}")
        message = f"Something went wrong while deleting user_name {user_name}."
        return {"flag": False, "message" : message}



def edit_user(data,group_access_db):

    try:
        try:
            user_name= data['username']
            edited_details=data['edited_details']
        except:
            traceback.print_exc()
            message = "id not present in request data."
            return {"flag": False, "message" : message}
    
        set_clause_arr = []
        for set_column, set_value in edited_details.items():
            set_clause_arr.append(f"`{set_column}` = '{set_value}'")
        set_clause_string = ', '.join(set_clause_arr)
            
        
        group_access_db.execute(f"UPDATE `Users_directory` SET {set_clause_string} WHERE `username` = '{user_name}'")
        message=F"user changes are updated Sucessfully"
        return {"flag": True, "message" : message}
    

    except Exception as e:
        logging.error(F"Something went wrong and error is {e}")
        message = f"Something went wrong while editing user_name {user_name}."
        return {"flag": False, "message" : message}
    
    


def fetch_user_roles(database):
    #The function is used to fetch user from User_Role_Site    
    try:
        query = f"SELECT roles FROM `User_Role_Site`'"
        df = database.execute_(query)
        # Extract the 'role' values from the DataFrame and convert them to a list
        roles = df['role'].tolist()
        return  roles   
    except Exception as e:
        logging.error(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting roles."
        return {"flag": False, "message" : message}
    


def fetch_tenants(database):
    #The function is used to  fetch tenants from Tenants table    
    try:
        query = f"SELECT tenants FROM `Tenants`'"
        df = database.execute_(query)
        # Extract the 'tenants' values from the DataFrame and convert them to a list
        tenants = df['tenants'].tolist()
        return  tenants   
    except Exception as e:
        logging.error(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting tenants."
        return {"flag": False, "message" : message}



               
def creating_the_user(data, database):
    #The function is used to create user     
    try:

        try:
            user_details=data['user_details']
        except:
            traceback.print_exc()
            message = "id not present in request data."
            return {"flag": False, "message" : message}
    
        try:
            create_user_query = generate_insert_query(user_details, 'Users_directory')
            database.execute(create_user_query)    
        except sqlalchemy.exc.IntegrityError:
            traceback.print_exc()
            message = "Duplicate entry for username"
            return {"flag": False, "message" : message}
        except:
            traceback.print_exc()
            message = f"Something went wrong while creating user_name"
            return {"flag": False, "message" : message}
        
        message="user created sucessfully"
        return  {"flag": True, "message" : message}
          
    except Exception as e:
        logging.error(F"Something went wrong and error is {e}")
        message = "Something went wrong while adding user_name"
        return {"flag": False, "message" : message}



def generate_insert_query(dict_data, table_name):
    columns_list,values_list = [],[]
    logging.debug(f"dict_data: {dict_data}")

    for column, value in dict_data.items():
        columns_list.append(f"`{column}`")
        values_list.append(f"'{value}'")

    columns_list = ', '.join(columns_list)
    values_list= ', '.join(values_list)

    insert_query = f"INSERT INTO `{table_name}` ({columns_list}) VALUES ({values_list})"
    
    return insert_query









'''
@Author:Amop Team
@Description:The route is used to edit , create  the users in the User Management Module
'''
@app.route("/user", methods=['POST', 'GET'])
def user():
    #retrieving the request parameters
    data = request.json
    logging.info(f'Request data in modify_user: {data}')
    tenant_id = data.get('tenant_id',None)
    session_id = data.get('session_id', None)
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    try:
        operation = data.pop('operation').lower()
        row_data = data.pop('rowData')
        user_name = row_data.get('username', None)
        updated_user= data.pop('user','')
    except:
        message = "Received unexpected request data."
        result={"flag": False, "message" : message}
    
    trace_id = generate_random_64bit_string()
    attr = ZipkinAttrs(
            trace_id=trace_id,
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='UserAccessManagement',
            zipkin_attrs=attr,
            span_name='modify_user',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        
        db_config['tenant_id'] = tenant_id
        group_access_db = DB('group_access', **db_config)
        queue_db = DB('queues', **db_config)
        current_time = datetime.now()
        current_time = current_time + timedelta(hours=5, minutes=30)
        formatted_time = current_time.strftime("%d-%b-%y %H:%M:%S")
        
        if operation == 'edit':
            user_id=data.pop('id')
            result = edit_user(user_id,row_data, group_access_db,updated_user,formatted_time)
            
        elif operation == 'create':
            result = create_user(row_data, group_access_db, queue_db,updated_user,formatted_time)
        
        else:
            result = {'message':'Didnot receive proper operator'}
        return_data = result
            
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        logging.info(f" #### um info return data got is {return_data}")
        #Inserting into  audit table
        #change the keys and values accordingly
        data = {"tenant_id": tenant_id, "user": user_name,
                        "api_service": "modify_user", "service_container": "user_management", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": str(return_data['message']), "trace_id": trace_id, "session_id": session_id,"status":str(return_data['flag'])}
        insert_data_dict_into_Table('database_name','audit',data)

        return jsonify(result)

def master_search(tenant_id, text, table_name, start_point, offset, columns_list, header_name):
    elastic_input = {}
    
    elastic_input['columns'] = columns_list
    elastic_input['start_point'] = start_point
    elastic_input['size'] = offset
    if header_name:
        elastic_input['filter'] = [{'field': header_name, 'value': "*" + text + "*"}]
    else:
        elastic_input['text'] = text
    elastic_input['source'] = table_name
    elastic_input['tenant_id'] = tenant_id
    files, total = elasticsearch_search(elastic_input)
    return files, total




'''
@Author:Amop Team
@Description:The route is used to edit , create  the users in the User Management Module
'''
@app.route("/displayusers", methods=['POST', 'GET'])
def displayusers():
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.pop('tenant_id', None)
    session_id = data.get('session_id', None)

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    username = data.pop('user',None)
    attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='UserAccessManagement',
            zipkin_attrs=attr,
            span_name='displayusers',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        if not username:
            message = "Session Expired, page inaccessible"
            return jsonify({"flag": False, "message" : message})

        
        db_config['tenant_id'] = tenant_id
        flag = data.pop('flag', None)
        group_access_db = DB('group_access', **db_config)
        #Check if sessions is active
        q = "SELECT count(id) as cnt from Live_sessions WHERE `user` = %s and `status` = 'active'"
        params = [username]
        active_df = group_access_db.execute_(q,params = params)
        active_count = int(active_df['cnt'][0])

        if active_count < 1:
            message = "Session Expired, page inaccessible"
            return jsonify({"flag": False, "message" : message})
        
        if flag == 'search':
            try:
                text = data['data'].pop('search_word')
                table_name = data['data'].pop('table_name', 'Users_directory')
                start_point = data['data']['start'] - 1
                end_point = data['data']['end']
                header_name = data['data'].get('column', None)
                offset = end_point - start_point
            except:
                traceback.print_exc()
                message = f"Input data is missing "
                response_data={"flag": False, "message" : message}    
            
            table_name = 'Users_directory'
            columns_list = list(group_access_db.execute_(f"SHOW COLUMNS FROM `{table_name}`")['Field'])
       
            files, total = master_search(tenant_id = tenant_id, text = text, table_name = table_name, start_point = 0, offset = 10, columns_list = columns_list, header_name=header_name)
            active_directory_query = f"SELECT * FROM `Users_directory`"
            active_directory_df = group_access_db.execute_(active_directory_query)
            active_directory_dict = active_directory_df.to_dict(orient= 'records')  

            if end_point > total:
                end_point = total
            if start_point == 1:
                pass
            else:
                start_point += 1
            
            pagination = {"start": start_point, "end": end_point, "total": total}
            
            response_data = {"flag": True, "data": files, "pagination": pagination}
        elif flag=='showusers':
            try:
                active_directory_query = f"SELECT * FROM `Users_directory`"
                active_directory_df = group_access_db.execute_(active_directory_query)  
                columns_to_remove = ['password', 'status', 'id']
                columns_to_display = list(active_directory_df.columns)
                for element in columns_to_remove:
                    columns_to_display.remove(element)
                active_directory_dict = active_directory_df.to_dict(orient= 'records')      
            except:
                traceback.print_exc()
                message = "Could not load from Active Directory"
                response_data = {"flag": False, "message" : message}
            field_definition_query = f"SELECT * FROM `field_definition` WHERE `status` = 1"
            field_definition_df = group_access_db.execute_(field_definition_query)
            headers_list = []
            for header in columns_to_display:
                try:
                    display_name = list(field_definition_df[field_definition_df['unique_name'] == header].display_name)[0]
                    headers_list.append({'display_name': display_name, 'unique_name': header})
                except:
                    traceback.print_exc()
                    logging.error(f"Check configuration for {header}")
            
            data = {
                "header" : headers_list,
                "rowdata" : active_directory_dict,
            }
            
            response_data = {"flag": True, "data" : data}
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
           # time_consumed = str(end_time-start_time)
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        
        logging.info(f"##For show existing users Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(response_data)