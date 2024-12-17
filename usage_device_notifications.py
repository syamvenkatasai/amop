
import os

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
import pandas as pd

import math
#import logging
import time
from sqlalchemy import create_engine, exc,text

import time
import psycopg2
import uuid
import pandas as pd
import numpy as np
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv
import os
import ast
import re
import pytds
import json
from datetime import datetime, timedelta

import time
from psycopg2.extras import execute_values

#from common_utils.logging_utils import Logging
# from logging_utils import Logging
#logging = Logging()

class UsageNotifications:
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def create_connection_oldpyodbc(self,db_type='',host='', db_name='',username='', password='',port='',driver='',max_retry=3):
        connection = None
        retry = 1       
        # print(f"db_type:{db_type}, host--{host}-db_name-{db_name}, username-{username},password-{password},port-{port},driver-{driver}")
        
        if db_type=='postgresql':            
            try:
                print(f"creating postgresql connection")
                
                connection = psycopg2.connect(
                    host=host,
                    database=db_name,
                    user=username,
                    password=password,
                    port=port
                )
                print("Connection to PostgreSQL DB successful")
            except Exception as e:
                print(f"Failed to connect to PostgreSQL DB: {e}")
        elif db_type=='mssql':
            print(f"conn : {db_type},{host},{db_name},{username},{password},{driver},{port}")
            print("Creating MSSQL connection")
            print(f"Creating MSSQL connection")
            try:
                # connection_string= f"""DRIVER={driver};SERVER={host};DATABASE={db_name};UID={username};PWD={password};"""
                
                # connection = pyodbc.connect(connection_string)
                # connection = pymssql.connect(host=host,user=username,password=password,db=db_name,connect_timeout=5)
                print("Connection to MSSQL successful!")
                print("Connection to MSSQL successful!")
            except Exception as e:
                print(f"Failed to connect to MSSQL DB: {e}")
        return connection
    
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def create_connection(self, db_type='', host='', db_name='', username='', password='', port='', driver='', max_retry=3):
        connection = None

        if db_type == 'postgresql':
            try:
                print(f"Creating PostgreSQL connection")
                connection = psycopg2.connect(
                    host=host,
                    database=db_name,
                    user=username,
                    password=password,
                    port=port
                )
                print("Connection to PostgreSQL DB successful")
            except Exception as e:
                print(f"Failed to connect to PostgreSQL DB: {e}")
        elif db_type == 'mssql':
            print(f"conn : {db_type},{host},{db_name},{username},{password},{driver},{port}")
            print("Creating MSSQL connection")
            print(f"Creating MSSQL connection using pytds")
            try:
                connection = pytds.connect(
                    server= 'altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com',
                    database='AltaworxCentral_Test',
                    user='ALGONOX-Vyshnavi',
                    password='cs!Vtqe49gM32FDi',
                    port='1433'
                )
                # connection = pymssql.connect(
                #     server=host,
                #     user=username,
                #     password=password,
                #     database=db_name,
                #     port=port,
                #     timeout=5
                # )
                # connection = pymssql.connect(host=host,user=username,password=password,db_name=db_name,connect_timeout=5)
                print("Connection to MSSQL successful!")
            except Exception as e:
                print(f"Failed to connect to MSSQL DB: {e}")

        return connection
    
    def execute_query(self,connection,query,params=None):  
    
        try:
            # Check if params are provided
            if params:
                # Execute the query with parameters
                print(f'params--------{params}')
                result_df = pd.read_sql_query(query, connection, params=params)
            else:
                # Execute the query without parameters
                result_df = pd.read_sql_query(query, connection)
            
            return result_df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def load_env_pgsql(self):
        load_dotenv() 
        hostname = os.getenv('LOCAL_DB_HOST')
        port = os.getenv('LOCAL_DB_PORT')
        user = os.getenv('LOCAL_DB_USER')
        password = os.getenv('LOCAL_DB_PASSWORD')
        db_type = os.getenv('LOCAL_DB_TYPE')
        db_name=os.getenv('LOCAL_DB_NAME')
        return hostname,port,user,password,db_type,db_name
    
    def load_env_mssql(self):
        from_host = os.getenv('FROM_DB_HOST')
        from_port = os.getenv('FROM_DB_PORT')
        from_db=os.getenv('FROM_DB_NAME')
        from_user = os.getenv('FROM_DB_USER')
        from_pwd = os.getenv('FROM_DB_PASSWORD')
        from_db_type = os.getenv('FROM_DB_TYPE')
        from_driver=os.getenv('FROM_DB_DRIVER')
        return from_host,from_port,from_db,from_user,from_pwd,from_db_type,from_driver

    def usage_notification(self,data):
        load_dotenv()
        flag = data['action']
        print(f'flag----------{flag}')
        rule_id_2_0 = data['rule_def_id']
        print(f'rule_id_2_0-----------{rule_id_2_0}')
        expression_type_id = '33e9b000-a43f-4649-92af-ec3d7925a8a2'
        hostname,port,user,password,db_type,db_name=self.load_env_pgsql()
        hostname,port,user,password,db_type,db_name=self.load_env_pgsql()
        mapping_table=os.getenv('MAPPING_TABLE')
        def to_camel_case(snake_str):
            components = snake_str.split('_')
            return components[0].capitalize() + ''.join(x.capitalize() for x in components[1:]) 
        postgres_conn_start = time.time()
        postgres_conn = self.create_connection(db_type, hostname, db_name, user, password, port)
        from_host,from_port,ssms_db_name,from_user,from_pwd,from_db_type,from_driver=self.load_env_mssql()
        mssql_conn_start = time.time()
        mssql_conn=self.create_connection(from_db_type,from_host,ssms_db_name,from_user,from_pwd,from_port,from_driver)
        print(f"mssql_conn {mssql_conn}")
        print(f"MSSQL connection time: {time.time() - mssql_conn_start:.4f} seconds")
        print(f"Postgres connection time: {time.time() - postgres_conn_start:.4f} seconds")
        print(f"Postgres connection time: {time.time() - postgres_conn_start:.4f} seconds")
        ##getting all the details for a particular transfer
        query_start = time.time()
        #fetching data from 2.0 rule_rule_definition table based on id column
        if flag == 'create':
            rule_rule_defintiton_details_query=f"select * from public.rule_rule_definition where rule_def_id = '{rule_id_2_0}'"
            rule_rule_definition_details=self.execute_query(postgres_conn,rule_rule_defintiton_details_query)
            #print(f'rule_rule_definition_details----------------{rule_rule_definition_details}')

            #insert the details  fetched from 2.0 rule_rule_definition table to 1.0 rule_rule_definition without id(that will generate automatically)

            if not rule_rule_definition_details.empty:
                print(f'in if condition')
                rule_rule_definition_details = rule_rule_definition_details.drop(columns=['id'])
                all_columns = rule_rule_definition_details.columns
                columns_to_insert = all_columns[:-8]  # Exclude last 5 columns and rule_id column
                columns_to_insert_camel_case = [to_camel_case(col) for col in columns_to_insert]
                #print(f'columns_to_insert-----------{columns_to_insert_camel_case}')
                column_names = ", ".join(columns_to_insert_camel_case)
                #print(f'column_names------------{column_names}')
                placeholders = ", ".join(["%s"] * len(columns_to_insert))  # Get column names
                insert_query = f""" INSERT INTO AltaworxCentral_Test.dbo.RULE_RuleDefinition ({column_names}) OUTPUT INSERTED.id VALUES ({placeholders});"""  #fetch the id from 1.0 database and store it in a new column
                print(f'insert_query------------{insert_query}')
                print(f'placeholders-------------{placeholders}')
                def convert_value(x):
                        if pd.isna(x):  # Handle NaT or NaN
                            return None
                        elif isinstance(x, np.int64):
                            return int(x)
                        elif isinstance(x, np.float64):
                            return float(x)
                        elif isinstance(x, np.bool_):
                            return bool(x)
                        elif isinstance(x, pd.Timestamp):
                            return x.to_pydatetime() 
                        else:
                            return x
                data_to_insert = tuple(
                        rule_rule_definition_details.iloc[0][columns_to_insert]  # Exclude rule_id
                        .apply(convert_value)
                        .tolist())
                print(f'data_to_insert----------{data_to_insert}')       
                with mssql_conn.cursor() as cursor:
                    cursor.execute(insert_query, data_to_insert)
                    #cursor.execute("SELECT LASTVAL()")  # Fetch the last inserted ID
                    new_id = cursor.fetchone()[0]
                    print(f'New ID inserted: {new_id}')
                    mssql_conn.commit()
                    #def_id = '0050535b-e2c3-4f98-9b63-cb3fc28fe6ca'


                # Now update the `1_0_rule_id` column in the `rule_rule_definition` table
                new_id = str(new_id)
                with postgres_conn.cursor() as cursor:
                    update_query = """UPDATE public.rule_rule_definition 
                                        SET "rule_id_1_0" = %s
                                        WHERE rule_def_id = %s"""
                    print(f'update_query----------{update_query}')
                    cursor.execute(update_query, (new_id, rule_id_2_0))
                    postgres_conn.commit()
                    print(f'Updated 1_0_rule_id in rule_rule_definition table')

            else:
                print("No data found to insert into backup table.")\
                
            #with that id insert into rules version table

            rule_rule_defintiton_bak_details_query=f"select * from AltaworxCentral_Test.dbo.RULE_RuleDefinition where id = '{new_id}'"
            rule_rule_defintiton_bak_details=self.execute_query(mssql_conn,rule_rule_defintiton_bak_details_query)

            print(f'rule_rule_defintiton_bak_details---------------{rule_rule_defintiton_bak_details}')
            #print(f'rule_rule_defintiton_bak_details.columns-------{rule_rule_defintiton_bak_details.columns}')


            if not rule_rule_defintiton_bak_details.empty:
                rule_rule_defintiton_bak_details = rule_rule_defintiton_bak_details.drop(columns=['RuleId'])
                rule_rule_defintiton_bak_details = rule_rule_defintiton_bak_details.drop(columns=['VersionId'])
                rule_rule_defintiton_bak_details = rule_rule_defintiton_bak_details.rename(columns={'id': 'RuleId'})
                #print(f'rule_rule_defintiton_bak_details.columns-------{rule_rule_defintiton_bak_details.columns}')
                all_columns = rule_rule_defintiton_bak_details.columns
                column_names = ", ".join(all_columns)
                #print(f'column_names------------{column_names}')
                placeholders = ", ".join(["%s"] * len(all_columns))
                insert_query = f"""
                    INSERT INTO AltaworxCentral_Test.dbo.RULE_Version ({column_names}) OUTPUT INSERTED.id
                    VALUES ({placeholders})
                    """
                #print(f'insert_query-----------{insert_query}')
            
                    # Build the INSERT query dynamically
                def convert_value(x):
                        if isinstance(x, np.int64):
                            return int(x)
                        elif isinstance(x, np.float64):
                            return float(x)
                        elif isinstance(x, np.bool_):
                            return bool(x)
                        elif isinstance(x, uuid.UUID):  # Handle UUIDs
                            return str(x)
                        elif isinstance(x, pd.Timestamp):
                            return x.to_pydatetime()
                        else:
                            return x
                data_to_insert = tuple(
                        rule_rule_defintiton_bak_details.iloc[0][all_columns]  # Exclude rule_id
                        .apply(convert_value)
                        .tolist())
                #print(f'data_to_insert----------{data_to_insert}')
                with mssql_conn.cursor() as cursor:
                        cursor.execute(insert_query, data_to_insert)
                        version_id = cursor.fetchone()[0]
                        print(f'New version ID inserted: {version_id}')
                        mssql_conn.commit()
                print("Data inserted successfully.")

                #now get the version_id and update back in rule_definition table

                update_query = """
                    UPDATE AltaworxCentral_Test.dbo.RULE_RuleDefinition
                    SET VersionId = %s
                    WHERE id = %s
                """

                # Get the new version_id (this was fetched earlier after the insert)
                # 'version_id' is the ID of the newly inserted row in the RULE_Version table
                print(f'Updating RULE_RuleDefinition with version_id {version_id} for id {new_id}')

                # Execute the UPDATE query
                with mssql_conn.cursor() as cursor:
                    cursor.execute(update_query, (version_id, new_id))
                    mssql_conn.commit()

                print("RULE_RuleDefinition table updated successfully.")
                

            rule_rule_defintiton_customer_details_query=f"select rule_id_1_0, customers_list, created_date, created_by from public.rule_rule_definition where rule_def_id = '{rule_id_2_0}'"
            rule_rule_definition_customer_details=self.execute_query(postgres_conn,rule_rule_defintiton_customer_details_query)

            print(f'rule_rule_definition_customer_details-------------{rule_rule_definition_customer_details}')
            customers_list = rule_rule_definition_customer_details.get('customers_list')
            rule_id_1_0_value = rule_rule_definition_customer_details.get('rule_id_1_0')
            created_date_value = rule_rule_definition_customer_details.get('created_date')
            print(f'created_date_value----------{created_date_value}')
            created_by_value = rule_rule_definition_customer_details.get('created_by')

            rule_id_1_0 = rule_id_1_0_value.iloc[0] if not rule_id_1_0_value.empty else None
            created_date= created_date_value.iloc[0] if not created_date_value.empty else datetime.utcnow()
            created_date = created_date.strftime('%Y-%m-%d %H:%M:%S')
            print(f'created_date-----------{created_date}')
            created_by = created_by_value.iloc[0] if not created_by_value.empty else None
            #print(f'customers_list---{customers_list}')
            customers_list = customers_list.iloc[0]  # Accessing the first (and probably only) element in the series
            customers_data = ast.literal_eval(customers_list)

            # Now you can access 'customer_names' and 'customer_groups'
            customer_names_id = [customer_id for customer in customers_data["customer_names"] for _, customer_id in customer.items()]
            customer_groups_id = [group_id for group in customers_data["customer_groups"] for _, group_id in group.items()]


            print(f"Customer Names IDs: {customer_names_id}")
            print(f"Customer Groups IDs: {customer_groups_id}")
            column_names = "RuleId, CustomerId, CustomerGroupId, CreatedDate, CreatedBy"
            placeholders = "%s, %s, %s, %s, %s"

            #print(f'customer_names_id----------{type(customer_names_id)}')
            for customer_id in customer_names_id:
                insert_query = f"""
            INSERT INTO AltaworxCentral_Test.dbo.NotificationRuleRecipient ({column_names})
            VALUES ({placeholders})
            """
                print(f'insert_query-------{insert_query}')
                print(f"Values: {[rule_id_1_0, customer_id, None, created_date, created_by]}")
                with mssql_conn.cursor() as cursor:
                    cursor.execute(
                        insert_query,
                        tuple([rule_id_1_0, customer_id, None, created_date, created_by])  # Replace None and other placeholders as needed
                )
            
            for customer_group_id in customer_groups_id:
                insert_query = f"""
            INSERT INTO AltaworxCentral_Test.dbo.NotificationRuleRecipient ({column_names})
            VALUES ({placeholders})
            """
                print(f'insert_query-------{insert_query}')
                with mssql_conn.cursor() as cursor:
                    cursor.execute(
                        insert_query,
                        tuple([rule_id_1_0, None, customer_group_id, created_date, created_by])  # Replace None and other placeholders as needed
                )

                mssql_conn.commit()

            print("Data inserted into notifiaction table successfully.")


            #fetching the data of expression list from 2.0 based on rule_id

            expression_details_query = f"select * from rule_rule_definition where id = '{rule_id_2_0}'"
            expression_details_df=self.execute_query(postgres_conn,expression_details_query)
            #expression_ids = expression_details_df.get('expression_ids')
            expression_details_str = expression_details_df['expression_ids'].iloc[0]
            created_date = expression_details_df['created_date'].iloc[0]
            created_by = expression_details_df['created_by'].iloc[0]
            #created_date= created_date_value.iloc[0] if not created_date_value.empty else datetime.utcnow()
            created_date = created_date.strftime('%Y-%m-%d %H:%M:%S')
            print(f'created_date------------{created_date}')
            expression_details = json.loads(expression_details_str)
            print(f'expression_details---------{expression_details}')

            def inserting(expression_details, ordinal_counter):
                field_1_id = None
                const_one_id = None
                field_2_id = None
                const_two_id = None
                print("inserting of an expression")
                print(f'expression_details----------{expression_details}')
                if 'const_one' in expression_details or 'field_one' in expression_details:
                    if 'field_one' in expression_details:
                        with mssql_conn.cursor() as cursor:
                            field_one_value = list(expression_details['field_one'].values())[0]
                            data_to_insert = (rule_id_1_0, expression_type_id, field_one_value, created_by, created_date, ordinal_counter)
                            print(f'data_to_insert-----------{data_to_insert}')
                            insert_query = """
                                INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, FieldId, CreatedBy, CreatedDate, Ordinal) 
                                OUTPUT INSERTED.id
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, data_to_insert)
                            field_1_id = cursor.fetchone()[0]
                            print(f'New ID inserted: {field_1_id}')
                            print(expression_details['field_one'], "inserted", "field_one")
                            mssql_conn.commit()
                    elif 'const_one' in expression_details:
                        with mssql_conn.cursor() as cursor:
                            data_to_insert = (rule_id_1_0, expression_type_id, expression_details['const_one'], created_by, created_date, ordinal_counter)
                            insert_query = """
                                INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, ConstantValue, CreatedBy, CreatedDate, Ordinal) 
                                OUTPUT INSERTED.id
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, data_to_insert)
                            const_one_id = cursor.fetchone()[0]
                            print(f'New ID inserted: {const_one_id}')
                            print(expression_details['const_one'], "inserted const_one")
                            mssql_conn.commit()
                if 'field_two' in expression_details or 'const_two' in expression_details:
                    if 'field_two' in expression_details:
                        with mssql_conn.cursor() as cursor:
                            field_two_value = list(expression_details['field_two'].values())[0]
                            data_to_insert = (rule_id_1_0, expression_type_id, field_two_value, created_by, created_date, ordinal_counter)
                            insert_query = """
                                INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, FieldId, CreatedBy, CreatedDate, Ordinal) 
                                OUTPUT INSERTED.id
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, data_to_insert)
                            field_2_id = cursor.fetchone()[0]
                            print(f'New ID inserted: {field_2_id}')
                            print(expression_details['field_two'], "inserted", "field_two")
                            mssql_conn.commit()
                    elif 'const_two' in expression_details:
                        with mssql_conn.cursor() as cursor:
                            data_to_insert = (rule_id_1_0, expression_type_id, expression_details['const_two'], created_by, created_date, ordinal_counter)
                            insert_query = """
                                INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, ConstantValue, CreatedBy, CreatedDate, Ordinal) 
                                OUTPUT INSERTED.id
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, data_to_insert)
                            const_two_id = cursor.fetchone()[0]
                            print(f'New ID inserted: {const_two_id}')
                            print(expression_details['const_two'], "inserted const_two")
                            mssql_conn.commit()
                left_hand_expression_id = field_1_id or const_one_id
                right_hand_expression_id = field_2_id or const_two_id
                condtion_value = list(expression_details['cond'].values())[0]
                # print(f'left_hand_expression_id------------{left_hand_expression_id}')
                # print(f'right_hand_expression_id--------------{right_hand_expression_id}')
                # print(f'condtion_value-----------{condtion_value}')
                print(f'before inserting left_hand_expression_id and right_hand_expression_id -----------{ordinal_counter}')
                if left_hand_expression_id and right_hand_expression_id and condtion_value:
                    with mssql_conn.cursor() as cursor:
                        final_insert_query = f"""
                        INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, LeftHandExpressionId, RightHandExpressionId, OperatorId, CreatedBy, CreatedDate, Ordinal)  OUTPUT INSERTED.id
                        VALUES ('{rule_id_1_0}', '{expression_type_id}', '{left_hand_expression_id}', '{right_hand_expression_id}', '{condtion_value}', '{created_by}', '{created_date}', {ordinal_counter}) ;
                        """
                        cursor.execute(final_insert_query)
                        final_query_id = cursor.fetchone()[0]
                        print(f'final_query_id--------------{final_query_id}')
                        print("Final rule_expression inserted")
                        mssql_conn.commit()
                        # with mssql_conn.cursor() as cursor:
                        #     update_ordinal_query = f"""
                        #     UPDATE AltaworxCentral_Test.dbo.RULE_Expression
                        #     SET Ordinal = {ordinal_counter}
                        #     WHERE id IN ('{left_hand_expression_id}', '{right_hand_expression_id}', '{final_query_id}')
                        #     """
                        #     cursor.execute(update_ordinal_query)
                        #     mssql_conn.commit()
                print(expression_details['cond'],"inserted cond")

                return final_query_id
            
            def expresion_builder(expression_details, ordinal_counter):
                print('in create function')
                if len(expression_details)>1:
                    print(F"exp got is",expression_details)
                    print(F"\n")
                    print(f'ordinal_counter----------{ordinal_counter}')
                    exp_3_id,ordinal_counter = expresion_builder(expression_details[2:], ordinal_counter)
                    print(f'ordinal_counter----------{ordinal_counter}')
                    print(f'exp_3_id--------{exp_3_id}')
                    exp_1_id=inserting(expression_details[0], ordinal_counter)
                    ordinal_counter=ordinal_counter-1
                    print(f'ordinal_counter----------{ordinal_counter}')
                    print(f'exp_1_id--------{exp_1_id}')
                    cond=list(expression_details[1].values())[0]
                    print(f'cond------{cond}')
                    left_hand_expression_id = exp_1_id  # The first expression part (field_1 or const_one)
                    right_hand_expression_id = exp_3_id
                    print(f'left_hand_expression_id--------------{left_hand_expression_id}')
                    print(f'right_hand_expression_id------------{right_hand_expression_id}')
                    print(exp_1_id,cond,exp_3_id)
                    if cond == 'OR':
                        operator_type_query = f"""select id from public.rule_operator_type where name = '{cond}' """
                        operator_type=self.execute_query(postgres_conn,operator_type_query)
                        operator_type = operator_type['id'].iloc[0]
                        print(f'operator_type----------{operator_type}')
                        with mssql_conn.cursor() as cursor:
                            final_insert_query = f"""
                            INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, LeftHandExpressionId, RightHandExpressionId, OperatorId, CreatedBy, CreatedDate)  OUTPUT INSERTED.id
                            VALUES ('{rule_id_1_0}', '{expression_type_id}', '{left_hand_expression_id}', '{right_hand_expression_id}', '{operator_type}', '{created_by}', '{created_date}') ;
                            """
                            cursor.execute(final_insert_query)
                            final_query_id = cursor.fetchone()[0]
                            print(f'final_query_id--------------{final_query_id}')
                            print("Final rule_expression inserted")
                            mssql_conn.commit()
                            with mssql_conn.cursor() as cursor:
                                update_ordinal_query = f"""
                                UPDATE AltaworxCentral_Test.dbo.RULE_Expression
                                SET Ordinal = {ordinal_counter}
                                WHERE id IN ('{final_query_id}')
                                """
                                cursor.execute(update_ordinal_query)
                                mssql_conn.commit()
                            return final_query_id,ordinal_counter-1

                    elif cond == 'AND':
                        operator_type_query = f"""select id from public.rule_operator_type where name = '{cond}' """
                        operator_type=self.execute_query(postgres_conn,operator_type_query)
                        operator_type = operator_type['id'].iloc[0]
                        with mssql_conn.cursor() as cursor:
                            final_insert_query = f"""
                            INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, LeftHandExpressionId, RightHandExpressionId, OperatorId, CreatedBy, CreatedDate)  OUTPUT INSERTED.id
                            VALUES ('{rule_id_1_0}', '{expression_type_id}', '{left_hand_expression_id}', '{right_hand_expression_id}', '{operator_type}', '{created_by}', '{created_date}') ;
                            """
                            cursor.execute(final_insert_query)
                            final_query_id = cursor.fetchone()[0]
                            print(f'final_query_id--------------{final_query_id}')
                            print("Final rule_expression inserted")
                            mssql_conn.commit()
                            with mssql_conn.cursor() as cursor:
                                update_ordinal_query = f"""
                                UPDATE AltaworxCentral_Test.dbo.RULE_Expression
                                SET Ordinal = {ordinal_counter}
                                WHERE id IN ('{final_query_id}')
                                """
                                cursor.execute(update_ordinal_query)
                                mssql_conn.commit()
                            return final_query_id,ordinal_counter-1
                else:
                    print(F"exp got is in else",expression_details)
                    print(F"\n")
                    expression_details=expression_details[0]
                    print(f'ordinal_counter-----------{ordinal_counter}')
                    return inserting(expression_details, ordinal_counter),ordinal_counter-1
            
            ordinal_counter = len(expression_details)
            expresion_builder(expression_details, ordinal_counter)


        elif flag == 'delete':
            rule_rule_defintiton_details_query=f"select rule_id_1_0, deleted_date, deleted_by from public.rule_rule_definition where rule_def_id = '{rule_id_2_0}'"
            rule_rule_definition_details=self.execute_query(postgres_conn,rule_rule_defintiton_details_query)
            rule_id_1_0 = rule_rule_definition_details.get('rule_id_1_0')
            deleted_date = rule_rule_definition_details.get('deleted_date')
            deleted_by = rule_rule_definition_details.get('deleted_by')
            deleted_date = deleted_date.iloc[0]
            if pd.isna(deleted_date):
                deleted_date = 'NULL'
            else:
                deleted_date = f"'{deleted_date.strftime('%Y-%m-%d %H:%M:%S')}'"
            deleted_by = deleted_by.iloc[0]
            rule_id_1_0 = rule_id_1_0.iloc[0]

            delete_query= f"update AltaworxCentral_Test.dbo.RULE_Version set IsActive = '0', IsDeleted = '1', DeletedDate = {deleted_date}, DeletedBy = '{deleted_by}' where RuleId = '{rule_id_1_0}'"
            with mssql_conn.cursor() as cursor:
                        cursor.execute(delete_query)
                        mssql_conn.commit()

        elif flag == 'edit':

            #fetch the details from 2.0 rule_rule_definition table
            rule_rule_defintiton_details_query=f"select * from public.rule_rule_definition where rule_def_id = '{rule_id_2_0}'"
            rule_rule_definition_details=self.execute_query(postgres_conn,rule_rule_defintiton_details_query)
            print(f'rule_rule_definition_details-----------{rule_rule_definition_details}')

            #fetch rule_id_1_0 column to take that as id column to update in 1.0 rule_defintion table
            rule_id_1_0 = rule_rule_definition_details.loc[0, 'rule_id_1_0']
            print(f'rule_id_1_0-----{rule_id_1_0}')

            #dropping id details
            rule_rule_definition_details = rule_rule_definition_details.drop(columns=['id']).iloc[:, :-8]
            
            #updating the details in rule_definition tables with the latest data
            all_columns = rule_rule_definition_details.columns
            columns_to_insert_camel_case = [to_camel_case(col) for col in all_columns]
            #print(f'columns_to_insert-----------{columns_to_insert_camel_case}')
            #update_set_statements = ", ".join(columns_to_insert_camel_case)
            #print(f'update_set_statements-------------{update_set_statements}')

            update_set_statements = ", ".join([f"{col} = %s" for col in columns_to_insert_camel_case])
            #print(f'update_set_statements-----------{update_set_statements}')
            update_query = f"UPDATE AltaworxCentral_Test.dbo.RULE_RuleDefinition SET {update_set_statements} WHERE id = '{rule_id_1_0}'"
            print(f"Update query: {update_query}")
            def convert_value(value):
                if isinstance(value, pd.Timestamp):  # Convert pandas.Timestamp to Python datetime
                    return value.to_pydatetime()
                if isinstance(value, bool):  # Ensure booleans are Python native
                    return bool(value)
                if isinstance(value, np.bool_):  # Convert numpy.bool_ to Python bool
                    return bool(value)
                if isinstance(value, np.int64):  # Explicitly handle numpy.int64 conversion to int
                    return int(value)
                if pd.isna(value):  # Convert NaT (pandas missing time) to None for SQL
                    return None
                if value is None:  # Handle None (SQL NULL)
                    return None
                return value  # Leave other types as they are
            #print(len(all_columns))
            #print(len(rule_rule_definition_details.columns))
            update_values = [convert_value(value) for value in rule_rule_definition_details.iloc[0].tolist()]
            update_values = [value if value is not None else None for value in update_values]
            update_values = tuple(update_values)
            print(f'values -----------{update_values}')
            with mssql_conn.cursor() as cursor:
                        cursor.execute(update_query, update_values)
                        mssql_conn.commit()

            print(f'query is executed to edit the rule_rule_definition')
            
            #insert the details as a new row in rules_verison table

            rule_rule_definition_updated_details_query=f"select * from AltaworxCentral_Test.dbo.RULE_RuleDefinition where id = '{rule_id_1_0}'"
            rule_rule_definition_updated_details=self.execute_query(mssql_conn,rule_rule_definition_updated_details_query)

            print(f'rule_rule_definition_updated_details------------{rule_rule_definition_updated_details.columns}')

            if not rule_rule_definition_updated_details.empty:
                rule_rule_definition_updated_details = rule_rule_definition_updated_details.drop(columns=['RuleId'])
                rule_rule_definition_updated_details = rule_rule_definition_updated_details.drop(columns=['VersionId'])
                rule_rule_definition_updated_details = rule_rule_definition_updated_details.rename(columns={'id': 'RuleId'})
                columns_to_insert = rule_rule_definition_updated_details.columns
                #columns_to_insert = all_columns[:-8]  # Exclude last 5 columns and rule_id column
                print(f'columns_to_insert-----------{columns_to_insert}')
                column_names = ", ".join(columns_to_insert)
                placeholders = ", ".join(["%s"] * len(columns_to_insert))

                insert_query = f"""
                    INSERT INTO AltaworxCentral_Test.dbo.RULE_Version ({column_names}) OUTPUT INSERTED.id
                    VALUES ({placeholders})
                    """
                print(f'insert_query-----------{insert_query}')
            
                    # Build the INSERT query dynamically
                def convert_value(x):
                        if isinstance(x, np.int64):
                            return int(x)
                        elif isinstance(x, np.float64):
                            return float(x)
                        elif isinstance(x, np.bool_):
                            return bool(x)
                        elif isinstance(x, pd.Timestamp):
                            return x.to_pydatetime()
                        else:
                            return x
                data_to_insert = (
                        rule_rule_definition_updated_details.iloc[0][columns_to_insert]  # Exclude rule_id
                        .apply(convert_value)
                        .tolist())
                print(f'data_to_insert----------{data_to_insert}')
                with mssql_conn.cursor() as cursor:
                        cursor.execute(insert_query, tuple(data_to_insert))
                        new_id = cursor.fetchone()[0]
                        print(f'New ID inserted: {new_id}')
                        cursor.execute("SELECT VersionNumber FROM AltaworxCentral_Test.dbo.RULE_Version WHERE RuleId = %s order by VersionNumber desc" , (rule_id_1_0,))
                        current_version_number = cursor.fetchone()
                        print(f'Current VersionNumber before update: {current_version_number}')
                        updated_version_number = int(current_version_number[0]) + 1
                        mssql_conn.commit()
                        update_query = """UPDATE AltaworxCentral_Test.dbo.RULE_Version 
                                    SET "VersionNumber" = %s
                                    WHERE id = %s"""
                        print(f'update_query----------{update_query}')
                        cursor.execute(update_query, (updated_version_number, new_id))
                        mssql_conn.commit()
                        print(f'Updated version_number in rule version table table')
                #fetched the id from updated_rule version and update it back to rule_defintion


                update_query = """
                    UPDATE AltaworxCentral_Test.dbo.RULE_RuleDefinition
                    SET VersionId = %s
                    WHERE id = %s
                """

                # Get the new version_id (this was fetched earlier after the insert)
                # 'version_id' is the ID of the newly inserted row in the RULE_Version table
                print(f'Updating RULE_RuleDefinition with version_id {new_id} for id {rule_id_1_0}')

                # Execute the UPDATE query
                with mssql_conn.cursor() as cursor:
                    cursor.execute(update_query, (new_id, rule_id_1_0))
                    mssql_conn.commit()
                          
                print("Data inserted successfully.")
        
            #edit customer_list based on changes
            customer_list_edited_details_query = f"select * from AltaworxCentral_Test.dbo.NotificationRuleRecipient where RuleId = '{rule_id_1_0}'"
            customer_list_edited_details=self.execute_query(mssql_conn,customer_list_edited_details_query)

            if not customer_list_edited_details.empty:
                delete_query = f"DELETE FROM AltaworxCentral_Test.dbo.NotificationRuleRecipient WHERE RuleId = '{rule_id_1_0}'"
                with mssql_conn.cursor() as cursor:
                        cursor.execute(delete_query)
                        mssql_conn.commit()
            rule_rule_defintiton_customer_details_query=f"select rule_id_1_0, customers_list, created_date, created_by from public.rule_rule_definition where rule_def_id = '{rule_id_2_0}'"
            rule_rule_definition_customer_details=self.execute_query(postgres_conn,rule_rule_defintiton_customer_details_query)

            print(f'rule_rule_definition_customer_details-------------{rule_rule_definition_customer_details}')
            customers_list = rule_rule_definition_customer_details.get('customers_list')
            print(f'customers_list--------{customers_list}')
            rule_id_1_0_value = rule_rule_definition_customer_details.get('rule_id_1_0')
            created_date_value = rule_rule_definition_customer_details.get('created_date')
            created_by_value = rule_rule_definition_customer_details.get('created_by')

            rule_id_1_0 = rule_id_1_0_value.iloc[0] if not rule_id_1_0_value.empty else None
            created_date= created_date_value.iloc[0] if not created_date_value.empty else None
            created_by = created_by_value.iloc[0] if not created_by_value.empty else None
            #print(f'customers_list---{customers_list}')
            created_date = created_date.strftime('%Y-%m-%d %H:%M:%S')
            customers_list = customers_list.iloc[0]  # Accessing the first (and probably only) element in the series
            print(f'customers_list------{customers_list}')
            customers_data = ast.literal_eval(customers_list)

            # Now you can access 'customer_names' and 'customer_groups'
            customer_names_id = [customer_id for customer in customers_data["customer_names"] for _, customer_id in customer.items()]
            customer_groups_id = [group_id for group in customers_data["customer_groups"] for _, group_id in group.items()]


            print(f"Customer Names IDs: {customer_names_id}")
            print(f"Customer Groups IDs: {customer_groups_id}")
            column_names = "RuleId, CustomerId, CustomerGroupId, CreatedDate, CreatedBy"
            placeholders = "%s, %s, %s, %s, %s"

            print(f'customer_names_id----------{type(customer_names_id)}')
            for customer_id in customer_names_id:
                insert_query = f"""
            INSERT INTO AltaworxCentral_Test.dbo.NotificationRuleRecipient ({column_names})
            VALUES ({placeholders})
            """
                print(f'insert_query-------{insert_query}')
                with mssql_conn.cursor() as cursor:
                    cursor.execute(
                        insert_query,
                        tuple([rule_id_1_0, customer_id, None, created_date, created_by])  # Replace None and other placeholders as needed
                )
            
            for customer_group_id in customer_groups_id:
                insert_query = f"""
            INSERT INTO AltaworxCentral_Test.dbo.NotificationRuleRecipient ({column_names})
            VALUES ({placeholders})
            """
                print(f'insert_query-------{insert_query}')
                with mssql_conn.cursor() as cursor:
                    cursor.execute(
                        insert_query,
                        tuple([rule_id_1_0, None, customer_group_id, created_date, created_by])  # Replace None and other placeholders as needed
                )

                mssql_conn.commit()

                print("Data inserted successfully.")

            #update is_active false for already existed expressions
            print(f'rule_id_2_0----------{rule_id_2_0}')
            rule_rule_defintiton_details_query=f"select * from public.rule_rule_definition where rule_def_id = '{rule_id_2_0}'"
            rule_rule_definition_details=self.execute_query(postgres_conn,rule_rule_defintiton_details_query)
            print(f'rule_rule_definition_details------------{rule_rule_definition_details}')
            expression_details_str = rule_rule_definition_details['expression_ids'].iloc[0]
            print(f'expression_details_str-------{expression_details_str}')
            rule_id_1_0 = rule_rule_definition_details.get('rule_id_1_0')
            deleted_date = rule_rule_definition_details.get('deleted_date')
            deleted_by = rule_rule_definition_details.get('deleted_by')
            deleted_date = deleted_date.iloc[0]
            if pd.isna(deleted_date):
                deleted_date = 'NULL'
            else:
                deleted_date = f"'{deleted_date.strftime('%Y-%m-%d %H:%M:%S')}'"
            deleted_by = deleted_by.iloc[0]
            rule_id_1_0 = rule_id_1_0.iloc[0]

            delete_query= f"update AltaworxCentral_Test.dbo.RULE_Expression set IsActive = '0', IsDeleted = '1', DeletedDate = {deleted_date}, DeletedBy = '{deleted_by}' where RuleDefinitionId = '{rule_id_1_0}'"
            print(f'delete_query----------{delete_query}')
            with mssql_conn.cursor() as cursor:
                        cursor.execute(delete_query)
                        mssql_conn.commit()

            #fetching the data of expression list from 2.0 based on rule_id
            created_date = rule_rule_definition_details['created_date'].iloc[0]
            created_date = created_date.strftime('%Y-%m-%d %H:%M:%S')
            created_by = rule_rule_definition_details['created_by'].iloc[0]
            expression_details = json.loads(expression_details_str)
            print(f'expression_details---------{expression_details}')
            def inserting(expression_details, ordinal_counter):
                field_1_id = None
                const_one_id = None
                field_2_id = None
                const_two_id = None
                print("inserting of an expression")
                print(f'expression_details----------{expression_details}')
                if 'const_one' in expression_details or 'field_one' in expression_details:
                    if 'field_one' in expression_details:
                        with mssql_conn.cursor() as cursor:
                            field_one_value = list(expression_details['field_one'].values())[0]
                            data_to_insert = (rule_id_1_0, expression_type_id, field_one_value, created_by, created_date, ordinal_counter)
                            print(f'data_to_insert-----------{data_to_insert}')
                            insert_query = """
                                INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, FieldId, CreatedBy, CreatedDate, Ordinal) 
                                OUTPUT INSERTED.id
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, data_to_insert)
                            field_1_id = cursor.fetchone()[0]
                            print(f'New ID inserted: {field_1_id}')
                            print(expression_details['field_one'], "inserted", "field_one")
                            mssql_conn.commit()
                    elif 'const_one' in expression_details:
                        with mssql_conn.cursor() as cursor:
                            data_to_insert = (rule_id_1_0, expression_type_id, expression_details['const_one'], created_by, created_date, ordinal_counter)
                            insert_query = """
                                INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, ConstantValue, CreatedBy, CreatedDate, Ordinal) 
                                OUTPUT INSERTED.id
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, data_to_insert)
                            const_one_id = cursor.fetchone()[0]
                            print(f'New ID inserted: {const_one_id}')
                            print(expression_details['const_one'], "inserted const_one")
                            mssql_conn.commit()
                if 'field_two' in expression_details or 'const_two' in expression_details:
                    if 'field_two' in expression_details:
                        with mssql_conn.cursor() as cursor:
                            field_two_value = list(expression_details['field_two'].values())[0]
                            data_to_insert = (rule_id_1_0, expression_type_id, field_two_value, created_by, created_date, ordinal_counter)
                            insert_query = """
                                INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, FieldId, CreatedBy, CreatedDate, Ordinal) 
                                OUTPUT INSERTED.id
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, data_to_insert)
                            field_2_id = cursor.fetchone()[0]
                            print(f'New ID inserted: {field_2_id}')
                            print(expression_details['field_two'], "inserted", "field_two")
                            mssql_conn.commit()
                    elif 'const_two' in expression_details:
                        with mssql_conn.cursor() as cursor:
                            data_to_insert = (rule_id_1_0, expression_type_id, expression_details['const_two'], created_by, created_date, ordinal_counter)
                            insert_query = """
                                INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, ConstantValue, CreatedBy, CreatedDate, Ordinal) 
                                OUTPUT INSERTED.id
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cursor.execute(insert_query, data_to_insert)
                            const_two_id = cursor.fetchone()[0]
                            print(f'New ID inserted: {const_two_id}')
                            print(expression_details['const_two'], "inserted const_two")
                            mssql_conn.commit()
                left_hand_expression_id = field_1_id or const_one_id
                right_hand_expression_id = field_2_id or const_two_id
                condtion_value = list(expression_details['cond'].values())[0]
                # print(f'left_hand_expression_id------------{left_hand_expression_id}')
                # print(f'right_hand_expression_id--------------{right_hand_expression_id}')
                # print(f'condtion_value-----------{condtion_value}')
                print(f'before inserting left_hand_expression_id and right_hand_expression_id -----------{ordinal_counter}')
                if left_hand_expression_id and right_hand_expression_id and condtion_value:
                    with mssql_conn.cursor() as cursor:
                        final_insert_query = f"""
                        INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, LeftHandExpressionId, RightHandExpressionId, OperatorId, CreatedBy, CreatedDate, Ordinal)  OUTPUT INSERTED.id
                        VALUES ('{rule_id_1_0}', '{expression_type_id}', '{left_hand_expression_id}', '{right_hand_expression_id}', '{condtion_value}', '{created_by}', '{created_date}', {ordinal_counter}) ;
                        """
                        cursor.execute(final_insert_query)
                        final_query_id = cursor.fetchone()[0]
                        print(f'final_query_id--------------{final_query_id}')
                        print("Final rule_expression inserted")
                        mssql_conn.commit()
                        # with mssql_conn.cursor() as cursor:
                        #     update_ordinal_query = f"""
                        #     UPDATE AltaworxCentral_Test.dbo.RULE_Expression
                        #     SET Ordinal = {ordinal_counter}
                        #     WHERE id IN ('{left_hand_expression_id}', '{right_hand_expression_id}', '{final_query_id}')
                        #     """
                        #     cursor.execute(update_ordinal_query)
                        #     mssql_conn.commit()
                print(expression_details['cond'],"inserted cond")

                return final_query_id
            
            def expresion_builder(expression_details, ordinal_counter):
                print('in edit function')
                if len(expression_details)>1:
                    print(F"exp got is",expression_details)
                    print(F"\n")
                    print(f'ordinal_counter----------{ordinal_counter}')
                    exp_3_id,ordinal_counter = expresion_builder(expression_details[2:], ordinal_counter)
                    print(f'ordinal_counter----------{ordinal_counter}')
                    print(f'exp_3_id--------{exp_3_id}')
                    exp_1_id=inserting(expression_details[0], ordinal_counter)
                    ordinal_counter=ordinal_counter-1
                    print(f'ordinal_counter----------{ordinal_counter}')
                    print(f'exp_1_id--------{exp_1_id}')
                    cond=list(expression_details[1].values())[0]
                    print(f'cond------{cond}')
                    left_hand_expression_id = exp_1_id  # The first expression part (field_1 or const_one)
                    right_hand_expression_id = exp_3_id
                    print(f'left_hand_expression_id--------------{left_hand_expression_id}')
                    print(f'right_hand_expression_id------------{right_hand_expression_id}')
                    print(exp_1_id,cond,exp_3_id)
                    if cond == 'OR':
                        operator_type_query = f"""select id from public.rule_operator_type where name = '{cond}' """
                        operator_type=self.execute_query(postgres_conn,operator_type_query)
                        operator_type = operator_type['id'].iloc[0]
                        print(f'operator_type----------{operator_type}')
                        with mssql_conn.cursor() as cursor:
                            final_insert_query = f"""
                            INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, LeftHandExpressionId, RightHandExpressionId, OperatorId, CreatedBy, CreatedDate)  OUTPUT INSERTED.id
                            VALUES ('{rule_id_1_0}', '{expression_type_id}', '{left_hand_expression_id}', '{right_hand_expression_id}', '{operator_type}', '{created_by}', '{created_date}') ;
                            """
                            cursor.execute(final_insert_query)
                            final_query_id = cursor.fetchone()[0]
                            print(f'final_query_id--------------{final_query_id}')
                            print("Final rule_expression inserted")
                            mssql_conn.commit()
                            with mssql_conn.cursor() as cursor:
                                update_ordinal_query = f"""
                                UPDATE AltaworxCentral_Test.dbo.RULE_Expression
                                SET Ordinal = {ordinal_counter}
                                WHERE id IN ('{final_query_id}')
                                """
                                cursor.execute(update_ordinal_query)
                                mssql_conn.commit()
                            return final_query_id,ordinal_counter-1

                    elif cond == 'AND':
                        operator_type_query = f"""select id from public.rule_operator_type where name = '{cond}' """
                        operator_type=self.execute_query(postgres_conn,operator_type_query)
                        operator_type = operator_type['id'].iloc[0]
                        with mssql_conn.cursor() as cursor:
                            final_insert_query = f"""
                            INSERT INTO AltaworxCentral_Test.dbo.RULE_Expression (RuleDefinitionId, ExpressionTypeId, LeftHandExpressionId, RightHandExpressionId, OperatorId, CreatedBy, CreatedDate)  OUTPUT INSERTED.id
                            VALUES ('{rule_id_1_0}', '{expression_type_id}', '{left_hand_expression_id}', '{right_hand_expression_id}', '{operator_type}', '{created_by}', '{created_date}') ;
                            """
                            cursor.execute(final_insert_query)
                            final_query_id = cursor.fetchone()[0]
                            print(f'final_query_id--------------{final_query_id}')
                            print("Final rule_expression inserted")
                            mssql_conn.commit()
                            with mssql_conn.cursor() as cursor:
                                update_ordinal_query = f"""
                                UPDATE AltaworxCentral_Test.dbo.RULE_Expression
                                SET Ordinal = {ordinal_counter}
                                WHERE id IN ('{final_query_id}')
                                """
                                cursor.execute(update_ordinal_query)
                                mssql_conn.commit()
                            return final_query_id,ordinal_counter-1
                else:
                    print(F"exp got is in else",expression_details)
                    print(F"\n")
                    expression_details=expression_details[0]
                    print(f'ordinal_counter-----------{ordinal_counter}')
                    return inserting(expression_details, ordinal_counter),ordinal_counter-1
            ordinal_counter = len(expression_details)
            expresion_builder(expression_details, ordinal_counter)
            
        else:
            pass
