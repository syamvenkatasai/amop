import copy
import os
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
from pandas import Timestamp


import logging
import time
from sqlalchemy import create_engine, exc,text
# import pyodbc
import pytds
import time
import psycopg2
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv
import os
import re

import json
from datetime import datetime, timedelta
import threading
import time
from psycopg2.extras import execute_values
import math


logging.basicConfig(filename='db_migration.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s:%(message)s')


class MigrationScheduler:

    def create_postgres_connection(self,postgres_db_name):
        print(f"In create_postgres_connection {postgres_db_name}")

        load_dotenv()
        hostname = os.environ('LOCAL_DB_HOST')
        port = os.environ('LOCAL_DB_PORT')
        user = os.environ('LOCAL_DB_USER')
        password = os.environ('LOCAL_DB_PASSWORD')
        db_type = os.environ('LOCAL_DB_TYPE')
        try:
            connection=self.create_connection(db_type,hostname,postgres_db_name,user,password,port)
            print(f"in create postgres connection {connection}")
        except Exception as e:
            print(f"Error while establishing connection {e}")
        return connection

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def create_connection(self,db_type='',host='', db_name='',username='', password='',port='',driver='',max_retry=3):
        connection = None
        retry = 1
        print(f"db_type:{db_type}, host--{host}-db_name-{db_name}, username-{username},password-{password},port-{port},driver-{driver}")
        db_type = db_type.strip()
        host = host.strip()
        db_name = db_name.strip()
        username = username.strip()
        password = password.strip()
        port = port.strip()
        driver = driver.strip()
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
            print(f"Creating MSSQL connection")
            try:
                connection = pytds.connect(
                    server=host,
                    database=db_name,
                    user=username,
                    password=password,
                    port=port
                )

                print("Connection to MSSQL successful!")
            except Exception as e:
                print(f"Failed to connect to MSSQL DB: {e}")
        return connection

    def execute_query(self,connection,query):
        try:
            result_df=pd.read_sql_query(query,connection)
            return result_df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def get_from_clause_index(self,query):
    # This regex will match the standalone "FROM" keyword
        match = re.search(r"\bFROM\b", query, re.IGNORECASE)
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@getting the index of from",match.start() )
        if match:
            return match.start()  # Return the starting index of "FROM"
        else:
            raise ValueError("Invalid query: Missing FROM clause.")

    def get_updated_records_query(self,columns, last_id, full_from_table, query):
        try:
            # Initialize the WHERE clause
            where_conditions = []
            modified_date_condition = None

            # Parse the FROM clause to extract table aliases
            #from_clause_start = query.upper().find("FROM")
            from_clause_start = self.get_from_clause_index(query)
            if from_clause_start == -1:
                raise ValueError("Invalid query: Missing FROM clause.")

            # Extract the part of the query after FROM
            from_clause = query[from_clause_start + 4:].strip()

            # Split on JOIN, comma, or WHERE to isolate the FROM section
            from_clause = from_clause.split(" WHERE")[0].split(" JOIN")[0].split(",")[0].strip()
            print(f"from clause {from_clause}")
            from_clause_split= from_clause.split()
            print("split [rint]",from_clause_split)
            table_aliases = {}
            for idx,word in enumerate(from_clause_split):
                if self.recognize_format(word):
                    print(idx,word,'1234567words')
                    #return None
                    # Check if there is an alias available
                    if idx + 1 < len(from_clause_split):
                        alias = from_clause_split[idx + 1]
                        table_aliases[alias] = word


            # Parse tables and aliases
            # for table_part in from_clause.split():
            #     table_part = table_part.strip().split(" ")
            #     table_name = table_part[0]
            #     alias = table_part[-1] if len(table_part) > 1 else table_name
            #     table_aliases[alias] = table_name

            # Check if ModifiedDate exists in the query
            modified_date_alias = None
            for alias in table_aliases:
                alias1=f"{alias}.modifieddate"
                alias2=f"{alias}.id"
                print("alias1",alias1)
                print("alias2",alias2)
                if alias1 in query.lower() or alias2 in query.lower():
                    modified_date_alias = alias
                    break

            print(f"modified_date_alias {modified_date_alias}")
            #return None
            # Constructing the WHERE clause dynamically
            for col, val in zip(columns, last_id):
                print(f"col is {col}")
                if col.lower()=='id' and modified_date_alias:
                    col_with_alias = f"{modified_date_alias}.{col}"
                    print(f"col with alias {col_with_alias} and type {type(modified_date_alias)}")
                    if isinstance(val, str):
                        where_conditions.append(f"{col_with_alias} > '{val}'")
                    else:
                        where_conditions.append(f"{col_with_alias} > {val}")
                elif col.lower() == "modifieddate" and modified_date_alias:

                    col_with_alias = f"{modified_date_alias}.{col}"
                    # print(f"col with alias {col_with_alias}")
                    if isinstance(val, str):
                        modified_date_condition = f"{col_with_alias} > '{val}'"
                    else:
                        modified_date_condition = f"{col_with_alias} > {val}"
                elif modified_date_alias is None and col.lower()=="modifieddate" and col.lower() in query.lower():
                    # print(f"in thiss")
                    if isinstance(val, str):
                        modified_date_condition = f"{col} > '{val}'"
                    else:
                        modified_date_condition = f"{col} > {val}"
                else:
                    print(f"in else")
                    if isinstance(val, str):
                        where_conditions.append(f"{col} > '{val}'")
                        print(f"where_conditions {where_conditions}")
                    else:
                        where_conditions.append(f"{col} > {val}")
                        print(f"where_conditions {where_conditions}")

            # Combine the conditions
            # print(f"modified_date_condition {modified_date_condition}")
            where_clause = ""
            if where_conditions:
                where_clause = " WHERE " + " OR ".join(where_conditions)

            if modified_date_condition:
                if where_clause:
                    where_clause += f" OR {modified_date_condition}"
                else:
                    where_clause = f" WHERE {modified_date_condition}"

            # Construct the final query
            if full_from_table:
                migrate_records = f"SELECT * FROM {full_from_table}{where_clause} ORDER BY id ASC"
            elif query:
                if "ORDER BY" in query.upper():
                    #query = query.replace("ORDER BY", where_clause + " ORDER BY")
                    parts = query.rsplit("ORDER BY", 1)
                    # Add the WHERE clause before the last "ORDER BY"
                    query = parts[0] + where_clause + " ORDER BY" + parts[1]
                else:
                    query += where_clause
                migrate_records = query

        except Exception as e:
            print(f"Error in generating query to select records: {e}")
            migrate_records = None

        return migrate_records

    def main(self):
        load_dotenv()  # Load environment variables from .env file
        migration_details_dict={}

        print("Beginning migration")

        hostname = os.environ('LOCAL_DB_HOST')
        port = os.environ('LOCAL_DB_PORT')
        db_name = 'Migration_Test'
        user = os.environ('LOCAL_DB_USER')
        password = os.environ('LOCAL_DB_PASSWORD')
        db_type = os.environ('LOCAL_DB_TYPE')
        migration_table=os.environ('MIGRATION_TABLE')

        postgres_conn = self.create_connection(db_type, hostname, db_name, user, password, port)
        query = f"SELECT migration_name FROM {migration_table}"
        rows = self.execute_query(postgres_conn, query)


        if rows.empty:
            print("No existing jobs found. Creating a new job.")
            new_job = self.create_new_migration(postgres_conn, migration_table)

        else:
            user_choice = input("Do you want to create a new migration job or select an existing one? (new/existing): ").strip().lower()

            if user_choice == 'new':
                print("Creating a new migration job.")
                new_job = self.create_new_migration(postgres_conn, migration_table)
            elif user_choice == 'existing':
                print("Existing jobs:")
                # print(rows)
                for index, row in rows.iterrows():
                    print(f"{index + 1}. {row['migration_name']}")

                choice = input("Enter 'single' to execute a single job immediately or 'scheduled' to schedule jobs: ").strip().lower()

                if choice == 'single':
                    job_name = input("Enter the job name you want to execute: ").strip()
                    self.main_migration_func(job_name, postgres_conn)
                elif choice == 'scheduled':
                    job_names = input("Enter the job names you want to select (comma-separated for multiple jobs): ").strip().split(',')
                    job_names = [job.strip() for job in job_names]

                    start_times = {}
                    for job_name in job_names:
                        start_time_str = input(f"Enter the start time for the migration '{job_name}' (in HH:MM:SS format): ")
                        start_time = datetime.strptime(start_time_str, "%H:%M:%S").time()
                        start_times[job_name] = start_time

                    days_to_run = int(input("Enter the number of days to run the job (including today): "))

                    now = datetime.now()
                    for job_name, start_time in start_times.items():
                        for day in range(days_to_run):
                            start_datetime = datetime.combine(now + timedelta(days=day), start_time)
                            delay_seconds = (start_datetime - now).total_seconds()
                            start_time_12hr = start_datetime.strftime("%I:%M:%S %p")
                            print(f"Scheduled to start '{job_name}' at: {start_time_12hr} on {start_datetime.date()}")
                            threading.Timer(delay_seconds, self.main_migration_func, [job_name, postgres_conn]).start()

            else:
                print("Invalid choice. Please enter 'new' or 'existing'.")
                self.main()


    def get_migration_details(self, job_name, conn,migrations_table):
        try:
            query = f"SELECT * FROM {migrations_table} WHERE migration_name = '{job_name}'"
            query_result = self.execute_query(conn, query)
            return query_result.to_dict(orient='records')[0]
        except Exception as e:
            print(f"Error while fetching the {job_name} details - {e}")
            return {}

    def get_db_config(self, migration_details_dict):
        db_config = migration_details_dict.get('from_db_config')
        if db_config and not isinstance(db_config, dict):
            db_config = json.loads(db_config)
        return db_config or {
            'hostname': os.environ('FROM_DB_HOST'),
            'port': os.environ('FROM_DB_PORT'),
            'user': os.environ('FROM_DB_USER'),
            'password': os.environ('FROM_DB_PASSWORD'),
            'from_db_type': os.environ('FROM_DB_TYPE')
        }

    def update_migration_details_dict(self,migration_details_dict,insert_records,success,failures,last_id_time,db_config):
        try:
            migration_details_dict['status']=insert_records
            migration_details_dict['last_id']=last_id_time

            migration_update_list=migration_details_dict['migration_update']
            if not migration_update_list:
                migration_update_list=[]
            # print(f"Migration Status {migration_update_list}")
            migration_update_dict={'Success_Records':success,'failed_records':failures}
            # migration_update_list=[migration_update]

            migration_update_list.append(migration_update_dict)
            migration_details_dict['migration_update']=json.dumps(migration_update_list)

            migration_details_dict['from_db_config']=json.dumps(db_config)
            to_mapping=migration_details_dict['table_mappings']
            migration_details_dict['table_mappings']=json.dumps(to_mapping)

            reverse_sync_mapping=migration_details_dict['reverse_sync_mapping']
            migration_details_dict['reverse_sync_mapping']=json.dumps(reverse_sync_mapping)

            migrated_time=datetime.now()
            # print(f"Current_time is {migrated_time}")
            migration_details_dict['last_migrated_time']=migrated_time
        except Exception as e:
            print(f"Error while updating migration details dict : {e}")
        return migration_details_dict

    def select_updated_records(self,columns_list, last_id_list, full_from_table):
        # Constructing the WHERE clause dynamically
        where_conditions = []
        for col, val in zip(columns_list, last_id_list):
            if isinstance(val, str):
                where_conditions.append(f"{col} > '{val}'")
            else:
                where_conditions.append(f"{col} > {val}")

        where_clause = " AND ".join(where_conditions)

        # Constructing the SQL query
        migrate_records = f"SELECT * FROM {full_from_table} WHERE {where_clause}"

        return migrate_records

    def main_migration_func(self,job_name):

        hostname = os.environ('LOCAL_DB_HOST')
        port = os.environ('LOCAL_DB_PORT')
        user = os.environ('LOCAL_DB_USER')
        password = os.environ('LOCAL_DB_PASSWORD')
        db_type = os.environ('LOCAL_DB_TYPE')
        postgres_db_name=os.environ('LOCAL_DB_NAME')
        postgres_conn=connection=self.create_connection(db_type,hostname,postgres_db_name,user,password,port)


        print(f"*********************** Executing job: {job_name}")
        load_dotenv()
        migration_table=os.environ('MIGRATION_TABLE')
        # migration_table='migrations_2'
        print(f"migrations table {migration_table}")
        migration_details_dict=self.get_migration_details(job_name,postgres_conn,migration_table)
        table_flag=migration_details_dict['table_flag']
        print(f"!!!!!!!!!!!!!!!!!!! Table Flag is {table_flag}")

        to_hostname = os.environ('LOCAL_DB_HOST')
        to_port = os.environ('LOCAL_DB_PORT')
        to_db_name =migration_details_dict['to_database']
        to_user = os.environ('LOCAL_DB_USER')
        to_password = os.environ('LOCAL_DB_PASSWORD')
        to_db_type = os.environ('LOCAL_DB_TYPE')
        to_table=migration_details_dict['to_table']
        to_connection=self.create_connection(to_db_type,to_hostname,to_db_name,to_user,to_password,to_port)


        migration_status=migration_details_dict['status']

        if table_flag:
            print(f"############### Table to Table migration")
            if migration_status=='first':
                first_table_migrate=self.first_migration(table_flag,to_connection,
                                                         postgres_conn,migration_details_dict,migration_table,job_name)
            else:
                print(f"############### LAST ID migration")
                last_id_migration=self.last_id_migration(table_flag,to_connection,
                                                         postgres_conn,migration_details_dict,migration_table,job_name)

        else:
            print(f"@@@@@@@@@@@@@@@@@@ Executing {job_name}")

            if migration_status=='first':
                print(f"FIRST MIGRATION")
                first_migration=self.first_migration(table_flag,to_connection,
                                                     postgres_conn,migration_details_dict,migration_table,job_name)
            else:
                print(f"LAST ID MIGRATION")
                last_id_migration=self.last_id_migration(table_flag,to_connection,
                                                         postgres_conn,migration_details_dict,migration_table,job_name)

    def first_migration(self,table_flag,to_connection,postgres_conn,migration_details_dict,migration_table,job_name):
        try:
            if table_flag:
                from_database=migration_details_dict['from_database']
                from_table=migration_details_dict['from_table']

                if not self.is_valid_table_name(from_table):
                    full_from_table=f'[{from_database}].[dbo].[{from_table}]'
                    # print(f"full table name {full_from_table}")
                else:
                    full_from_table=from_table
                print("table to table migration")
                from_query=f"select * from {full_from_table} order by {primary_id_col}"
            else:
                print(f"!!!!!!!!!!!!! query based migration")
                from_query=migration_details_dict['from_query']
                to_query=migration_details_dict['to_query']

            print(f"::::::::::::::common process for both::::::::::")
            #establishing from conn
            db_config=self.get_db_config(migration_details_dict)
            from_host=db_config['hostname']
            from_port=db_config['port']
            from_user=db_config['user']
            from_pwd=db_config['password']
            from_db_type=db_config['from_db_type']
            from_database=migration_details_dict['from_database']
            from_driver=os.environ('FROM_DB_DRIVER')
            from_connection=self.create_connection(from_db_type,from_host,from_database,from_user,from_pwd,from_port,from_driver)
            print(f"Connection ssms: {from_connection}")
            primary_id_col=migration_details_dict['primary_id_column']
            print(f"primary id column is {primary_id_col}")
            df_size=os.environ('DF_SIZE')
            df_size=int(df_size)
            # db_config=self.get_db_config(migration_details_dict)
            to_table=migration_details_dict['to_table']

            max_retries=3
            from_query=' '.join(from_query.split())
            from_query=from_query.rstrip(';')
            print(f"from query before {from_query}")
            count_query=self.get_count_query(from_query)

            count_query=' '.join(count_query.split())
            print(f"COUNT QUERY : {count_query}")
            batch_queries=self.generate_batch_queries(count_query,from_query,from_connection,df_size,primary_id_col)
            if batch_queries:
                for query in batch_queries:
                    print(f"executing query {query}")
                    attempt=0
                    df=None
                    try:
                        while attempt < max_retries:
                            if not self.check_connection(from_connection):
                                logging.warning("Connection lost. Attempting to reconnect...")
                                from_connection=self.create_connection(from_db_type,from_host,from_database,from_user,from_pwd,from_port,from_driver)
                                if from_connection is None:
                                    logging.error("Failed to re-establish connection.")
                                    break
                            df=self.execute_query(from_connection,query)

                            if df is not None:
                                df = df.astype(object).mask(df.isna(), None)
                                print(f"Dataframe got is \n {df.head()}")
                                print(f"################# The number of rows in the DataFrame is: {df.shape[0]}")
                                df=self.modify_uuid_cols(df)
                                break  # Exit the retry loop if successful
                            else:
                                logging.error("Error in executing query, retrying...")
                                attempt += 1
                                time.sleep(5)
                    except Exception as e:
                        logging.error(f"An error occurred: {str(e)}")
                        attempt += 1
                        time.sleep(5)


                    insert_records=self.initate_complete_migration(df,to_connection,postgres_conn,migration_table,job_name,migration_details_dict,to_table,db_config)
            else:
                print(f"No batch queries check count")
        except:
            print(f"Error in first migration {e}")

    def last_id_migration(self,table_flag,to_connection,postgres_conn,migration_details_dict,migration_table,job_name):
        try:
            to_db_name =migration_details_dict['to_database']
            to_table=migration_details_dict['to_table']
            time_column_check=migration_details_dict['time_column_check']
            time_columns = [col.strip() for col in time_column_check.split(',')]
            df_size=os.environ('DF_SIZE')
            df_size=int(df_size)
            print(f"time column check got is {time_columns}")
            primary_id_col=migration_details_dict['primary_id_column']

            last_id=migration_details_dict['last_id']

            if last_id:

                try:
                    if isinstance(last_id,str) or isinstance(last_id,int):
                        if last_id == '[null]' or last_id == 'null' or last_id=='[]':
                            print(f"in if where last id is [null]")
                            last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)

                        else:
                            # last_id=f'[{last_id}]'
                            last_id=json.loads(last_id)
                            print(f"json loads last_id {last_id}")

                except Exception as e:
                    print(f"Erro loading last_id list {e}")
                    last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                    # last_id=self.load_last_id(last_id)
                print(f"last_id after load_last_id is {last_id}")

            if not last_id:

                last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)




            if table_flag:
                print(f"############### Table to Table migration")
                from_table=migration_details_dict['from_table']
                from_database=migration_details_dict['from_database']
                if not self.is_valid_table_name(from_table):
                    full_from_table=f'[{from_database}].[dbo].[{from_table}]'
                    # print(f"full table name {full_from_table}")
                else:
                    full_from_table=from_table
                print(f"##################### The last id migrated is {last_id} from column {time_column_check},{full_from_table}")
                from_query=self.get_updated_records_query(time_columns,last_id,full_from_table,None)

            else:
                print(f"##################### The last id migrated is {last_id} from column {time_column_check}")
                print(f"calling from_query")
                from_query_=migration_details_dict['from_query']
                print(f"from_query from db is {from_query_}")
                to_query=migration_details_dict['to_query']
                if 'modifieddate' in from_query_.lower() or 'modified_date' in from_query_.lower():
                    last_migrated_time=migration_details_dict['last_migrated_time']
                    print(f"last_migrated time is {last_migrated_time} and type {type(last_migrated_time)}")
                    last_migrated_time_str=self.to_mssql_datetime(f'{last_migrated_time}')
                    print(f"last migrated time after fn is {last_migrated_time_str} and type{type(last_migrated_time_str)}")
                    time_columns_dup=copy.deepcopy(time_columns)
                    time_columns_dup.append('ModifiedDate')
                    last_id_dup=copy.deepcopy(last_id)
                    last_id_dup.append(last_migrated_time_str)

                    print(f"after modifying time col and last_migrated time {time_columns},{time_columns_dup},{last_id},{last_id_dup}")
                    from_query_=' '.join(from_query_.split())
                    from_query_=from_query_.rstrip(';')
                    #print(time_columns_dup,"time_columns_dup",last_id_dup) it contains last id, last_migrated_time
                    #return None
                    from_query=self.get_updated_records_query(time_columns_dup,last_id_dup,None,from_query_)
                    print(f"after updating query {from_query}")
                else:
                    from_query_=' '.join(from_query_.split())
                    from_query_=from_query_.rstrip(';')
                    from_query=self.get_updated_records_query(time_columns,last_id,None,from_query_)
                    print(f"From query git is  {from_query}")

            print(f"common process for both")
            #establishing from conn
            db_config=self.get_db_config(migration_details_dict)
            from_host=db_config['hostname']
            from_port=db_config['port']
            from_user=db_config['user']
            from_pwd=db_config['password']
            from_db_type=db_config['from_db_type']
            from_driver=os.environ('FROM_DB_DRIVER')
            from_database=migration_details_dict['from_database']
            from_connection=self.create_connection(from_db_type,from_host,from_database,from_user,from_pwd,from_port,from_driver)

            from_query=' '.join(from_query.split())
            from_query=from_query.rstrip(';')
            print(f"going to count query is {from_query}")
            count_query=self.get_count_query(from_query)
            batch_queries=self.generate_batch_queries(count_query,from_query,from_connection,df_size,time_column_check)
            max_retries=3
            if batch_queries:
                for query in batch_queries:
                    print(f"executing query {query}")
                    attempt=0
                    df=None
                    try:
                        while attempt < max_retries:
                            if not self.check_connection(from_connection):
                                logging.warning("Connection lost. Attempting to reconnect...")
                                from_connection=self.create_connection(from_db_type,from_host,from_database,from_user,from_pwd,from_port,from_driver)
                                if from_connection is None:
                                    logging.error("Failed to re-establish connection.")
                                    break
                            df=self.execute_query(from_connection,query)

                            if df is not None:
                                df = df.astype(object).mask(df.isna(), None)
                                print(f"Dataframe got is \n {df.head()}")
                                print(f"################# The number of rows in the DataFrame is: {df.shape[0]}")
                                df=self.modify_uuid_cols(df)
                                break  # Exit the retry loop if successful
                            else:
                                logging.error("Error in executing query, retrying...")
                                attempt += 1
                                time.sleep(5)
                    except Exception as e:
                        logging.error(f"An error occurred: {str(e)}")
                        attempt += 1
                        time.sleep(5)

                    if df is None:
                        print(f"Error in executing query, check the query once")
                    else:
                        # df=self.execute_query(from_connection,from_query)
                        # df = df.astype(object).mask(df.isna(), None)
                        print(f"DataFrame is \n {df.head()}")
                        print(f"############# The number of rows in the DataFrame is: {df.shape[0]}")
                        print(f'the last 5 rows:{df.tail(1)}')

                        insert_records=self.initate_complete_migration(df,to_connection,postgres_conn,migration_table,job_name,migration_details_dict,to_table,db_config)
            else:
                print(f"No batch Queries formed, check count")
        except Exception as e:
            print(f"Error while updating after last_id {e}")


    def initate_complete_migration(self,df,to_connection,postgres_conn,migration_table,job_name,migration_details_dict,to_table,db_config):
        try:
            to_db_name =migration_details_dict['to_database']
            time_column_check=migration_details_dict['time_column_check']
            time_columns = [col.strip() for col in time_column_check.split(',')]

            print(f"time column check got is {time_columns}")
            primary_id_col=migration_details_dict['primary_id_column']

            if not to_table:
                print(f"######### to table is emty so getting temp_table and to_mapping list")
                temp_table=migration_details_dict['temp_table']
                to_mapping=migration_details_dict['table_mappings']

                print(f"@@@@@@@@@@@ temp table to insert data is {temp_table}")
                print(f"######### table mappings {to_mapping}")

                insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,temp_table,primary_id_col,time_columns)

                if last_id_time is None:
                    print(f"last_id is {last_id_time}")
                    last_id_time=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                    print(f"last_id after {last_id_time}")

                table_names = [item["table_name"] for item in to_mapping]
                print(f"insertion in temp table is done, transferring to tables: {table_names}")

                print(f"now neeed to insert queries in tables")

                insert_flag, inserted_records, failed_records, last_id_tm=self.insert_records_postgresql_to_mapping(postgres_conn,to_mapping,primary_id_col,time_columns)
                print(f"insert_flag :{insert_flag}")
                print(f"inserted_records :: {inserted_records} failed_records {failed_records}")
                print(f"last_id_time_ is {last_id_tm}")

                if insert_flag:
                    print(f"################################## Migration Successfull")

                else:
                    print(f"Errors in Migration")
                    last_id_tm=None

                migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)
                # migration_details_dict_updated['table_mappings']=json.dumps(to_mapping)
                print(f"migration details dict {migration_details_dict_updated}")
                print(f"###################### Details to update in table \n Status:{migration_details_dict_updated['status']},Last ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")
                print(f"Updating table")
                uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})
                return True

            else:
                print(f"To table is {to_table}")
                insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,to_table,primary_id_col,time_columns)
                if last_id_time is None:
                        last_id_time=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)

                migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)

                print(f"############################# Details to update in table \n Status:{migration_details_dict_updated['status']},Last_ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")
                print(f"Updating table")
                uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})
                return True


        except Exception as e:
            print(f"error while completing migration: {e}")
            return False


    def insert_records_postgresql_to_mapping(self,postgres_connection, tables_dict, migration_track_col, time_columns):
        print(f"#######################################################")
        # print(f"Starting postgres connection is {postgres_connection}")
        inserted_records = 0
        failed_records = 0
        insert_flag = True
        batch_size = 5000
        failed_log = []
        last_id_time = None
        total_start_time = time.time()

        def insert_table_data(df, table_name):
            nonlocal inserted_records, failed_records, last_id_time, insert_flag

            columns = df.columns.tolist()
            num_batches = math.ceil(len(df) / batch_size)
            print(f"Processing table {table_name} with {len(columns)} columns")
            print(f"Number of Batches Created: {num_batches} with batch size {batch_size}")

            # Prepare the SQL queries
            columns_str = ', '.join(columns)
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns])

            insert_query_update = f'''
                INSERT INTO {table_name} ({columns_str})
                VALUES %s
                ON CONFLICT ({migration_track_col}) DO UPDATE
                SET {update_clause}
            '''
            insert_query_no_conflict = f'INSERT INTO {table_name} ({columns_str}) VALUES %s'

            for batch_index in range(num_batches):
                start_index = batch_index * batch_size
                end_index = min((batch_index + 1) * batch_size, len(df))

                batch_df = df.iloc[start_index:end_index]
                rows = [tuple(row) for row in batch_df.to_numpy()]

                batch_start_time = time.time()

                try:
                    with postgres_connection.cursor() as cur:
                        execute_values(cur, insert_query_no_conflict, rows)
                    postgres_connection.commit()
                    inserted_records += len(rows)
                except psycopg2.Error as e:
                    print(f"Error inserting batch {batch_index + 1} without ON CONFLICT: {e}")
                    postgres_connection.rollback()

                    try:
                        with postgres_connection.cursor() as cur:
                            execute_values(cur, insert_query_update, rows)
                        postgres_connection.commit()
                        inserted_records += len(rows)
                    except psycopg2.Error as e2:
                        print(f"Error inserting batch {batch_index + 1} with ON CONFLICT: {e2}")
                        postgres_connection.rollback()

                        for row_index, row in batch_df.iterrows():
                            try:
                                with postgres_connection.cursor() as cur:
                                    cur.execute(insert_query_no_conflict, (tuple(row),))
                                postgres_connection.commit()
                                inserted_records += 1
                            except psycopg2.Error as e3:
                                print(f"Error inserting row {row_index + start_index} without ON CONFLICT: {e3}")
                                postgres_connection.rollback()

                                try:
                                    with postgres_connection.cursor() as cur:
                                        cur.execute(insert_query_update, (tuple(row),))
                                    postgres_connection.commit()
                                    inserted_records += 1
                                except psycopg2.Error as e4:
                                    print(f"Error inserting row {row_index + start_index} with ON CONFLICT: {e4}")
                                    postgres_connection.rollback()
                                    failed_records += 1
                                    insert_flag = False
                                    failed_log.append({
                                        'table_name': table_name,
                                        'batch_index': batch_index,
                                        'row_index': row_index + start_index,
                                        'error': str(e4),
                                        'failed_id': row[migration_track_col]  # Capture ID of the failed record
                                    })
                                    with open('error_log.txt', 'a') as f:
                                        f.write(f"Failed row details:\nTable: {table_name}\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e4)}\nFailed ID: {row[migration_track_col]}\n\n")

                batch_end_time = time.time()
                print(f"Inserted batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                print(f"Batch insertion time: {batch_end_time - batch_start_time:.2f} seconds")

                last_id_time = [batch_df[col].iloc[-1] for col in time_columns]

        for table_dict in tables_dict:
            query = table_dict.get("query")
            print(f"query is {query}")
            table_name = table_dict.get("table_name")
            print(f"the data to be inserted on table {table_name}")

            try:

                df = self.execute_query(postgres_connection, query)
                df = df.astype(object).mask(df.isna(), None)
                print(f"query result is \n {df.head()}")
                print(f"The number of rows in the DataFrame is: {df.shape[0]}")

                insert_table_data(df, table_name)
            except Exception as e:
                print(f"Error executing query or inserting data into table {table_name}: {e}")
                with open('error_log.txt', 'a') as f:
                    f.write(f"Error executing query or inserting data into table {table_name}: {str(e)}\n")

        total_end_time = time.time()
        print(f"Total insertion time: {total_end_time - total_start_time:.2f} seconds")
        last_id_time_=self.jsonify_last_id(last_id_time)
        print(f"last_id_time_ is {last_id_time_}")

        return insert_flag, inserted_records, failed_records, last_id_time_


    def modify_uuid_cols(self,df):
        # Regular expression pattern for UUID
        uuid_pattern = re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')

        # Step 1: Identify columns that contain UUIDs
        uuid_columns = []
        for col in df.columns:
            if df[col].apply(lambda x: bool(uuid_pattern.match(str(x)))).any():
                uuid_columns.append(col)

        # Step 2: Convert identified UUID columns to strings
        for col in uuid_columns:
            # df[col] = df[col].astype(str)
            # Apply conversion only where value is a valid UUID
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and uuid_pattern.match(str(x)) else x)
        # Print the columns identified as containing UUIDs
        print(f"Columns identified as containing UUIDs: {uuid_columns}")
        return df

    def check_connection(self, connection):
        """
        Check if the connection is still active by executing a simple query.

        Args:
            connection: The database connection object.

        Returns:
            bool: True if the connection is active, False otherwise.
        """
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            return True
        # except pyodbc.Error:
        except pytds.DatabaseError:
            return False

    def recognize_format(self,s):

        pattern1 = r'^[a-zA-Z0-9]+(\.[a-zA-Z0-9]+)+$'
        pattern2 = r'^\[[a-zA-Z0-9]+\](\.\[[a-zA-Z0-9]+\])+$'
        pattern3 = r'^[a-zA-Z0-9]+(\.[a-zA-Z0-9]+)+$'          # No brackets format
        pattern4 = r'^\[[a-zA-Z0-9_]+\](\.\[[a-zA-Z0-9_]+\])+$'
        pattern5= r'^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)+$'

        if re.match(pattern1, s):
            return True
        elif re.match(pattern2, s):
            return True
        elif re.match(pattern3, s):
            return True
        elif re.match(pattern4, s):
            return True
        elif re.match(pattern5, s):
            return True
        else:
            return False

    def clean_query(self, original_query):
    # Split the query by 'ORDER BY' into parts
        parts = original_query.rsplit('ORDER BY')
        #return None
    # Count the number of ORDER BY clauses
        order_by_count = len(parts) - 1  # Each 'ORDER BY' splits the query into parts

    # Check if there are multiple ORDER BY clauses
        if order_by_count > 1:
        # Rebuild the query without the second ORDER BY clause
            query_without_second_order_by = parts[0] + 'ORDER BY' + ''.join(parts[1:2])  # Keep the first ORDER BY and everything after the first
        elif order_by_count == 1:
        # Only one ORDER BY found, return the part before it
            query_without_second_order_by = parts[0]
        else:
            # No ORDER BY found
            query_without_second_order_by = original_query

    # Remove trailing semicolon and whitespace
        query_cleaned = query_without_second_order_by.rstrip(';').strip()

        return query_cleaned  # Return both cleaned query



    def clean_query_bug2(self, original_query):
        # Remove the last occurrence of ORDER BY
        # Find the last "ORDER BY" in the query
        last_order_by_match = re.search(r'\sORDER\s+BY\s+.*?$', original_query, flags=re.IGNORECASE)

        if last_order_by_match:
            # Remove the matched part (last ORDER BY clause)
            query_without_order_by = original_query[:last_order_by_match.start()].strip()
        else:
            query_without_order_by = original_query.strip()

        # Remove trailing semicolon
        query_cleaned = re.sub(r';$', '', query_without_order_by).strip()

        return query_cleaned

    def clean_query_bug(self,original_query): #removing the unwanted order by
        # Check if the query contains ORDER BY and remove it
        last_five_words =  [word.lower() for word in from_query.split()[-4:]]
        print(f"Last five words of the query: {last_five_words}")
        if 'order' in last_five_words and 'by' in last_five_words:
        # if re.search(r'\sORDER\sBY\s', original_query, flags=re.IGNORECASE):
            query_without_order_by = re.sub(r'\sORDER\sBY\s.*$', '', original_query, flags=re.IGNORECASE).strip()
        else:
            query_without_order_by = original_query.strip()

        # Remove trailing semicolon
        query_cleaned = re.sub(r';$', '', query_without_order_by).strip()

        return query_cleaned

    def get_count_query(self,original_query):
        # Clean the original query
        cleaned_query = self.clean_query(original_query)

        # Construct the count query
        count_query = f"""
        SELECT COUNT(*) AS total_count
        FROM (
            {cleaned_query}
        ) AS subquery;
        """
        print(f"count_query {count_query}")
        return count_query

    def generate_batch_queries(self, count_query, from_query, db_conn, df_size, time_column_check):
        """
        Generate batch SQL queries for pagination from a SQL Server database.

        Parameters:
        - count_query (str): SQL query to get the count of records.
        - from_query (str): SQL query to fetch the records.
        - df_size (int): Number of records per batch.

        Returns:
        - List[str]: List of SQL queries for each batch.
        """
        try:
            print(f"primary id col is {time_column_check}")
            # List to hold the batch queries
            batch_queries = []

            with db_conn.cursor() as cursor:
                # Execute the count query
                cursor.execute(count_query)
                total_count = cursor.fetchone()[0]

            print(f"TOTAL COUNT {total_count} type {type(total_count)}")
            if total_count == 0:
                raise ValueError("No Updated Records in DB")

            # Split the query into words to extract the last 5 words
            # last_five_words = from_query.split()[-5:]  # Get the last 5 words
            last_five_words =  [word.lower() for word in from_query.split()[-4:]]
            print(f"Last five words of the query: {last_five_words}")

            # Check if 'order by' is in the last 5 words
            if 'order' in last_five_words and 'by' in last_five_words:
                print(f"'ORDER BY' clause present")
                # Use the existing ORDER BY clause
                from_query = from_query.rstrip(';')
                base_query = from_query
            else:
                print(f"Adding 'ORDER BY'")
                print(f"time_column_check col is {time_column_check}")
                # Add a placeholder ORDER BY clause
                base_query = f"{from_query} ORDER BY {time_column_check}"

            # Calculate the total number of batches needed
            num_batches = (total_count + df_size - 1) // df_size

            # Generate batch queries
            for batch_number in range(num_batches):
                # Calculate the offset
                offset = batch_number * df_size

                # Construct the batch query with OFFSET and FETCH NEXT
                batch_query = (
                    f"{base_query} "
                    f"OFFSET {offset} ROWS "
                    f"FETCH NEXT {df_size} ROWS ONLY"
                )
                # print(f"batch_query : {batch_query}")

                # Append the batch query to the list
                batch_queries.append(batch_query)

            return batch_queries
        except Exception as e:
            print(f"Exception in count checking {e}")
            return False


    def generate_batch_queries_bak9oct24(self,count_query, from_query,db_conn, df_size,primary_id_col): #in order by checking its checking in whole query
        """
        Generate batch SQL queries for pagination from a SQL Server database.

        Parameters:
        - count_query (str): SQL query to get the count of records.
        - from_query (str): SQL query to fetch the records.
        - df_size (int): Number of records per batch.

        Returns:
        - List[str]: List of SQL queries for each batch.
        """
        try:
            print(f"primary id col is {primary_id_col}")
            # List to hold the batch queries
            batch_queries = []

            with db_conn.cursor() as cursor:
                # Execute the count query
                cursor.execute(count_query)
                total_count = cursor.fetchone()[0]

            print(f"TOTAL COUNT {total_count} type {type(total_count)}")
            if total_count==0:
                raise ValueError("No Updated Records in DB")

            # Determine if the from_query already contains an ORDER BY clause
            if 'order by' in from_query.lower():
                print(f"order by cluase present")
                # Use the existing ORDER BY clause
                from_query=from_query.rstrip(';')
                base_query = from_query

            else:
                print(f"adding order by")
                print(f"primary id col is {primary_id_col}")
                # Add a placeholder ORDER BY clause
                base_query = f"{from_query} ORDER BY {primary_id_col}"

            # Calculate the total number of batches needed
            # Assuming you will use `count_query` externally to determine the number of batches
            # For simplicity, let's assume a dummy total_count value here

            num_batches = (total_count + df_size - 1) // df_size

            # Generate batch queries
            for batch_number in range(num_batches):
                # Calculate the offset
                offset = batch_number * df_size

                # Construct the batch query with OFFSET and FETCH NEXT
                batch_query = (
                    f"{base_query} "
                    f"OFFSET {offset} ROWS "
                    f"FETCH NEXT {df_size} ROWS ONLY"
                )
                print(f"batch_query : {batch_query}")

                # Append the batch query to the list
                batch_queries.append(batch_query)

            return batch_queries
        except Exception as e:
            print(f"Exception in count checking {e}")
            return False

    def to_mssql_datetime(self,timestamp_str):
        """
        Convert a high-precision timestamp string to a format compatible with SQL Server's datetime.

        Args:
        - timestamp_str (str): A timestamp string with high precision (e.g., '2024-08-08 13:27:12.954566').

        Returns:
        - str: A string formatted as SQL Server datetime (e.g., '2024-08-08 13:27:12.954').
        """
        try:
            # Step 1: Parse the string timestamp into a Python datetime object
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')

            # Step 2: Truncate the microseconds to fit MSSQL datetime (3 digits of milliseconds)
            dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)

            # Step 3: Convert the datetime object back to a string compatible with MSSQL datetime format
            mssql_datetime_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

            return mssql_datetime_str
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format: {timestamp_str}. Error: {e}")

    def get_all_records(self,connection,table_name,primary_id_col):
        try:
            df_rows=f"select * from {table_name} order by {primary_id_col}"
            df = self.execute_query(connection,df_rows)
            df = df.astype(object).mask(df.isna(), None)
            return df

        except Exception as e:
            print(f"Error while fetching all records {e}")
            return None

    def load_last_id(self,last_id_str):
        try:
            # Attempt to convert to integer
            last_id_int = int(last_id_str)
            return last_id_int  # Return as integer if successful
        except ValueError:
            try:
                # Attempt to convert to datetime
                last_id_timestamp = datetime.strptime(last_id_str, '%Y-%m-%d %H:%M:%S')
                return last_id_timestamp
            except ValueError:
                try:
                    last_id_timestamp = datetime.fromisoformat(last_id_str)
                    return last_id_timestamp  # Return as datetime if successful
                except ValueError:
                    try:
                        last_id=json.loads(last_id_str)
                        return last_id
                    except ValueError:
                        print(f"Error while loading last_id from db")
                        return None # Return None if conversion fails

    def enclose_uppercase_words(self,input_string):
        # Split the input string by commas and strip any surrounding whitespace
        words = [word.strip() for word in input_string.split(',')]

        # Function to check if a word contains uppercase characters
        def needs_quotes(word):
            return any(char.isupper() for char in word)

        # Enclose words with uppercase characters in double quotes
        quoted_words = [f'"{word}"' if needs_quotes(word) else word for word in words]

        # Join the words back into a single string separated by commas
        return ', '.join(quoted_words)

    def insert_dict(self, conn, table_name, data_dict):
        """
        Insert a row into a PostgreSQL table using a dictionary.

        :param conn: psycopg2 connection object
        :param table_name: Name of the table to insert into
        :param data_dict: Dictionary containing column-value pairs to insert
        """
        try:
            # Convert dictionary values to JSON strings if necessary
            for key, value in data_dict.items():
                if isinstance(value, dict):
                    data_dict[key] = json.dumps(value)

            # Generate the column names and placeholders for the SQL query
            columns = ', '.join(data_dict.keys())
            placeholders = ', '.join(['%s'] * len(data_dict))

            # Create the SQL query
            sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            values = list(data_dict.values())

            # print(f"SQL Query: {sql_query}")
            # print(f"Values: {values}")

            # Execute the query
            with conn.cursor() as cursor:
                cursor.execute(sql_query, values)
                conn.commit()
            print("Insert successful")
            return True
        except Exception as e:
            conn.rollback()
            print(f"Error inserting into table: {e}")
            return False

    def is_valid_table_name(self,table_name):
        pattern = r'^\[\w+\]\.\[\w+\]\.\[\w+\]$'
        return re.match(pattern, table_name) is not None

    def update_table(self,conn, table_name, data_dict, condition_dict):
        """
        Update a PostgreSQL table using a dictionary.

        :param conn: psycopg2 connection object
        :param table_name: Name of the table to update
        :param data_dict: Dictionary containing column-value pairs to update
        :param condition_dict: Dictionary containing column-value pairs for the WHERE condition
        """
        try:
            # Generate the SET part of the SQL query
            set_clause = ', '.join([f"{col} = %s" for col in data_dict.keys()])

            if condition_dict:

            # Generate the WHERE part of the SQL query
                where_clause = ' AND '.join([f"{col} = %s" for col in condition_dict.keys()])


            if condition_dict:
            # Complete SQL query
                sql_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                values = list(data_dict.values()) + list(condition_dict.values())

            else:
                sql_query = f"UPDATE {table_name} SET {set_clause}"
                values = list(data_dict.values())

            # Combine the values for the SET and WHERE parts


            print(f"SQL Quey {sql_query} and values are {values}")

            ## Execute the query
            with conn.cursor() as cursor:

                cursor.execute(sql_query, values)
                conn.commit()
            print("################################## Update successful")
        except Exception as e:
            conn.rollback()
            print(f"Error updating table: {e}")

    def camel_to_snake_case(self,camel_str):
        # Convert a camelCase or PascalCase string to snake_case
        return ''.join(['_' + i.lower() if i.isupper() else i for i in camel_str]).lstrip('_')

    def insert_records_postgresql_batch_5(self, postgres_connection, df, insert_table_name, primary_id_col, time_columns):
        """
        Inserts records into a PostgreSQL table in batches, with error handling for individual rows.

        Args:
            postgres_connection: The connection object to the PostgreSQL database.
            df: The DataFrame containing the data to be inserted.
            insert_table_name: The name of the table where data will be inserted.
            migration_track_col: The column used for tracking the migration progress.
            time_columns: List of columns that hold timestamp data.

        Returns:
            A tuple containing:
            - insert_flag (bool): Indicates whether the insertion was successful.
            - inserted_records (int): The number of successfully inserted records.
            - failed_records (int): The number of failed records.
            - last_id_time (str): JSON string of the last timestamp(s) from the successfully inserted records.

        Process:
        1. Calculates the number of batches needed based on the batch size (5000).
        2. Attempts to insert each batch:
            a. First, with the ON CONFLICT clause to handle conflicts.
            b. If the batch insertion fails, tries inserting the batch without the ON CONFLICT clause.
            c. If the batch insertion still fails, attempts to insert each row individually:
                i. First, with the ON CONFLICT clause.
                ii. If the individual row insertion fails, tries without the ON CONFLICT clause.
        3. Logs errors for any rows that fail to insert.
        4. Tracks the number of successfully inserted and failed records.
        5. Calculates and logs the total insertion time.
        6. Serializes and returns the last timestamp(s) and id(S) from the successfully inserted records.

        Notes:
        - Retries for 3 times when insert/update fails
        - Logs detailed error messages and failed record IDs to 'error_log.txt'.
        """
        # print(f"Inserting into table {insert_table_name}, column {migration_track_col}")
        # print(f"time columns got are {time_columns}")
        try:

            inserted_records = 0
            failed_records = 0
            insert_flag = True
            batch_size = 5000
            failed_log = []
            last_id_time = None
            num_batches = math.ceil(len(df) / batch_size)
            print(f"############# Records to insert: {df.shape[0]}")
            print(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")

            # Extract column names once
            columns = ', '.join(df.columns)
            print(f"Columns: {columns}")

            # Check for time_columns and convert to snake_case if not in columns
            time_column_snake_case = []

            for col in time_columns:
                if col not in columns:
                    # Convert camelCase to snake_case
                    snake_case_col = self.camel_to_snake_case(col)
                    time_column_snake_case.append(snake_case_col)
                else:
                    time_column_snake_case.append(self.camel_to_snake_case(col))  # Convert existing ones too

            print(f"time_column_snake_case::: {time_column_snake_case}")

            # Prepare the update clause for ON CONFLICT
            #print(update_clause)
            # Prepare the update clause for ON CONFLICT (excluding 'device_history_id')
            #update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns  ])
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in time_column_snake_case])

            #update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns])
            print("*******************update_clause",update_clause)
            total_start_time = time.time()

            # Define the insert block operation
            def insert_block_operation():
                nonlocal inserted_records, failed_records, last_id_time, insert_flag

                for batch_index in range(num_batches):
                    start_index = batch_index * batch_size
                    end_index = min((batch_index + 1) * batch_size, len(df))

                    batch_df = df.iloc[start_index:end_index]
                    rows = [tuple(row) for row in batch_df.to_numpy()]

                    insert_query_update = f'''
                        INSERT INTO {insert_table_name} ({columns})
                        VALUES %s
                        ON CONFLICT ({primary_id_col}) DO UPDATE
                        SET {update_clause}
                    '''
                    print("#######################insert_query_update",insert_query_update)
                    # print(f"update clause is {update_clause}")
                    insert_query_no_conflict = f'INSERT INTO {insert_table_name} ({columns}) VALUES %s'

                    batch_start_time = time.time()

                    try:
                        with postgres_connection.cursor() as cur:
                            # Attempt to insert the batch without ON CONFLICT clause
                            print(f"Trying to insert without update")
                            execute_values(cur, insert_query_no_conflict, rows)
                        postgres_connection.commit()

                        # Update last_id_time with the values from the last row of the batch
                        last_id_time = [rows[-1][batch_df.columns.get_loc(col)] for col in time_column_snake_case]

                    except psycopg2.Error as e:
                        print(f"Error inserting batch {batch_index + 1} without ON CONFLICT: {e}")
                        postgres_connection.rollback()  # Rollback after the first error

                        try:
                            with postgres_connection.cursor() as cur:
                                # Attempt to insert the batch with ON CONFLICT clause
                                print(f"trying insert with update")
                                print("updating query execution starts")
                                # print(f"insert query with update {insert_query_update}")
                                execute_values(cur, insert_query_update, rows)
                                print("executed the updating query execution starts")
                            postgres_connection.commit()

                            # Update last_id_time with the values from the last row of the batch
                            last_id_time = [rows[-1][batch_df.columns.get_loc(col)] for col in time_column_snake_case]


                        except psycopg2.Error as e2:
                            print(f"Error inserting batch {batch_index + 1} with ON CONFLICT: {e2}")
                            postgres_connection.rollback()  # Rollback after the second error

                            # Try inserting rows individually
                            for row_index, row in batch_df.iterrows():
                                try:
                                    with postgres_connection.cursor() as cur:
                                        # Attempt to insert individual row without ON CONFLICT clause
                                        cur.execute(insert_query_no_conflict, (row,))
                                    postgres_connection.commit()
                                    inserted_records += 1

                                    # Update last_id_time with the values from the current row
                                    last_id_time = [row[col] for col in time_column_snake_case]
                                except psycopg2.Error as e3:
                                    print(f"Error inserting row {row_index + start_index} without ON CONFLICT: {e3}")
                                    postgres_connection.rollback()  # Rollback after the third error

                                    try:
                                        with postgres_connection.cursor() as cur:
                                            # Attempt to insert individual row without ON CONFLICT clause
                                            cur.execute(insert_query_update, (row,))
                                        postgres_connection.commit()
                                        inserted_records += 1

                                        # Update last_id_time with the values from the current row
                                        last_id_time = [row[col] for col in time_column_snake_case]

                                    except psycopg2.Error as e4:
                                        print(f"Error inserting row {row_index + start_index} with ON CONFLICT: {e4}")
                                        postgres_connection.rollback()  # Rollback after the fourth error
                                        failed_records += 1
                                        insert_flag = False
                                        # Log the failed row details
                                        failed_log.append({
                                            'batch_index': batch_index,
                                            'row_index': row_index + start_index,
                                            'error': str(e4),
                                            'failed_id': row['id']  # Capture ID of the failed record
                                        })
                                        with open('error_log.txt', 'a') as f:
                                            f.write(f"Failed row details:\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e4)}\nFailed ID: {row['id']}\n\n")
                    else:
                        # If batch insertion is successful, count all rows as inserted
                        inserted_records += len(rows)

                    batch_end_time = time.time()
                    print(f"Inserted batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                    print(f"Batch insertion time: {batch_end_time - batch_start_time:.2f} seconds")

                    # Update last_id_time with the time columns from the last row of the current batch
                    # last_id_time = [batch_df[col].iloc[-1] for col in time_columns] #uncomment this

                    # if batch_index == 4:
                    #     break

            # Retry the entire block operation up to 3 times
            for attempt in range(3):
                try:
                    insert_block_operation()
                    break  # Break the retry loop if successful
                except Exception as e:
                    print(f"Error in whole insert block operation on attempt {attempt + 1}: {e}")
                    last_id_time=None
                    with open('error_log.txt', 'a') as f:
                        f.write(f"Error in whole insert block operation on attempt {attempt + 1}: {str(e)}\n")

            total_end_time = time.time()
            print(f"Total insertion time: {total_end_time - total_start_time:.2f} seconds")

            # Serialize and jsonify the last_id_time
            print(f"last_id_time is {last_id_time}")
            if last_id_time:
                last_id_time_ = self.serialize_timestamps_and_jsonify(last_id_time)
                print(f"last_id_time_: {last_id_time_}")
            else:
                last_id_time=None

            if failed_records>0:
                last_id_time_=None

            # return insert_flag, inserted_records, failed_records, last_id_time_
        except Exception as e:
            print(f"Error while inserting records {e}")
        return insert_flag, inserted_records, failed_records, last_id_time_

    def update_records_postgresql_batch(self, postgres_connection, df, update_table_name, migration_track_col, time_columns):
        """
        Updates records in a PostgreSQL table in batches, with error handling for individual rows.

        Args:
            postgres_connection: The connection object to the PostgreSQL database.
            df: The DataFrame containing the data to be updated.
            update_table_name: The name of the table where data will be updated.
            migration_track_col: The column used for tracking the migration progress.
            time_columns: List of columns that hold timestamp data.

        Returns:
            A tuple containing:
            - update_flag (bool): Indicates whether the update was successful.
            - updated_records (int): The number of successfully updated records.
            - failed_records (int): The number of failed records.
            - last_id_time (str): JSON string of the last timestamp(s) from the successfully updated records.

        Process:
        1. Calculates the number of batches needed based on the batch size (5000).
        2. Attempts to update each batch:
            a. Updates the batch records.
            b. If the batch update fails, tries updating the batch one record at a time.
        3. Logs errors for any rows that fail to update.
        4. Tracks the number of successfully updated and failed records.
        5. Calculates and logs the total update time.
        6. Serializes and returns the last timestamp(s) and id(S) from the successfully updated records.

        Notes:
        - Retries for 3 times when update fails
        - Logs detailed error messages and failed record IDs to 'error_log.txt'.
        """

        updated_records = 0
        failed_records = 0
        update_flag = True
        batch_size = 5000
        failed_log = []
        last_id_time = None
        num_batches = math.ceil(len(df) / batch_size)
        print(f"############# Records to update: {df.shape[0]}")
        print(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")

        # Extract column names once, excluding the primary key column
        columns = [col for col in df.columns if col != 'id']
        column_names = ', '.join(columns)
        print(f"Columns: {column_names}")

        # Prepare the set clause for the update statement
        set_clause = ', '.join([f"{col} = %s" for col in columns])
        update_query = f'''
            UPDATE {update_table_name}
            SET {set_clause}
            WHERE id = %s
        '''

        total_start_time = time.time()

        # Define the update block operation
        def update_block_operation():
            nonlocal updated_records, failed_records, last_id_time, update_flag

            for batch_index in range(num_batches):
                start_index = batch_index * batch_size
                end_index = min((batch_index + 1) * batch_size, len(df))

                batch_df = df.iloc[start_index:end_index]
                rows = [tuple(row) for row in batch_df.to_numpy()]

                batch_start_time = time.time()

                try:
                    with postgres_connection.cursor() as cur:
                        # Attempt to update the batch
                        print(f"Trying to update batch {batch_index + 1}")
                        for row in rows:
                            cur.execute(update_query, (*row[1:], row[0]))
                    postgres_connection.commit()
                except psycopg2.Error as e:
                    print(f"Error updating batch {batch_index + 1}: {e}")
                    postgres_connection.rollback()  # Rollback after the first error

                    # Try updating rows individually
                    for row_index, row in batch_df.iterrows():
                        try:
                            with postgres_connection.cursor() as cur:
                                # Attempt to update individual row
                                cur.execute(update_query, (tuple(row[1:]), row['id']))
                            postgres_connection.commit()
                            updated_records += 1
                        except psycopg2.Error as e3:
                            print(f"Error updating row {row_index + start_index}: {e3}")
                            postgres_connection.rollback()  # Rollback after the third error
                            failed_records += 1
                            update_flag = False
                            # Log the failed row details
                            failed_log.append({
                                'batch_index': batch_index,
                                'row_index': row_index + start_index,
                                'error': str(e3),
                                'failed_id': row['id']  # Capture ID of the failed record
                            })
                            with open('error_log.txt', 'a') as f:
                                f.write(f"Failed row details:\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e3)}\nFailed ID: {row['id']}\n\n")
                else:
                    # If batch update is successful, count all rows as updated
                    updated_records += len(rows)

                batch_end_time = time.time()
                print(f"Updated batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                print(f"Batch update time: {batch_end_time - batch_start_time:.2f} seconds")

                # Update last_id_time with the time columns from the last row of the current batch
                last_id_time = [batch_df[col].iloc[-1] for col in time_columns]

        # Retry the entire block operation up to 3 times
        for attempt in range(3):
            try:
                update_block_operation()
                break  # Break the retry loop if successful
            except Exception as e:
                print(f"Error in whole update block operation on attempt {attempt + 1}: {e}")
                with open('error_log.txt', 'a') as f:
                    f.write(f"Error in whole update block operation on attempt {attempt + 1}: {str(e)}\n")

        total_end_time = time.time()
        print(f"Total update time: {total_end_time - total_start_time:.2f} seconds")

        # Serialize and jsonify the last_id_time
        last_id_time_ = self.serialize_timestamps_and_jsonify(last_id_time)
        print(f"last_id_time_: {last_id_time_}")

        return update_flag, updated_records, failed_records, last_id_time_


    def log_failed_batch(self,batch_index, error):
        with open('error_log.txt', 'a') as f:
            f.write(f"Failed batch details:\nBatch index: {batch_index}\nError: {str(error)}\n\n")

    def serialize_timestamps_and_jsonify(self,input_list):
        try:
            if input_list:
                serialized_list = []

                for item in input_list:
                    if isinstance(item, Timestamp):
                        serialized_list.append(item.to_pydatetime().isoformat())
                    elif isinstance(item, datetime):
                        serialized_list.append(item.isoformat())
                    else:
                        serialized_list.append(item)

                return json.dumps(serialized_list)
            else:
                return None
        except Exception as e:
            print(f"Error serialize_timestamps_and_jsonify - {e}")
            return None

    def update_last_id_list(self,db_name, insert_table_name, time_check_cols, primary_id_col):
        try:
            postgres_connection = self.create_postgres_connection(db_name)
            last_id_time = []
            with postgres_connection.cursor() as cursor:
                # Constructing the SELECT query dynamically for multiple columns
                select_cols = ', '.join(time_check_cols)
                if not primary_id_col:
                    query=f"SELECT {select_cols} FROM {insert_table_name} ORDER BY {select_cols} DESC LIMIT 1"
                else:
                    query = f"SELECT {select_cols} FROM {insert_table_name} ORDER BY {primary_id_col} DESC LIMIT 1"

                cursor.execute(query)
                result = cursor.fetchone()


                if result:
                    for value in result:
                        if isinstance(value, datetime):
                            last_id_time.append(value.isoformat())
                        else:
                            last_id_time.append(value)
                else:
                    last_id_time = None

        except Exception as fetch_error:
            last_id_time = None
            print(f"Error fetching last inserted record: {fetch_error}")

        return last_id_time


if __name__ == "__main__":
    scheduler = MigrationScheduler()
    scheduler.main()
scheduler.main_migration_func("integration")