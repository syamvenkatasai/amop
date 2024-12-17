"""
Author : Vyshnavi.K, Bhavani.Ganta
Created Date : 09-12-2024
main fn: main_migration_func
"""

# 1. Standard Library Imports
import os
import json
import re
import time
import math
import threading
from datetime import datetime, timedelta
import copy
import uuid

# 2. Third-Party Imports
import pandas as pd
from pandas import Timestamp
from sqlalchemy import create_engine, exc, text
import pytds
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv

# 3. Local Application Imports
from common_utils.logging_utils import Logging

logging = Logging()


# logging.basicConfig(filename='db_migration.log', level=logging.ERROR,
                    # format='%(asctime)s %(levelname)s:%(message)s')


class MigrationScheduler:

    def create_postgres_connection(self,postgres_db_name):
        print(f"In create_postgres_connection {postgres_db_name}")

        load_dotenv()
        hostname = os.getenv('PSQL_DB_HOST')
        port = os.getenv('PSQL_DB_PORT')
        user = os.getenv('PSQL_DB_USER')
        password = os.getenv('PSQL_DB_PASSWORD')
        db_type = os.getenv('PSQL_DB_TYPE')
        try:
            connection=self.create_connection(db_type,hostname,postgres_db_name,user,password,port)
            logging.info(f"in create postgres connection {connection}")
        except Exception as e:
            logging.info(f"Error while establishing connection {e}")
        return connection

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def create_connection(self,db_type='',host='', db_name='',username='', password='',port='',driver='',max_retry=3):
        """
        Establishes a database connection to PostgreSQL or MSSQL based on the provided parameters.

        Retries the connection up to a specified number of times in case of failure.

        Args:
            db_type (str): Type of the database ('postgresql' or 'mssql').
            host (str): Hostname or IP address of the database server.
            db_name (str): Name of the database.
            username (str): Username for authentication.
            password (str): Password for authentication.
            port (str): Port number for the database connection.
            driver (str): Driver details (used for MSSQL if required).
            max_retry (int): Maximum number of retry attempts (default: 3).

        Returns:
            connection: A database connection object or None if the connection fails.
        """
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
                logging.info("Connection to PostgreSQL DB successful")
            except Exception as e:
                logging.warning(f"Failed to connect to PostgreSQL DB: {e}")
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

                logging.info("Connection to MSSQL successful!")
            except Exception as e:
                logging.warning(f"Failed to connect to MSSQL DB: {e}")
        return connection

    def execute_query(self,connection,query):
        """
        Executes a SQL query using the given database connection and returns the result as a DataFrame.

        Args:
            connection: The database connection object to execute the query.
            query (str): The SQL query to be executed.

        Returns:
            pd.DataFrame: The result of the query as a pandas DataFrame, or None if an error occurs.
        """
        try:
            result_df=pd.read_sql_query(query,connection)
            return result_df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def get_from_clause_index(self,query):
        """
        Identifies the starting index of the "FROM" clause in a SQL query.

        This method uses a regular expression to locate the standalone "FROM" keyword
        in the provided SQL query, ignoring case sensitivity. It raises an exception
        if the "FROM" clause is not found in the query.

        Args:
            query (str): The SQL query string.

        Returns:
            int: The starting index of the "FROM" keyword in the query.

        Raises:
            ValueError: If the query does not contain a "FROM" clause.
        """
    # This regex will match the standalone "FROM" keyword
        match = re.search(r"\bFROM\b", query, re.IGNORECASE)
        # print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@getting the index of from",match.start() )
        if match:
            return match.start()  # Return the starting index of "FROM"
        else:
            raise ValueError("Invalid query: Missing FROM clause.")

    def get_updated_records_query(self,columns, last_id, full_from_table, query):
        """
        Generate a query to fetch updated records based on dynamic conditions.

        This method constructs a SQL query dynamically to fetch records that are updated
        (based on `id` and `modifieddate`) after the provided values. It supports handling
        table aliases and building WHERE clauses intelligently.

        Args:
            columns (list): List of column names to use in the WHERE clause.
            last_id (list): List of values corresponding to the columns for filtering.
            full_from_table (str): Fully qualified table name if provided.
            query (str): The base query string to enhance with dynamic WHERE conditions.

        Returns:
            str: A dynamically constructed query string.
            None: If an error occurs during query generation.

        Raises:
            ValueError: If the base query is invalid (e.g., missing a FROM clause).
        """
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

            # print(f"modified_date_alias {modified_date_alias}")
            #return None
            # Constructing the WHERE clause dynamically
            for col, val in zip(columns, last_id):
                # print(f"col is {col}")
                if col.lower()=='id' and modified_date_alias:
                    col_with_alias = f"{modified_date_alias}.{col}"
                    # print(f"col with alias {col_with_alias} and type {type(modified_date_alias)}")
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
                    # print(f"in else")
                    if isinstance(val, str):
                        where_conditions.append(f"{col} > '{val}'")
                        # print(f"where_conditions {where_conditions}")
                    else:
                        where_conditions.append(f"{col} > {val}")
                        # print(f"where_conditions {where_conditions}")

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
    """
    #Commenting below bloc because this is for user interaction not used in lambda
    def main(self):
        load_dotenv()  # Load environment variables from .env file
        migration_details_dict={}

        print("Beginning migration")

        hostname = os.getenv('PSQL_DB_HOST')
        port = os.getenv('PSQL_DB_PORT')
        db_name = 'Migration_Test'
        user = os.getenv('PSQL_DB_USER')
        password = os.getenv('PSQL_DB_PASSWORD')
        db_type = os.getenv('PSQL_DB_TYPE')
        migration_table=os.getenv('MIGRATION_TABLE')

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
                self.main()"""


    def get_migration_details(self, job_name, conn,migrations_table):
        """
        Fetch migration details for a specific job from the migrations table.

        This method queries the database to retrieve migration details for a given job name.
        It executes the query, fetches the result as a pandas DataFrame, and converts it
        to a dictionary format.

        Args:
            job_name (str): The name of the migration job to fetch details for.
            conn (object): The database connection object to execute the query.
            migrations_table (str): The name of the migrations table in the database.

        Returns:
            dict: A dictionary containing the details of the migration job.
                If no details are found or an error occurs, an empty dictionary is returned.
        """
        try:
            query = f"SELECT * FROM {migrations_table} WHERE migration_name = '{job_name}'"
            query_result = self.execute_query(conn, query)
            return query_result.to_dict(orient='records')[0]
        except Exception as e:
            print(f"Error while fetching the {job_name} details - {e}")
            return {}

    def get_db_config(self, migration_details_dict):
        """
        Extract and process database configuration from migration details.

        This method retrieves the database configuration from the given migration details dictionary.
        If the configuration is a JSON string, it converts it into a Python dictionary.
        If the configuration is not present or invalid, it falls back to a default configuration.

        Args:
            migration_details_dict (dict): A dictionary containing migration details,
                                        including the database configuration under 'from_db_config'.

        Returns:
            dict: A dictionary containing database configuration details, either from the input
        """
        db_config = migration_details_dict.get('from_db_config')
        if db_config and not isinstance(db_config, dict):
            db_config = json.loads(db_config)
        return db_config or {
            'hostname': os.getenv('MSSQL_DB_HOST'),
            'port': os.getenv('MSSQL_DB_PORT'),
            'user': os.getenv('MSSQL_DB_USER'),
            'password': os.getenv('MSSQL_DB_PASSWORD'),
            'from_db_type': os.getenv('MSSQL_DB_TYPE')
        }

    def update_migration_details_dict(
            self,
            migration_details_dict,
            insert_records,
            success,
            failures,
            last_id_time,
            db_config):
        """
        Update the migration details dictionary with the latest migration status.

        This method updates various fields in the migration details dictionary,
        including status, last migrated ID/time, database configurations,
        and mappings, while handling success and failure records.

        Args:
            migration_details_dict (dict): The dictionary containing migration details to be updated.
            insert_records (int): The number of records inserted in the migration.
            success (int): The count of successfully migrated records.
            failures (int): The count of failed migrations.
            last_id_time (str/datetime): The last ID or timestamp of the migrated record.
            db_config (dict): The database configuration dictionary.

        Returns:
            dict: The updated migration details dictionary.
        """
        try:
            # Update the current migration status and the last migrated ID/time
            migration_details_dict['status']=insert_records
            migration_details_dict['last_id']=last_id_time

            # Update migration update list with success and failure records
            migration_update_list=migration_details_dict['migration_update'] or []
            if isinstance(migration_update_list, str):
                migration_update_list = json.loads(migration_update_list)  # If it's a string, convert it back to a list
            if not isinstance(migration_update_list, list):
                migration_update_list = []
            print(f"Migration Status {migration_update_list}")
            migration_update_dict={'Success_Records':success,'failed_records':failures}
            #migration_update_list=[migration_update]

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
            logging.error(f"Error while updating migration details dict : {e}")
        return migration_details_dict

    def select_updated_records(self,columns_list, last_id_list, full_from_table):
        """
        Construct and return a SQL query to select updated records based on dynamic column values.

        This method generates a WHERE clause dynamically using the provided list of columns
        and corresponding last IDs or values. The final SQL query is constructed to select records
        that have values greater than the provided last IDs.

        Args:
            columns_list (list): List of column names used for comparison in the WHERE clause.
            last_id_list (list): List of last ID values to compare against for each column in columns_list.
            full_from_table (str): The name of the table from which records are selected.

        Returns:
            str: The constructed SQL query as a string.
        """
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
        """
        The main migration function that handles the process of migrating data from one database to another.
        This includes both table-to-table migration and query-based migration, with support for reverse synchronization.

        Args:
            job_name (str): The name of the migration job to be executed.

        Returns:
            bool: Returns True if the migration job completes successfully, otherwise returns False.
        """
        try:
            print("starting the job",{job_name})
            load_dotenv()
            hostname = os.getenv('PSQL_DB_HOST')
            port = os.getenv('PSQL_DB_PORT')
            user = os.getenv('PSQL_DB_USER')
            password = os.getenv('PSQL_DB_PASSWORD')
            db_type = os.getenv('PSQL_DB_TYPE')
            db_name=os.getenv('PSQL_DB_NAME')
            logging.info("postgress sql db configuration loaded successfully")
            # print(hostname,port)

            # Create the PostgreSQL connection to the source database
            postgres_conn = self.create_connection(
                db_type, hostname, db_name, user, password, port
            )
            logging.info(f"*********************** Executing job: {job_name}")
            load_dotenv()
            migration_table=os.getenv('MIGRATION_TABLE')
            # migration_table='migrations_2'
            logging.info(f"migrations table {migration_table}")
            migration_details_dict=self.get_migration_details(job_name,postgres_conn,migration_table)

            table_flag=migration_details_dict['table_flag']
            logging.info(f"!!!!!!!!!!!!!!!!!!! Table Flag is {table_flag}")


            to_hostname = os.getenv('PSQL_DB_HOST')
            to_port = os.getenv('PSQL_DB_PORT')
            to_db_name =migration_details_dict['to_database']
            to_user = os.getenv('PSQL_DB_USER')
            to_password = os.getenv('PSQL_DB_PASSWORD')
            to_db_type = os.getenv('PSQL_DB_TYPE')
            to_table=migration_details_dict['to_table']
            to_connection=self.create_connection(to_db_type,to_hostname,to_db_name,to_user,to_password,to_port)


            migration_status=migration_details_dict['status']
            reverse_sync_flag = migration_details_dict["reverse_sync"]
            reverse_sync_mappings = migration_details_dict["reverse_sync_mapping"]

            # Handle reverse sync if the flag is set
            if reverse_sync_flag:
                logging.info(
                    f"#################### Reverse sync for {job_name} starting"
                )
                reverse_sync = self.reverse_sync_migration(
                    job_name,
                    to_connection,
                    reverse_sync_mappings,
                    migration_details_dict,
                )

            if table_flag:
                logging.info(f"############### Table to Table migration")
                if migration_status=='first':
                    first_table_migrate=self.first_migration(table_flag,to_connection,
                                                         postgres_conn,migration_details_dict,migration_table,job_name)
                else:
                    logging.info(f"############### LAST ID migration")
                    last_id_migration=self.last_id_migration(table_flag,to_connection,
                                                         postgres_conn,migration_details_dict,migration_table,job_name)

            else:
                logging.info(f"@@@@@@@@@@@@@@@@@@ Executing {job_name}")

                if migration_status=='first':
                    logging.info(f"FIRST MIGRATION")
                    first_migration=self.first_migration(table_flag,to_connection,
                                                     postgres_conn,migration_details_dict,migration_table,job_name)
                else:
                    logging.info(f"LAST ID MIGRATION")
                    last_id_migration=self.last_id_migration(table_flag,to_connection, postgres_conn,migration_details_dict,migration_table,job_name)
            return_flag = True
        except Exception as e:
            # If any error occurs, log the error and set return_flag to False
            return_flag = False
            logging.error(f"Error in job {job_name} -{e}")
        return return_flag

    def first_migration(
            self,
            table_flag,
            to_connection,
            postgres_conn,
            migration_details_dict,
            migration_table,
            job_name
    ):
        """
        Perform the first migration process for moving data from
        a source (SQL Server or query) to a target database (PostgreSQL).

        Args:
            table_flag (bool): Flag to determine if the migration is based on a table or a query.
            to_connection: The connection object to the target database (PostgreSQL).
            postgres_conn: The connection to the PostgreSQL database.
            migration_details_dict (dict): Contains all the necessary details for
            migration like source database, table, queries, etc.
            migration_table (str): The target migration table name.
            job_name (str): The name of the job to log migration status.

        Returns:
            None
        """
        try:
            if table_flag:
                from_database=migration_details_dict['from_database']
                from_table=migration_details_dict['from_table']

                if not self.is_valid_table_name(from_table):
                    full_from_table=f'[{from_database}].[dbo].[{from_table}]'
                    # print(f"full table name {full_from_table}")
                else:
                    full_from_table=from_table
                logging.info("table to table migration")
                from_query=f"select * from {full_from_table} order by {primary_id_col}"
            else:
                logging.info(f"!!!!!!!!!!!!! query based migration")
                from_query=migration_details_dict['from_query']
                to_query=migration_details_dict['to_query']

            logging.info(f"::::::::::::::common process for both::::::::::")
            #establishing from conn
            db_config=self.get_db_config(migration_details_dict)
            from_host=db_config['hostname']
            from_port=db_config['port']
            from_user=db_config['user']
            from_pwd=db_config['password']
            from_db_type=db_config['from_db_type']
            from_database=migration_details_dict['from_database']
            from_driver=os.getenv('MSSQL_DB_DRIVER')
            from_connection=self.create_connection(from_db_type,from_host,from_database,from_user,from_pwd,from_port,from_driver)
            logging.info(f"Connection ssms: {from_connection}")
            primary_id_col=migration_details_dict['primary_id_column']
            logging.info(f"primary id column is {primary_id_col}")
            df_size=os.getenv('DF_SIZE')
            df_size=int(df_size)
            # db_config=self.get_db_config(migration_details_dict)
            to_table=migration_details_dict['to_table']

            max_retries=3
            from_query=' '.join(from_query.split())
            from_query=from_query.rstrip(';')
            logging.info(f"from query before {from_query}")
            count_query=self.get_count_query(from_query)

            count_query=' '.join(count_query.split())
            logging.info(f"COUNT QUERY : {count_query}")
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
                logging.info(f"No batch queries check count")
        except:
            logging.error(f"Error in first migration {e}")

    def last_id_migration(
            self,
            table_flag,
            to_connection,
            postgres_conn,
            migration_details_dict,
            migration_table,
            job_name
    ):
        """
        Migrates data from one database table to another using a batch processing approach.
        It performs migrations based on the last migrated record (identified by the `last_id`) and applies
        various checks such as connection validation, query execution, and retries in case of failure.

        Args:
            table_flag (bool): A flag indicating whether to perform table-to-table migration (True) or use custom queries (False).
            to_connection (object): The database connection object for the destination database.
            postgres_conn (object): The connection object for PostgreSQL (used if needed for custom queries).
            migration_details_dict (dict): A dictionary containing migration details such as the source and target tables, time columns, and last migrated ID.
            migration_table (str): The name of the migration table in the destination database.
            job_name (str): The name of the migration job.

        Returns:
            None: This function does not return any values. It performs data migration and logs the progress. If an error occurs, it is logged.

        Raises:
            ValueError: If no records are available for migration (i.e., batch queries return no results).
            Exception: If there is a failure during the migration process (e.g., database connection issues or query execution errors).
        """
        try:
            to_db_name =migration_details_dict['to_database']
            to_table=migration_details_dict['to_table']
            time_column_check=migration_details_dict['time_column_check']
            time_columns = [col.strip() for col in time_column_check.split(',')]
            df_size=os.getenv('DF_SIZE')
            df_size=int(df_size)
            logging.info(f"time column check got is {time_columns}")
            primary_id_col=migration_details_dict['primary_id_column']

            last_id=migration_details_dict['last_id']

            if last_id:

                try:
                    if isinstance(last_id,str) or isinstance(last_id,int):
                        if last_id == '[null]' or last_id == 'null' or last_id=='[]':
                            logging.info(f"in if where last id is [null]")
                            last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)

                        else:
                            # last_id=f'[{last_id}]'
                            last_id=json.loads(last_id)
                            logging.info(f"json loads last_id {last_id}")

                except Exception as e:
                    logging.exception(f"Erro loading last_id list {e}")
                    last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                    # last_id=self.load_last_id(last_id)
                logging.error(f"last_id after load_last_id is {last_id}")

            if not last_id:
                last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)




            if table_flag:
                logging.info(f"############### Table to Table migration")
                from_table=migration_details_dict['from_table']
                from_database=migration_details_dict['from_database']
                if not self.is_valid_table_name(from_table):
                    full_from_table=f'[{from_database}].[dbo].[{from_table}]'
                    # print(f"full table name {full_from_table}")
                else:
                    full_from_table=from_table
                logging.info(f"##################### The last id migrated is {last_id} from column {time_column_check},{full_from_table}")
                from_query=self.get_updated_records_query(time_columns,last_id,full_from_table,None)

            else:
                logging.info(f"##################### The last id migrated is {last_id} from column {time_column_check}")
                logging.info(f"calling from_query")
                from_query_=migration_details_dict['from_query']
                logging.info(f"from_query from db is {from_query_}")
                to_query=migration_details_dict['to_query']
                if 'modifieddate' in from_query_.lower() or 'modified_date' in from_query_.lower():
                    last_migrated_time=migration_details_dict['last_migrated_time']
                    logging.info(f"last_migrated time is {last_migrated_time} and type {type(last_migrated_time)}")
                    last_migrated_time_str=self.to_mssql_datetime(f'{last_migrated_time}')
                    logging.info(f"last migrated time after fn is {last_migrated_time_str} and type{type(last_migrated_time_str)}")
                    time_columns_dup=copy.deepcopy(time_columns)
                    time_columns_dup.append('ModifiedDate')
                    last_id_dup=copy.deepcopy(last_id)
                    last_id_dup.append(last_migrated_time_str)

                    logging.info(f"after modifying time col and last_migrated time {time_columns},{time_columns_dup},{last_id},{last_id_dup}")
                    from_query_=' '.join(from_query_.split())
                    from_query_=from_query_.rstrip(';')
                    #print(time_columns_dup,"time_columns_dup",last_id_dup) it contains last id, last_migrated_time
                    #return None
                    from_query=self.get_updated_records_query(time_columns_dup,last_id_dup,None,from_query_)
                    logging.info(f"after updating query {from_query}")
                else:
                    from_query_=' '.join(from_query_.split())
                    from_query_=from_query_.rstrip(';')
                    from_query=self.get_updated_records_query(time_columns,last_id,None,from_query_)
                    logging.info(f"From query git is  {from_query}")

            logging.info(f"common process for both")
            #establishing from conn
            db_config=self.get_db_config(migration_details_dict)
            from_host=db_config['hostname']
            from_port=db_config['port']
            from_user=db_config['user']
            from_pwd=db_config['password']
            from_db_type=db_config['from_db_type']
            from_driver=os.getenv('MSSQL_DB_DRIVER')
            from_database=migration_details_dict['from_database']
            from_connection=self.create_connection(from_db_type,from_host,from_database,from_user,from_pwd,from_port,from_driver)

            from_query=' '.join(from_query.split())
            from_query=from_query.rstrip(';')
            logging.info(f"going to count query is {from_query}")
            count_query=self.get_count_query(from_query)
            batch_queries=self.generate_batch_queries(count_query,from_query,from_connection,df_size,time_column_check)
            max_retries=3
            if batch_queries:
                for query in batch_queries:
                    logging.info(f"executing query {query}")
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
                                logging.info(f"Dataframe got is \n {df.head()}")
                                logging.info(f"################# The number of rows in the DataFrame is: {df.shape[0]}")
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
                        logging.info(f"Error in executing query, check the query once")
                    else:
                        # df=self.execute_query(from_connection,from_query)
                        # df = df.astype(object).mask(df.isna(), None)
                        logging.info(f"DataFrame is \n {df.head()}")
                        logging.info(f"############# The number of rows in the DataFrame is: {df.shape[0]}")
                        logging.info(f'the last 5 rows:{df.tail(1)}')

                        insert_records=self.initate_complete_migration(df,to_connection,postgres_conn,migration_table,job_name,migration_details_dict,to_table,db_config)
            else:
                logging.info(f"No batch Queries formed, check count")
        except Exception as e:
            logging.error(f"Error while updating after last_id {e}")


    def initate_complete_migration(self,df,to_connection,postgres_conn,migration_table,job_name,migration_details_dict,to_table,db_config):
        try:
            to_db_name =migration_details_dict['to_database']
            time_column_check=migration_details_dict['time_column_check']
            time_columns = [col.strip() for col in time_column_check.split(',')]

            # print(f"time column check got is {time_columns}")
            primary_id_col=migration_details_dict['primary_id_column']

            if not to_table:
                logging.info(f"######### to table is emty so getting temp_table and to_mapping list")
                temp_table=migration_details_dict['temp_table']
                to_mapping=migration_details_dict['table_mappings']

                logging.info(f"@@@@@@@@@@@ temp table to insert data is {temp_table}")
                logging.info(f"######### table mappings {to_mapping}")

                insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,temp_table,primary_id_col,time_columns)

                if last_id_time is None:
                    logging.info(f"last_id is {last_id_time}")
                    last_id_time=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                    logging.info(f"last_id after {last_id_time}")

                table_names = [item["table_name"] for item in to_mapping]
                logging.info(f"insertion in temp table is done, transferring to tables: {table_names}")

                logging.info(f"now neeed to insert queries in tables")

                insert_flag, inserted_records, failed_records, last_id_tm=self.insert_records_postgresql_to_mapping(postgres_conn,to_mapping,primary_id_col,time_columns)
                logging.info(f"insert_flag :{insert_flag}")
                logging.info(f"inserted_records :: {inserted_records} failed_records {failed_records}")
                logging.info(f"last_id_time_ is {last_id_tm}")

                if insert_flag:
                    logging.info(f"################################## Migration Successfull")

                else:
                    logging.warning(f"Errors in Migration")
                    last_id_tm=None

                migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)
                # migration_details_dict_updated['table_mappings']=json.dumps(to_mapping)
                logging.info(f"migration details dict {migration_details_dict_updated}")
                logging.info(f"###################### Details to update in table \n Status:{migration_details_dict_updated['status']},Last ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")
                logging.info(f"Updating table")
                uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})
                return True

            else:
                logging.info(f"To table is {to_table}")
                insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,to_table,primary_id_col,time_columns)
                if last_id_time is None:
                        last_id_time=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)

                migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)

                logging.info(f"############################# Details to update in table \n Status:{migration_details_dict_updated['status']},Last_ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")
                logging.info(f"Updating table")
                uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})
                return True


        except Exception as e:
            logging.error(f"error while completing migration: {e}")
            return False


    def insert_records_postgresql_to_mapping(self,postgres_connection, tables_dict, migration_track_col, time_columns):
        logging.info(f"#######################################################")
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
            logging.info(f"Processing table {table_name} with {len(columns)} columns")
            logging.info(f"Number of Batches Created: {num_batches} with batch size {batch_size}")

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
                    logging.warning(f"Error inserting batch {batch_index + 1} without ON CONFLICT: {e}")
                    postgres_connection.rollback()

                    try:
                        with postgres_connection.cursor() as cur:
                            execute_values(cur, insert_query_update, rows)
                        postgres_connection.commit()
                        inserted_records += len(rows)
                    except psycopg2.Error as e2:
                        logging.warning(f"Error inserting batch {batch_index + 1} with ON CONFLICT: {e2}")
                        postgres_connection.rollback()

                        for row_index, row in batch_df.iterrows():
                            try:
                                with postgres_connection.cursor() as cur:
                                    cur.execute(insert_query_no_conflict, (tuple(row),))
                                postgres_connection.commit()
                                inserted_records += 1
                            except psycopg2.Error as e3:
                                logging.warning(f"Error inserting row {row_index + start_index} without ON CONFLICT: {e3}")
                                postgres_connection.rollback()

                                try:
                                    with postgres_connection.cursor() as cur:
                                        cur.execute(insert_query_update, (tuple(row),))
                                    postgres_connection.commit()
                                    inserted_records += 1
                                except psycopg2.Error as e4:
                                    logging.warning(f"Error inserting row {row_index + start_index} with ON CONFLICT: {e4}")
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
                logging.info(f"Inserted batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                logging.info(f"Batch insertion time: {batch_end_time - batch_start_time:.2f} seconds")

                last_id_time = [batch_df[col].iloc[-1] for col in time_columns]

        for table_dict in tables_dict:
            query = table_dict.get("query")
            # print(f"query is {query}")
            table_name = table_dict.get("table_name")
            logging.info(f"the data to be inserted on table {table_name}")

            try:

                df = self.execute_query(postgres_connection, query)
                df = df.astype(object).mask(df.isna(), None)
                logging.info(f"query result is \n {df.head()}")
                logging.info(f"The number of rows in the DataFrame is: {df.shape[0]}")

                insert_table_data(df, table_name)
            except Exception as e:
                logging.error(f"Error executing query or inserting data into table {table_name}: {e}")
                with open('error_log.txt', 'a') as f:
                    f.write(f"Error executing query or inserting data into table {table_name}: {str(e)}\n")

        total_end_time = time.time()
        logging.info(f"Total insertion time: {total_end_time - total_start_time:.2f} seconds")
        last_id_time_=self.jsonify_last_id(last_id_time)
        logging.info(f"last_id_time_ is {last_id_time_}")

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
            logging.info(f"primary id col is {time_column_check}")
            # List to hold the batch queries
            batch_queries = []

            with db_conn.cursor() as cursor:
                # Execute the count query
                cursor.execute(count_query)
                total_count = cursor.fetchone()[0]

            logging.info(f"TOTAL COUNT {total_count} type {type(total_count)}")
            if total_count == 0:
                raise ValueError("No Updated Records in DB")

            # Split the query into words to extract the last 5 words
            # last_five_words = from_query.split()[-5:]  # Get the last 5 words
            last_five_words =  [word.lower() for word in from_query.split()[-4:]]
            # print(f"Last five words of the query: {last_five_words}")

            # Check if 'order by' is in the last 5 words
            if 'order' in last_five_words and 'by' in last_five_words:
                # print(f"'ORDER BY' clause present")
                # Use the existing ORDER BY clause
                from_query = from_query.rstrip(';')
                base_query = from_query
            else:
                # print(f"Adding 'ORDER BY'")
                # print(f"time_column_check col is {time_column_check}")
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
            logging.error(f"Exception in count checking {e}")
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
            logging.error(f"Error while fetching all records {e}")
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
                        logging.error(f"Error while loading last_id from db")
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
            logging.info("Insert successful")
            return True
        except Exception as e:
            conn.rollback()
            logging.error(f"Error inserting into table: {e}")
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


            logging.info(f"SQL Quey {sql_query} and values are {values}")

            ## Execute the query
            with conn.cursor() as cursor:

                cursor.execute(sql_query, values)
                conn.commit()
            logging.info("################################## Update successful")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating table: {e}")

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
            logging.info(f"############# Records to insert: {df.shape[0]}")
            logging.info(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")

            # Extract column names once
            columns = ', '.join(df.columns)
            # print(f"Columns: {columns}")

            # Check for time_columns and convert to snake_case if not in columns
            time_column_snake_case = []

            for col in time_columns:
                if col not in columns:
                    # Convert camelCase to snake_case
                    snake_case_col = self.camel_to_snake_case(col)
                    time_column_snake_case.append(snake_case_col)
                else:
                    time_column_snake_case.append(self.camel_to_snake_case(col))  # Convert existing ones too

            logging.info(f"time_column_snake_case::: {time_column_snake_case}")

            # Prepare the update clause for ON CONFLICT
            #print(update_clause)
            # Prepare the update clause for ON CONFLICT (excluding 'device_history_id')
            #update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns  ])
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in time_column_snake_case])

            #update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns])
            # logging.info("*******************update_clause",update_clause)
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
                    # print("#######################insert_query_update",insert_query_update)
                    # print(f"update clause is {update_clause}")
                    insert_query_no_conflict = f'INSERT INTO {insert_table_name} ({columns}) VALUES %s'

                    batch_start_time = time.time()

                    try:
                        with postgres_connection.cursor() as cur:
                            # Attempt to insert the batch without ON CONFLICT clause
                            logging.info(f"Trying to insert without update")
                            execute_values(cur, insert_query_no_conflict, rows)
                        postgres_connection.commit()

                        # Update last_id_time with the values from the last row of the batch
                        last_id_time = [rows[-1][batch_df.columns.get_loc(col)] for col in time_column_snake_case]

                    except psycopg2.Error as e:
                        logging.warning(f"Error inserting batch {batch_index + 1} without ON CONFLICT: {e}")
                        postgres_connection.rollback()  # Rollback after the first error

                        try:
                            with postgres_connection.cursor() as cur:
                                # Attempt to insert the batch with ON CONFLICT clause
                                logging.info(f"trying insert with update")
                                # print("updating query execution starts")
                                # print(f"insert query with update {insert_query_update}")
                                execute_values(cur, insert_query_update, rows)
                                # print("executed the updating query execution starts")
                            postgres_connection.commit()

                            # Update last_id_time with the values from the last row of the batch
                            last_id_time = [rows[-1][batch_df.columns.get_loc(col)] for col in time_column_snake_case]


                        except psycopg2.Error as e2:
                            logging.warning(f"Error inserting batch {batch_index + 1} with ON CONFLICT: {e2}")
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
                                    logging.warning(f"Error inserting row {row_index + start_index} without ON CONFLICT: {e3}")
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
                                        logging.warning(f"Error inserting row {row_index + start_index} with ON CONFLICT: {e4}")
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
                                        # with open('error_log.txt', 'a') as f:
                                        #     f.write(f"Failed row details:\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e4)}\nFailed ID: {row['id']}\n\n")
                                        logging.error(
                                            f"Failed row details:\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e4)}\nFailed ID: {row['id']}\n\n"
                                        )
                    else:
                        # If batch insertion is successful, count all rows as inserted
                        inserted_records += len(rows)

                    batch_end_time = time.time()
                    logging.info(f"Inserted batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                    logging.info(f"Batch insertion time: {batch_end_time - batch_start_time:.2f} seconds")

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
                    logging.error(f"Error in whole insert block operation on attempt {attempt + 1}: {e}")
                    last_id_time=None
                    logging.error(
                        f"Error in whole insert block operation on attempt {attempt + 1}: {str(e)}\n"
                    )
                    # with open('error_log.txt', 'a') as f:
                    #     f.write(f"Error in whole insert block operation on attempt {attempt + 1}: {str(e)}\n")

            total_end_time = time.time()
            logging.info(f"Total insertion time: {total_end_time - total_start_time:.2f} seconds")

            # Serialize and jsonify the last_id_time
            logging.info(f"last_id_time is {last_id_time}")
            if last_id_time:
                last_id_time_ = self.serialize_timestamps_and_jsonify(last_id_time)
                logging.info(f"last_id_time_: {last_id_time_}")
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
        logging.info(f"############# Records to update: {df.shape[0]}")
        logging.info(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")

        # Extract column names once, excluding the primary key column
        columns = [col for col in df.columns if col != 'id']
        column_names = ', '.join(columns)
        logging.info(f"Columns: {column_names}")

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
                        logging.info(f"Trying to update batch {batch_index + 1}")
                        for row in rows:
                            cur.execute(update_query, (*row[1:], row[0]))
                    postgres_connection.commit()
                except psycopg2.Error as e:
                    logging.warning(f"Error updating batch {batch_index + 1}: {e}")
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
                            logging.warning(f"Error updating row {row_index + start_index}: {e3}")
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
                            logging.error(
                                f"Failed row details:\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e3)}\nFailed ID: {row['id']}\n\n"
                            )
                            # with open('error_log.txt', 'a') as f:
                            #     f.write(f"Failed row details:\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e3)}\nFailed ID: {row['id']}\n\n")
                else:
                    # If batch update is successful, count all rows as updated
                    updated_records += len(rows)

                batch_end_time = time.time()
                logging.info(f"Updated batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                logging.info(f"Batch update time: {batch_end_time - batch_start_time:.2f} seconds")

                # Update last_id_time with the time columns from the last row of the current batch
                last_id_time = [batch_df[col].iloc[-1] for col in time_columns]

        # Retry the entire block operation up to 3 times
        for attempt in range(3):
            try:
                update_block_operation()
                break  # Break the retry loop if successful
            except Exception as e:
                logging.error(f"Error in whole update block operation on attempt {attempt + 1}: {e}")
                # with open('error_log.txt', 'a') as f:
                #     f.write(f"Error in whole update block operation on attempt {attempt + 1}: {str(e)}\n")

        total_end_time = time.time()
        logging.info(f"Total update time: {total_end_time - total_start_time:.2f} seconds")

        # Serialize and jsonify the last_id_time
        last_id_time_ = self.serialize_timestamps_and_jsonify(last_id_time)
        logging.info(f"last_id_time_: {last_id_time_}")

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
            logging.info(f"Error serialize_timestamps_and_jsonify - {e}")
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
            logging.error(f"Error fetching last inserted record: {fetch_error}")

        return last_id_time

    def get_ssms_last_id(self, ssms_connection, full_from_table):
        """
            Retrieves the maximum ID from a specific table in the SQL Server database.

            Args:
                ssms_connection: The connection object to the SQL Server Management Studio (SSMS) database.
                full_from_table (str): The name of the table from which to fetch the maximum ID and last modified date.

            Returns:
                tuple: A tuple containing the maximum ID (int or None) .
                False: In case of an error during the query execution.
        """
        try:
        # Query to fetch the maximum ID and last modified date
            query = f"SELECT MAX(id) AS last_id, MAX(modifieddate) AS last_modified_date FROM {full_from_table}"

        # Create a cursor and execute the query
            cursor = ssms_connection.cursor()
            cursor.execute(query)

            result = cursor.fetchone()
            last_id = result[0] if result else None

            # Close the connection
            cursor.close()
            return last_id
        except Exception as e:
            print(f"Error occurred: {e}")
            return False

    def get_ssms_last_id_(self, ssms_connection, full_from_table): #removing on 11-dec-2024 fro revcustomer bug
        """
        Retrieves the maximum ID and last modified date from a specific table in the SQL Server database.

        Args:
            ssms_connection: The connection object to the SQL Server Management Studio (SSMS) database.
            full_from_table (str): The name of the table from which to fetch the maximum ID and last modified date.

        Returns:
            tuple: A tuple containing the maximum ID (int or None) and the last modified date in PostgreSQL format (str or None).
            False: In case of an error during the query execution.
        """
        try:
        # Query to fetch the maximum ID and last modified date
            query = f"SELECT MAX(id) AS last_id, MAX(modifieddate) AS last_modified_date FROM {full_from_table}"

        # Create a cursor and execute the query
            cursor = ssms_connection.cursor()
            cursor.execute(query)

        # Fetch the result
            result = cursor.fetchone()
            if result:
                last_id = result[0]  # MAX(id)
                mssql_last_modified_date = result[1]  # MAX(modifieddate)

            # Convert MSSQL datetime to PostgreSQL-compatible format if it exists
                if mssql_last_modified_date:
                    psql_last_modified_date = mssql_last_modified_date.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    psql_last_modified_date = None
            else:
                last_id = None
                psql_last_modified_date = None

        # Close the cursor
            cursor.close()

        # Return the results as a tuple
            return last_id, psql_last_modified_date

        except Exception as e:
            logging.error(f"Error occurred:fn get_ssms_last_id {e}")
            return False

    def convert_timestamp_to_sql_datetime(self, ts):
        """
        Converts a pandas timestamp to a SQL-compatible datetime format.
        The function checks if the timestamp is null and returns None. Otherwise, it formats
        the timestamp to a string in the format 'YYYY-MM-DD HH:MM:SS.MMM' which is compatible
        with SQL datetime types.

        Args:
            ts (Timestamp or None): The pandas timestamp to convert.

        Returns:
            str or None: The formatted datetime string for SQL or None if the input is null.
        """
        if pd.isnull(ts):
            return None  # or 'NULL' if you are preparing a string for SQL queries
        return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def construct_merge_query(self, df, table_name, compare_column):
        """
        Constructs a SQL MERGE query for inserting or updating records in the target table.
        This function compares data from a pandas DataFrame with the target table using a specified
        comparison column and generates an appropriate SQL query.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data to merge.
            table_name (str): The name of the target table.
            compare_column (str): The column to compare for matching records in the merge.

        Returns:
            str: The SQL MERGE query as a string.
        """
        # if 'id' in df.columns:
        #     df = df.drop(columns=['id'])

        # Extract column names from the DataFrame to be used in the query
        columns = df.columns.tolist()

        # Prepare the column names for the query
        source_columns = ", ".join(columns)

        # Build the USING (VALUES...) section dynamically
        values_list = []
        for _, row in df.iterrows():
            values = []
            for col in columns:
                value = row[col]
                if pd.isnull(value):  # Handle NULL values
                    values.append("NULL")
                elif isinstance(value, str):  # Add quotes for string values
                    values.append(f"'{value}'")
                else:
                    values.append(str(value))  # For numeric or boolean values
            values_list.append(f"({', '.join(values)})")

        values_section = ",\n    ".join(values_list)

        # Build the UPDATE SET part by excluding the compare_column
        '''update_set_clause = ",\n        ".join(
            [
                f"target.{col} = source.{col}"
                for col in columns if col != compare_column or col != "Id" or col != "id"
            ]
        )
        print(update_set_clause)'''
        update_set_clause = ",\n        ".join(
        [
        f"target.{col} = source.{col}"
        for col in columns
        if col.lower() != "id" and col != compare_column  # Exclude "id" and compare_column
        ]
        )
        #print(update_set_clause)
        #return None
        # Remove 'id' column if it exists in the DataFrame
        if "id" in df.columns:
            psql_values = df["id"].tolist()
            df = df.drop(columns=["id"])
        columns2 = df.columns.tolist()
        # Build the INSERT part using all columns
        insert_columns = ", ".join(columns2)
        insert_values = ", ".join([f"source.{col}" for col in columns2])

        # Construct the full MERGE query
        query = f"""MERGE INTO {table_name} AS target
        USING (VALUES
            {values_section}
        ) AS source ({source_columns})
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
            OUTPUT inserted.id;
        """

        # Construct the final MERGE query
        # query = f"""
        # MERGE INTO {table_name} AS target
        # USING (VALUES
        #     {values_section}
        # ) AS source ({source_columns})
        # ON target.{compare_column} = source.{compare_column}  -- Unique column
        # WHEN MATCHED THEN
        #     UPDATE SET
        #         {update_set_clause}
        # WHEN NOT MATCHED THEN
        #     INSERT ({insert_columns})
        #     VALUES ({insert_values});
        # """
        return query,psql_values

    def update_psql_id_with_10_return_ids(self,postgres_conn,table20,mssql_ids,psql_values):
        logging.info("updating the postgress sql ids ")
        if len(mssql_ids) != len(psql_values):
            raise ValueError("The length of inserted_ids and id_values must be the same")
        case_statements = []
        logging.info("mssql inserted ids are",mssql_ids)
        logging.info("pssql ids are ",psql_values)
        for mssql_id, psql_value in zip(mssql_ids, psql_values):
            case_statements.append(f"WHEN id ='{psql_value}' THEN '{mssql_id}'")
        logging.info(f"case_statements are : {case_statements}")
        update_query = f"""
        UPDATE {table20}
        SET id = CASE
            {' '.join(case_statements)}
            ELSE id
            END
        WHERE id IN ({', '.join(f"'{val}'" for val in psql_values)});
        """
        #print(postgres_conn)
        logging.info("Generated Update Query:\n", update_query)
        # Execute the query
        try:
           with postgres_conn.cursor() as cursor:
            cursor.execute(update_query)  # Execute the query
            postgres_conn.commit()  # Commit the transaction
            logging.info("Query executed and changes committed successfully.")
        except Exception as e:
            postgres_conn.rollback()  # Rollback in case of error
            logging.error("Error occurred during query execution:", str(e))
            raise ValueError(f"Error in updating ids in postgresql with 1.0 ids in reverse sync")

    def insert_into_ssms(
        self, ssms_conn, df, insert_table_name, migration_track_col, time_columns,table_20,postgres_conn
    ):
        """
        Inserts records into a SQL Server Management Studio (SSMS) database in batches.
        The method constructs and executes a merge query for inserting or updating records
        and handles errors by retrying failed batches and logging failures.

        Args:
            ssms_conn: The connection object to the SSMS database.
            df (pandas.DataFrame): The DataFrame containing the data to be inserted.
            insert_table_name (str): The name of the target table in the database.
            migration_track_col (str): The column used for tracking migration in the merge query.
            time_columns (list): The columns containing timestamp information for tracking the last record.

        Returns:
            bool: Returns True if the insertion was successful, False if there were failures.
        """
        try:

            inserted_records = 0
            failed_records = 0
            insert_flag = True
            batch_size = 400
            failed_log = []
            last_id_time = None
            num_batches = math.ceil(len(df) / batch_size)
            logging.info(f"############# Records to insert: {df.shape[0]}")
            logging.info(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")
            logging.info(f"time columns are {time_columns}")

            def insert_block_operation():
                """
                Performs the insert operation in batches, constructs the merge queries,
                and handles errors such as failed records and retries.
                """
                nonlocal inserted_records, failed_records, last_id_time, insert_flag
                for batch_index in range(num_batches):
                    # Get the start and end indices for the current batch
                    start_index = batch_index * batch_size
                    end_index = min((batch_index + 1) * batch_size, len(df))
                    # Extract the current batch from the DataFrame
                    batch_df = df.iloc[start_index:end_index]

                    # print(f"batch_df is {batch_df.head()}")

                    # Construct the merge query for the batch
                    insert_query_update,psql_values = self.construct_merge_query(
                        batch_df, insert_table_name, migration_track_col
                    )
                    #print(f"insert_query_update {insert_query_update}")

                    if insert_query_update:
                        try:
                            # Execute the merge query and commit the transaction
                            cursor = ssms_conn.cursor()
                            # print(cursor)

                            # print(insert_query_update)

                            cursor.execute(insert_query_update)  # Execute the query
                            mssql_ids = cursor.fetchall()

                            #mssql_ids=[(80,)]

                            #mssql_ids=list(mssql_ids[0])
                            # print("these are before list",mssql_ids)
                            mssql_ids=[item[0] for item in mssql_ids]
                            results=[]
                            for last_id_ssms in mssql_ids:
                                result = self.convert_uuid_id(last_id_ssms)
                                results.append(result)
                            mssql_ids=results
                            # print(mssql_ids)
                            logging.info("the new id which is inserted in mssql",mssql_ids)
                            logging.info("the postgress sql ids are",psql_values)
                            # print(mssql_ids)
                            # print(table_20)
                            #print(postgres_conn)

                            ssms_conn.commit()  # Commit the transaction
                            logging.info("Records are inserted into mssql database successfully")

                            self.update_psql_id_with_10_return_ids(postgres_conn,table_20,mssql_ids,psql_values)
                            inserted_records += len(
                                batch_df
                            )  # Update the count of inserted records
                            last_row = batch_df.iloc[
                                -1
                            ]  # Get the last row in the batch

                            last_id_time = [
                                last_row[col]
                                for col in time_columns
                                if col in batch_df.columns
                            ]  # Capture time column values
                            return None
                        except Exception as e:
                            # If the batch insertion fails, rollback and log the failure
                            ssms_conn.rollback()
                            logging.error(
                                f"########### Failed to execute query for batch {batch_index}: {e}"
                            )
                            failed_records += len(
                                batch_df
                            )  # Increment the count of failed records
                            failed_log.append(
                                {
                                    "batch_index": batch_index,
                                    "query": insert_query_update,
                                }
                            )
                            # Retry the insertion by executing individual queries for each row
                            try:
                                print(
                                    f"############### Retrying with individual queries:"
                                )
                                for idx, row in batch_df.iterrows():
                                    # Construct the insert query for the individual row
                                    individual_insert_query = (
                                        self.construct_merge_query(
                                            pd.DataFrame([row]),
                                            insert_table_name,
                                            migration_track_col,
                                        )
                                    )
                                    cursor.execute(
                                        insert_query_update
                                    )  # Execute the query
                                    ssms_conn.commit()  # Commit the transaction
                                    inserted_records += 1
                                    last_id_time = [
                                        row[col]
                                        for col in time_columns
                                        if col in batch_df.columns
                                    ]
                                    # print(individual_insert_query)
                            except Exception as e:
                                # If individual queries fail, rollback and log the error
                                ssms_conn.rollback()
                                logging.error(
                                    f"Failed to execute query for batch {batch_index}: {e}"
                                )
                                failed_records += len(
                                    batch_df
                                )  # Increment the count of failed records
                                failed_log.append(
                                    {
                                        "batch_index": batch_index,
                                        "query": insert_query_update,
                                    }
                                )
                                insert_flag = False

                    else:
                        # If no insert query was generated, flag as failed
                        insert_flag = False
                        raise ValueError(f"Couldn't get insert query for the records")

            # Retry mechanism for handling failed insert block operations
            for attempt in range(3):
                try:
                    insert_block_operation()
                    break  # Break the retry loop if successful
                except Exception as e:
                    logging.error(
                        f"Error in whole insert block operation on attempt {attempt + 1}: {e}"
                    )
                    last_id_time = None
                    insert_flag = False
                    # with open('error_log.txt', 'a') as f:
                    # f.write(f"Error in whole insert block operation on attempt {attempt + 1}: {str(e)}\n")
                    logging.error(
                        f"Error in whole insert block operation on attempt {attempt + 1}: {str(e)}\n"
                    )

        except Exception as e:
            # Handle unexpected errors and log them
            insert_flag = False
            logging.error(
                f"Exception in inserting records into 1.0 for Reverse Sync: {e}"
            )
        return insert_flag

    def convert_uuid_id(self,id):
        if isinstance(id, uuid.UUID):
            return str(id)
        uuid_pattern = re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')
        if isinstance(id, str) and uuid_pattern.match(id):
            return str(id)  # Convert and return the UUID as a string
        return id

    def reverse_sync_migration(
        self, job_name, postgres_conn, reverse_sync_mappings, migration_details_dict
    ):

        """
        Handles the reverse synchronization migration for data transfer between the source and target databases.
        It retrieves data from the source database using the provided reverse sync mappings and inserts it into
        the target database.

        Args:
            job_name (str): The name of the migration job.
            postgres_conn (connection): The connection to the PostgreSQL target database.
            reverse_sync_mappings (list): A list of mappings that define how to sync data between databases.
            migration_details_dict (dict): Dictionary containing the migration job details.

        Returns:
            None
        """
        logging.info(f"############################# Starting Reverse sync")
        # Get the database configuration for the source database
        db_config = self.get_db_config(migration_details_dict)
        from_host = db_config["hostname"]
        from_port = db_config["port"]
        from_user = db_config["user"]
        from_pwd = db_config["password"]
        from_db_type = db_config["from_db_type"]
        from_database = migration_details_dict["from_database"]
        from_driver = os.getenv("MSSQL_DB_DRIVER")

        logging.info("Create a connection to the source database using the retrieved configuration")
        from_connection = self.create_connection(
            from_db_type,
            from_host,
            from_database,
            from_user,
            from_pwd,
            from_port,
            from_driver,
        )

        # Iterate through each mapping item in reverse_sync_mappings
        # print(f"From connection is {from_connection}")
        for dict_item in reverse_sync_mappings:
            from_20_query = dict_item["query"]
            table_10 = dict_item["table"]
            migration_track_col = dict_item["ref"]
            table_20=dict_item["table20"]
            logging.info("1.0 table is ",table_10,"2.0 table is ",table_20)

            # Check if the table name is valid and construct the full table name
            if not self.is_valid_table_name(table_10):
                full_from_table = f"[{from_database}].[dbo].[{table_10}]"
                logging.info(f"full table name {full_from_table}")
            else:
                full_from_table = table_10

            # Get the last ID from the source database
            logging.info(f"table 10 is {full_from_table}")
            last_id_ssms = self.get_ssms_last_id(from_connection, full_from_table)
            last_id_ssms=self.convert_uuid_id(last_id_ssms)
            logging.info(f"last id from 1.0 is {last_id_ssms} ")

            # Clean up and format the query for retrieving updated records
            from_query_20 = " ".join(from_20_query.split())
            from_query_20 = from_query_20.rstrip(";")

            # Check if the query involves 'modified_date', which means using a timestamp-based condition
            if "modified_date" in from_query_20.lower() and isinstance(last_id_ssms, int):
                logging.info(f"entered integer id part")
                last_migrated_time = migration_details_dict["last_migrated_time"]
                logging.info(
                    f"last_migrated time is {last_migrated_time} and type {type(last_migrated_time)}"
                )
                time_cols = ["id", "modified_date"]
                time_cols_ssms = ["id", "modifieddate"]
                primary_id_col = ["id"]
                last_id = [last_id_ssms, f"{last_migrated_time}"]
                logging.info(
                    f"after modifying time col and last_migrated time {time_cols},{last_id}"
                )
            elif "modified_date" in from_20_query.lower() and "created_date" in from_20_query.lower() and isinstance(last_id_ssms, str):
                 logging.info("this part for the uuid columns")
                 time_cols=["modified_date","created_date"]
                 time_cols_ssms=["id"]
                 last_migrated_time=migration_details_dict["last_migrated_time"]
                 last_id=[f"{last_migrated_time}",f"{last_migrated_time}"]
            else:
                # Default to using only the ID column for the condition
                time_cols = ["id"]
                time_cols_ssms = ["id"]
                last_id = [last_id_ssms]

            # Get the SQL query to fetch updated records from the source database
            pgsql_from_query = self.get_updated_records_query(
                time_cols, last_id, None, from_query_20
            )
            logging.info(f"after updating query {pgsql_from_query}")

            df_from_20 = self.execute_query(postgres_conn, pgsql_from_query)
            if df_from_20.empty:
                logging.info("DataFrame is empty; there are no updated records.")
                return None
            else:
                logging.info(f"df_from_20 is {df_from_20}")

            # Convert datetime columns to SQL-compatible format
            for col in df_from_20.columns:
                if pd.api.types.is_datetime64_any_dtype(df_from_20[col]):
                    df_from_20[col] = df_from_20[col].apply(
                        self.convert_timestamp_to_sql_datetime
                    )

            # Replace NaN values with None and convert boolean values to integers (1/0)
            df_from_20 = df_from_20.astype(object).mask(df_from_20.isna(), None)
            #df_from_20.replace({True: 1, False: 0}, inplace=True)
            df_from_20 = df_from_20.applymap(lambda x: 1 if x is True else (0 if x is False else x))
            logging.info(f"############# Result DF:::::{df_from_20}")
            df_from_20=self.modify_uuid_cols(df_from_20)

            # Insert the data into the target database (SSMS)

            self.insert_into_ssms(
                from_connection,
                df_from_20,
                full_from_table,
                migration_track_col,
                time_cols_ssms,
                table_20,postgres_conn
            )

##Calling the main method of class
# if __name__ == "__main__":
#     scheduler = MigrationScheduler()
#     scheduler.main_migration_func("automation_rule_altaworx")