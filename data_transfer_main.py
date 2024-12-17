from dotenv import load_dotenv
import math
import time
import psycopg2
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv
import os
import re
import pytds
import json
from common_utils.logging_utils import Logging

logging = Logging()


class DataTransfer:
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def create_connection_oldpyodbc(
        self,
        db_type="",
        host="",
        db_name="",
        username="",
        password="",
        port="",
        driver="",
        max_retry=3,
    ):
        connection = None
        retry = 1
        # print(f"db_type:{db_type}, host--{host}-db_name-{db_name}, username-{username},password-{password},port-{port},driver-{driver}")

        if db_type == "postgresql":
            try:
                logging.info(f"creating postgresql connection")

                connection = psycopg2.connect(
                    host=host,
                    database=db_name,
                    user=username,
                    password=password,
                    port=port,
                )
                logging.info("Connection to PostgreSQL DB successful")
            except Exception as e:
                logging.error(f"Failed to connect to PostgreSQL DB: {e}")
        elif db_type == "mssql":
            print(
                f"conn : {db_type},{host},{db_name},{username},{password},{driver},{port}"
            )
            print("Creating MSSQL connection")
            logging.info(f"Creating MSSQL connection")
            try:
                # connection_string= f"""DRIVER={driver};SERVER={host};DATABASE={db_name};UID={username};PWD={password};"""

                # connection = pyodbc.connect(connection_string)
                # connection = pymssql.connect(host=host,user=username,password=password,db=db_name,connect_timeout=5)
                logging.info("Connection to MSSQL successful!")
                print("Connection to MSSQL successful!")
            except Exception as e:
                logging.error(f"Failed to connect to MSSQL DB: {e}")
        return connection

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def create_connection(
        self,
        db_type="",
        host="",
        db_name="",
        username="",
        password="",
        port="",
        driver="",
        max_retry=3,
    ):
        connection = None

        if db_type == "postgresql":
            try:
                logging.info(f"Creating PostgreSQL connection")
                connection = psycopg2.connect(
                    host=host,
                    database=db_name,
                    user=username,
                    password=password,
                    port=port,
                )
                logging.info("Connection to PostgreSQL DB successful")
            except Exception as e:
                logging.error(f"Failed to connect to PostgreSQL DB: {e}")
        elif db_type == "mssql":
            print(
                f"conn : {db_type},{host},{db_name},{username},{password},{driver},{port}"
            )
            print("Creating MSSQL connection")
            logging.info(f"Creating MSSQL connection using pytds")
            try:
                connection = pytds.connect(
                    server=host,
                    database=db_name,
                    user=username,
                    password=password,
                    port=port,
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
                logging.info("Connection to MSSQL successful!")
            except Exception as e:
                logging.error(f"Failed to connect to MSSQL DB: {e}")

        return connection

    def execute_query(self, connection, query, params=None):

        try:
            # Check if params are provided
            if params:
                # Execute the query with parameters
                result_df = pd.read_sql_query(query, connection, params=params)
            else:
                # Execute the query without parameters
                result_df = pd.read_sql_query(query, connection)

            return result_df
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return None

    def is_valid_table_name(self, table_name):
        pattern = r"^\[\w+\]\.\[\w+\]\.\[\w+\]$"
        return re.match(pattern, table_name) is not None

    def map_cols(self, table_mapping, col_mapping, postgres_data):
        try:
            insert_data = {}

            for table_name, records in postgres_data.items():
                # Get target table names for table_name
                target_tables = table_mapping.get(table_name, [])

                for target_table in target_tables:
                    # Get column mappings for the target table
                    mapping = col_mapping.get(target_table, {})

                    temp_records = [
                        {
                            mapping[key]: value
                            for key, value in record.items()
                            if key in mapping
                        }
                        for record in records
                    ]

                    insert_data[target_table] = temp_records

            return insert_data

        except Exception as e:
            logging.error(f"Error while mapping columns: {e}")
            return {}

    def insert_data_to_db(
        self, table_name, data_list, mssql_conn, return_col, table_name_10
    ):
        print(f"insert data {table_name},{data_list}")
        try:
            if not mssql_conn:
                raise ValueError("Invalid MSSQL connection")

            if not data_list:
                raise ValueError("Data list is empty")

            cursor = mssql_conn.cursor()

            # Extract column names and placeholders
            columns = ", ".join(data_list[0].keys())
            # placeholders = ', '.join(['?'] * len(data_list[0])) #for pyodbc
            placeholders = ", ".join(
                ["%s"] * len(data_list[0])
            )  # Use %s placeholders for pytds

            # Construct the SQL query
            if return_col:
                sql_query = f"INSERT INTO {table_name} ({columns}) OUTPUT INSERTED.{return_col} VALUES ({placeholders})"
            else:
                sql_query = (
                    f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                )

            try:
                return_val = None
                return_fk_name = None

                # Convert rows to tuples and execute in a batch
                for row in data_list:
                    values = tuple(
                        # row[col] if isinstance(row[col], (str, int, float, bool)) else json.dumps(row[col])
                        (
                            row[col]
                            if row[col] is None
                            or isinstance(row[col], (str, int, float, bool))
                            else json.dumps(row[col])
                        )
                        for col in row.keys()
                    )
                    cursor.execute(sql_query, values)
                    if return_col:
                        return_val = cursor.fetchone()[0]
                        return_fk_name = f"{table_name_10}.{return_col}"

                mssql_conn.commit()
                logging.info("Insert Successful")

                if return_col:
                    return return_val, return_fk_name

            except Exception as e:
                logging.error(f"Failed to insert row into {table_name}: {e}")
                mssql_conn.rollback()
                return None

            finally:
                cursor.close()
        except Exception as e:
            logging.error(f"Error while inserting row into {table_name} : {e}")

    def update_table(self, conn, table_name, data_dict, condition_dict):
        """
        Update a PostgreSQL table using a dictionary.

        :param conn: psycopg2 connection object
        :param table_name: Name of the table to update
        :param data_dict: Dictionary containing column-value pairs to update
        :param condition_dict: Dictionary containing column-value pairs for the WHERE condition
        """
        try:
            # Replace NaN with None in data_dict
            data_dict = {
                k: (None if isinstance(v, float) and math.isnan(v) else v)
                for k, v in data_dict.items()
            }

            # Replace NaN with None in condition_dict
            condition_dict = {
                k: (None if isinstance(v, float) and math.isnan(v) else v)
                for k, v in condition_dict.items()
            }
            # Generate the SET part of the SQL query
            set_clause = ", ".join([f"{col} = %s" for col in data_dict.keys()])

            # Handle condition dict with lists or single values
            where_clause = []
            # print(f"in updat_table val dict is {data_dict}")
            values = list(data_dict.values())

            for col, val in condition_dict.items():
                if isinstance(val, list):
                    # Generate the WHERE clause for IN condition
                    placeholders = ", ".join(["%s"] * len(val))
                    where_clause.append(f"{col} IN ({placeholders})")
                    values.extend(val)  # Add the list values to the values list
                else:
                    # Handle single value condition
                    where_clause.append(f"{col} = %s")
                    values.append(val)

            # Combine WHERE conditions
            if where_clause:
                where_clause = " AND ".join(where_clause)
                sql_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            else:
                sql_query = f"UPDATE {table_name} SET {set_clause}"

            # Print the SQL query and values for debugging
            logging.info(f"SQL Query: {sql_query}")
            logging.info(f"Values: {values}")

            # Execute the query
            with conn.cursor() as cursor:
                cursor.execute(sql_query, values)
                conn.commit()

            logging.info("Update successful")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error while updating table {table_name}: {e}")

    def load_env_pgsql(self):
        load_dotenv()
        hostname = os.getenv("LOCAL_DB_HOST")
        port = os.getenv("LOCAL_DB_PORT")
        user = os.getenv("LOCAL_DB_USER")
        password = os.getenv("LOCAL_DB_PASSWORD")
        db_type = os.getenv("LOCAL_DB_TYPE")
        db_name = os.getenv("LOCAL_DB_NAME")
        return hostname, port, user, password, db_type, db_name

    def load_env_mssql(self):
        from_host = os.getenv("FROM_DB_HOST")
        from_port = os.getenv("FROM_DB_PORT")
        from_db = os.getenv("FROM_DB_NAME")
        from_user = os.getenv("FROM_DB_USER")
        from_pwd = os.getenv("FROM_DB_PASSWORD")
        from_db_type = os.getenv("FROM_DB_TYPE")
        from_driver = os.getenv("FROM_DB_DRIVER")
        return (
            from_host,
            from_port,
            from_db,
            from_user,
            from_pwd,
            from_db_type,
            from_driver,
        )

    def save_data_to_10(self, transfer_name, postgres_data):
        try:
            # establishing postgres connection
            start_time = time.time()
            if transfer_name == "sim_managment_inventory":
                pg = {}
                data_list = postgres_data["sim_management_inventory"]
                service_provider_id = data_list[0]["service_provider_id"]
                print(f"11111 {service_provider_id}")
                if service_provider_id != 6:
                    transfer_name = "sim_managment_inventory_m2m"
                else:
                    transfer_name = "sim_managment_inventory_mobility"
            print(f"STARTING {transfer_name}")

            load_dotenv()
            hostname, port, user, password, db_type, db_name = self.load_env_pgsql()
            mapping_table = os.getenv("MAPPING_TABLE")
            postgres_conn_start = time.time()
            postgres_conn = self.create_connection(
                db_type, hostname, db_name, user, password, port
            )
            logging.info(
                f"Postgres connection time: {time.time() - postgres_conn_start:.4f} seconds"
            )
            print(
                f"Postgres connection time: {time.time() - postgres_conn_start:.4f} seconds"
            )
            ##getting all the details for a particular transfer
            query_start = time.time()
            mapping_details_query = f"select table_mapping_10_to_20,col_mapping_10_to_20,return_params_10,fk_cols_10 from {mapping_table} where transfer_name ='{transfer_name}' order by id asc"
            mapping_details = self.execute_query(postgres_conn, mapping_details_query)
            print(f"Query execution time: {time.time() - query_start:.4f} seconds")
            logging.info(
                f"Query execution time: {time.time() - query_start:.4f} seconds"
            )

            if mapping_details.empty or mapping_details is None:
                raise ValueError(
                    "No Mappings present for transfer"
                )  # raising error if unable to get required data from db

            mapping_details = mapping_details.to_dict(orient="records")[0]
            tables_dict = mapping_details.get(
                "table_mapping_10_to_20", {}
            )  # 1.0 to 2.0 table mappings dict
            col_mappings = mapping_details.get(
                "col_mapping_10_to_20", {}
            )  # 1.0 to 2.0 column mappings
            return_params_10 = mapping_details.get(
                "return_params_10", {}
            )  # cols data that we need to return from 1.0
            fk_cols_10 = mapping_details.get("fk_cols_10", {})  # foreign key columns
            db_config = mapping_details.get("db_config", {})  # ssms db config details
            print(f"mapping_details {mapping_details}")

            # from_host=db_config['hostname']
            # from_port=db_config['port']
            # from_user=db_config['user']
            # from_pwd=db_config['password']
            # from_db_type=db_config['from_db_type']
            # from_driver=os.getenv('FROM_DB_DRIVER')
            # ssms_db_name=os.getenv('FROM_DB_NAME')

            # get the data to insert dict for ssms according to mappings
            mapping_start = time.time()
            total_insert_dict = self.map_cols(tables_dict, col_mappings, postgres_data)
            logging.info(
                f"Data mapping time: {time.time() - mapping_start:.4f} seconds"
            )

            # making mssql_connection

            (
                from_host,
                from_port,
                ssms_db_name,
                from_user,
                from_pwd,
                from_db_type,
                from_driver,
            ) = self.load_env_mssql()
            mssql_conn_start = time.time()
            mssql_conn = self.create_connection(
                from_db_type,
                from_host,
                ssms_db_name,
                from_user,
                from_pwd,
                from_port,
                from_driver,
            )
            print(f"mssql_conn {mssql_conn}")
            logging.info(
                f"MSSQL connection time: {time.time() - mssql_conn_start:.4f} seconds"
            )

            return_ids = []

            fk_track_dict = {}

            insert_start = time.time()
            for table_name_10, data_list in total_insert_dict.items():
                # if the table name in not mssql format -- modifying it
                if not self.is_valid_table_name(table_name_10):
                    full_from_table = f"[{ssms_db_name}].[dbo].[{table_name_10}]"

                if return_params_10:
                    print(f"Heree 2111 {return_params_10}")
                    return_col = return_params_10.get(
                        table_name_10, None
                    )  # get the col whose value should be returned for this table
                    fk_col_dict = fk_cols_10.get(
                        table_name_10, {}
                    )  # get the fk_col dict for this table
                else:
                    return_col = None
                    fk_col_dict = None

                # Condition 1: for Single Main table
                if not return_col:
                    # Condition: where the table does not return any fk (single tables which does not have any dependent tables)
                    print(f"#### Single Table: {table_name_10}")
                    return_id = self.insert_data_to_db(
                        full_from_table,
                        total_insert_dict[table_name_10],
                        mssql_conn,
                        None,
                        table_name_10,
                    )

                # condition1: for main tables--these only return an fk
                elif return_col and table_name_10 not in fk_cols_10:
                    print(f"##### Main Table: {table_name_10}")
                    logging.info(f"##### Main Table: {table_name_10}")
                    return_id, return_fk_col = self.insert_data_to_db(
                        full_from_table,
                        total_insert_dict[table_name_10],
                        mssql_conn,
                        return_col,
                        table_name_10,
                    )
                    return_ids.append(
                        return_id
                    )  # adding the return col value to main return list
                    fk_track_dict[return_fk_col] = (
                        return_id  # adding the return fk_col and return fk col value to an fk_tract_dict
                    )

                # condition2: Tables which have an fk column and also return a col as fk
                elif return_col and table_name_10 in fk_cols_10:
                    print(f"###### {table_name_10} Table has FK and returns col as FK")
                    logging.info(
                        f"###### {table_name_10} Table has FK and returns col as FK"
                    )
                    for (
                        key,
                        val,
                    ) in (
                        fk_track_dict.items()
                    ):  # if any fkey present in fk_track_dict present in fk_cols_dict from db then modify cols
                        modify_col = fk_col_dict.get(
                            key, None
                        )  # get the name of the col that we need to modify(fk_col_name)
                        if modify_col:
                            # print(f"modify col got is {modify_col} new val is {val}")
                            for row_item in data_list:
                                row_item[modify_col] = (
                                    val  # updating the fk_col value in data dict which is to be inserted in db
                                )

                    insert_id, fk_col = self.insert_data_to_db(
                        full_from_table,
                        data_list,
                        mssql_conn,
                        return_col,
                        table_name_10,
                    )
                    fk_track_dict[fk_col] = insert_id

                # condition3: where tables don't return any fk but contain fk col that needs to be updated
                elif (
                    table_name_10 in fk_cols_10
                    and table_name_10 not in return_params_10
                ):
                    print(f"###### {table_name_10} Table has FK")
                    logging.info(f"###### {table_name_10} Table has FK")
                    for key, val in fk_track_dict.items():
                        modify_col = fk_col_dict.get(key, None)
                        if modify_col:
                            for row_item in data_list:
                                row_item[modify_col] = val

                    insert = self.insert_data_to_db(
                        full_from_table,
                        data_list,
                        mssql_conn,
                        return_col,
                        table_name_10,
                    )
            logging.info(
                f"Data insertion time: {time.time() - insert_start:.4f} seconds"
            )

            if transfer_name == "bulk_change":
                total_time = time.time() - start_time
                logging.info(f"Total execution time: {total_time:.4f} seconds")
                return return_ids[0] if return_ids else None

        except Exception as e:
            logging.error(f"Error while saving 2.0 data in 1.0 : {e}")

    def get_10_data(
        self,
        transfer_name,
        mssql_conn,
        id_10,
        details_list,
        main_flag=None,
        main_result_dict=None,
    ):
        if main_flag:
            main_result_dict = {}
            for dict_item in details_list:
                where_col = dict_item["where_col"]
                table_10_key = dict_item["table_10"]
                select_query = dict_item["select_query"]
                select_query_p = select_query.replace("?", "%s")  # replacing of pymssql

                if "?" in select_query.lower() and where_col == "id_10":
                    params = (id_10,)
                    # Retry mechanism
                    for _ in range(10):  # Adjust the number of retries as needed
                        result_df = self.execute_query(
                            mssql_conn, select_query_p, params=params
                        )
                        if transfer_name == "bulk_change":
                            try:
                                status = result_df["status"][0]
                            except KeyError:
                                status = result_df["Status"][0]

                            if status == "NEW" or status == "PROCESSING":
                                print(
                                    "Status is 'NEW' OR 'PROCESSING', retrying the query..."
                                )
                                time.sleep(30)  # Wait before retrying
                                continue  # Retry the query
                            else:
                                break

                        if result_df is not None and not result_df.empty:
                            break  # Exit the loop if a valid result is obtained
                        else:
                            print("Query returned no results, retrying in 5 seconds...")
                            time.sleep(30)  # Wait for 10 seconds before retrying

                    # Check if a valid result was obtained after retries
                    if result_df is not None and not result_df.empty:
                        result_dict = result_df.to_dict(orient="records")
                        main_result_dict[table_10_key] = result_dict
                    else:
                        logging.error(f"Query failed after retries for {table_10_key}")

            return main_result_dict
        else:
            dependent_dict = {}
            where_val_list = []
            for dict_item in details_list:
                table_10_key = dict_item["table_10"]
                select_query = dict_item["select_query"]
                where_col = dict_item["where_col"]
                where_table_name, where_col_name = where_col.split(".")
                # where_table_name=where_split[0]
                # where_col_name=where_split[1]

                where_vals = [
                    item[where_col_name]
                    for item in main_result_dict.get(where_table_name, [])
                ]
                where_val_list.extend(where_vals)

                if where_vals:
                    where_tuple = tuple(where_vals)
                    # print(f"where tuple {where_tuple}")
                    if len(where_tuple) == 1:
                        where_tuple_str = str(where_tuple[0])
                        # print(f"where tuple is 1 value {where_tuple_str} type is {type(where_tuple_str)} `('{where_tuple_str}')`")
                        select_query_p = select_query.replace(
                            "?", f"('{where_tuple_str}')"
                        )
                    else:
                        # Parameterize query to avoid SQL injection
                        select_query_p = select_query.replace("?", str(where_tuple))
                    # Parameterize query to avoid SQL injection
                    # select_query_p = select_query.replace('?', str(where_tuple))
                    result_df = self.execute_query(mssql_conn, select_query_p)

                    if result_df is not None:
                        result_dict = result_df.to_dict(orient="records")
                        dependent_dict[table_10_key] = result_dict
                    else:
                        dependent_dict[table_10_key] = []

                else:
                    print(f"No values found for where clause: {where_col}")

            return dependent_dict, where_val_list

    def save_data_20_from_10(self, id_10, id_20, transfer_name):
        try:
            load_dotenv()
            hostname, port, user, password, db_type, db_name = self.load_env_pgsql()
            postgres_conn1 = self.create_connection(
                db_type, hostname, db_name, user, password, port
            )
            mapping_table = os.getenv("MAPPING_TABLE")
            # getting details from db
            mapping_details_query = f"select db_name_20,db_config,reverse_table_mapping,reverse_col_mapping,data_from_10,update_cond_cols from {mapping_table} where transfer_name ='{transfer_name}' order by id asc"
            mapping_details = self.execute_query(postgres_conn1, mapping_details_query)

            if mapping_details.empty or mapping_details is None:
                raise ValueError(
                    "No Mappings present for transfer"
                )  # raising error if unable to get required data from db

            mapping_details = mapping_details.to_dict(orient="records")[0]
            table_info_dict = mapping_details["data_from_10"]
            update_cond_20 = mapping_details["update_cond_cols"]

            (
                from_host,
                from_port,
                from_db,
                from_user,
                from_pwd,
                from_db_type,
                from_driver,
            ) = self.load_env_mssql()
            mssql_conn = self.create_connection(
                from_db_type,
                from_host,
                from_db,
                from_user,
                from_pwd,
                from_port,
                from_driver,
            )

            main_details_list = table_info_dict[
                "main"
            ]  # get info list from db which helps in getting data from 10

            main_result_dict = self.get_10_data(
                transfer_name, mssql_conn, id_10, main_details_list, main_flag=True
            )
            logging.info(
                f"Data dict obtained from 1.0 for main tables: \n {main_result_dict}"
            )

            # print(f"Main dict {main_result_dict}")

            dependent_info_list = table_info_dict[
                "dependent"
            ]  # get info list from db which helps in getting data from 10
            dependent_dict, where_val_list = self.get_10_data(
                transfer_name,
                mssql_conn,
                id_10,
                dependent_info_list,
                main_flag=False,
                main_result_dict=main_result_dict,
            )
            logging.info(
                f"Data dict obtained from 1.0 for dependent tables: \n {dependent_dict} \n {where_val_list}"
            )
            # print(f"Depedent data {dependent_dict} \n {where_val_list}")

            reverse_tables = mapping_details["reverse_table_mapping"]
            reverse_col_mappings = mapping_details["reverse_col_mapping"]

            main_insert_data = self.map_cols(
                reverse_tables, reverse_col_mappings, main_result_dict
            )
            db_name_20 = mapping_details["db_name_20"]
            # print(f"############# Main insert_data {main_insert_data}")

            postgres_conn = self.create_connection(
                db_type, hostname, db_name_20, user, password, port
            )

            main_cond_dict = update_cond_20[
                "main"
            ]  # get the dict from db whic contains condition params
            main_multiple_col_conditions = update_cond_20["main_multi_col"]
            dependent_cond_dict = update_cond_20["dependent"]

            for table_name_20, data_list in main_insert_data.items():
                logging.info(
                    f"Data to be updated in table {table_name_20} is {data_list}"
                )
                if data_list:
                    if table_name_20 in main_cond_dict.keys():
                        where_20_col = main_cond_dict[table_name_20]
                        for record_dict in data_list:
                            print(f"record_dict is {record_dict}")
                            update = self.update_table(
                                postgres_conn,
                                table_name_20,
                                record_dict,
                                {where_20_col: id_20},
                            )

                    elif table_name_20 in main_multiple_col_conditions.keys():
                        # logging.info(f"in elif condiion table_name {table_name_20}")
                        where_20_col = main_multiple_col_conditions[table_name_20]
                        split_where_cols = where_20_col.split(",")
                        where_col_0 = split_where_cols[0]
                        where_col_rest = split_where_cols[1:]

                        for record_dict in data_list:
                            # Build the where_dict using a dictionary comprehension
                            where_dict = {where_col_0: id_20}
                            where_dict.update(
                                {
                                    col: record_dict[col]
                                    for col in where_col_rest
                                    if col in record_dict
                                }
                            )
                            # print(f"where cond dict {where_dict}")
                            # print(f"record_dict passed is {record_dict}")
                            # Execute the update
                            update = self.update_table(
                                postgres_conn, table_name_20, record_dict, where_dict
                            )

                else:
                    raise ValueError("No records obtained to insert in 2.0")

            dependent_insert_data = self.map_cols(
                reverse_tables, reverse_col_mappings, dependent_dict
            )
            print(f"Dependent_dict is {dependent_insert_data}")

            for table_name_20, data_list in dependent_insert_data.items():
                if not data_list:
                    raise ValueError("No Data to Insert")

                where_20_col = dependent_cond_dict.get(table_name_20)
                if not where_20_col:
                    raise ValueError("Where condition column not found")

                for record_dict in data_list:
                    where_val = record_dict.get(where_20_col)
                    if where_val is not None:
                        # Prepare the update where conditions
                        where_dict = {where_20_col: where_val}
                        # Execute the update
                        self.update_table(
                            postgres_conn, table_name_20, record_dict, where_dict
                        )

            return True

        except Exception as e:

            logging.error(
                f"Error while fetching data from 1.0 and saving it int 2.0 : {e}"
            )
            return False

    def insert_automation_rule_table_data(self, transfer_name, data_dict):
        load_dotenv()
        hostname, port, user, password, db_type, db_name = self.load_env_pgsql()
        logging.info(f"DB Conn details {hostname,port,user,password,db_type,db_name}")
        postgres_conn1 = self.create_connection(
            db_type, hostname, db_name, user, password, port
        )
        mapping_table = os.getenv("MAPPING_TABLE")

        mapping_details_query = f"select table_mapping_10_to_20,col_mapping_10_to_20,return_params_10,fk_cols_10 from {mapping_table} where transfer_name ='{transfer_name}' order by id asc"
        # print(f"query {mapping_details_query}")
        mapping_details = self.execute_query(postgres_conn1, mapping_details_query)
        # print(f"mapping_details {mapping_details}")
        mapping_details = mapping_details.to_dict(orient="records")[0]

        tables_dict = mapping_details.get("table_mapping_10_to_20", {})
        # print(f"Tables dict is {tables_dict}")  # 1.0 to 2.0 table mappings dict
        col_mappings = mapping_details.get("col_mapping_10_to_20", {})
        # print(f"col_mappings are {col_mappings}")   # 1.0 to 2.0 column mappings
        return_params_10 = mapping_details.get("return_params_10", {})
        # print(f"return params {return_params_10}")   # cols data that we need to return from 1.0
        fk_cols_10 = mapping_details.get("fk_cols_10", {})
        # print(f"fk cols 10 are {fk_cols_10}")            # foreign key columns
        db_config = mapping_details.get("db_config", {})

        (
            from_host,
            from_port,
            ssms_db_name,
            from_user,
            from_pwd,
            from_db_type,
            from_driver,
        ) = self.load_env_mssql()
        mssql_conn = self.create_connection(
            from_db_type,
            from_host,
            ssms_db_name,
            from_user,
            from_pwd,
            from_port,
            from_driver,
        )

        fk_values_dict = {}
        data_dict_total = data_dict["automation_data"]

        for key, value in data_dict_total.items():
            if key in tables_dict.keys():
                table_name_10 = tables_dict.get(key)
                for table_name in table_name_10:
                    # print(f"debug {return_params_10}")
                    return_fk_col = return_params_10.get(table_name)
                    main_pgsql_dict = {key: [value]}
                    # print(f"main {main_pgsql_dict}")
                    data_10_mapping_dict = self.map_cols(
                        tables_dict, col_mappings, main_pgsql_dict
                    )
                    # print(f"data_10_mapping_dict {data_10_mapping_dict}")
                    for table_name_100, value_list in data_10_mapping_dict.items():
                        return_fk_col_val, fk_col_name = self.insert_data_to_db(
                            table_name_100,
                            value_list,
                            mssql_conn,
                            return_fk_col,
                            table_name_100,
                        )
                        fk_values_dict[fk_col_name] = return_fk_col_val
                # print(f"fk_values_dict {fk_values_dict}")
            else:
                if isinstance(value, list):
                    for item in value:
                        print(f"item {item}")

                        data_dict_item = self.ensure_values_are_lists(item)
                        data_mapping_dict = self.map_cols(
                            tables_dict, col_mappings, data_dict_item
                        )
                        # print(f"data mapping dict {data_mapping_dict}")
                        for table_name_10, data_list in data_mapping_dict.items():
                            # print(f"table and lists are {table_name_10},{data_list}")
                            if not data_list or data_list == [{}]:
                                # print(f"should skip {table_name_10}")
                                continue
                            fk_col_ = return_params_10.get(table_name_10)
                            if fk_col_:
                                # print('ifffff',table_name_10)
                                # print(f"daata {data_list}")
                                # print(f"fk cols is {fk_col_}")
                                automation_rule_followup_id, fk_col = (
                                    self.insert_data_to_db(
                                        table_name_10,
                                        data_list,
                                        mssql_conn,
                                        fk_col_,
                                        table_name_10,
                                    )
                                )
                                # print(f"id :{automation_rule_followup_id}, fk_col: {fk_col}")
                                fk_values_dict[fk_col] = automation_rule_followup_id
                            else:
                                fk_col_ = None
                                # print(f"111111111111111: In else:::table name {table_name_10}")
                                if table_name_10 in fk_cols_10.keys():
                                    # print(f"fks for this table {fk_cols_10[table_name_10]}")
                                    fk_cols_tables = fk_cols_10[table_name_10]
                                    reversed_fk_dict = {
                                        value: key
                                        for key, value in fk_cols_tables.items()
                                    }
                                    fk_col_names = [
                                        value
                                        for value in fk_cols_10[table_name_10].values()
                                    ]
                                    # print('############',fk_col_names)
                                    # print('@@@@@@@@@@@@@@@',fk_values_dict)
                                    # print(f"data list before {data_list}")
                                    for item in data_list:
                                        for key_col, data_val in item.items():
                                            # print(f"key {key_col},{data_val}")
                                            if key_col in fk_col_names:
                                                modified_fk_value = fk_values_dict.get(
                                                    reversed_fk_dict.get(key_col, None),
                                                    None,
                                                )
                                                # if modified_fk_value:
                                                item[key_col] = modified_fk_value
                                    # print(f"table name {table_name_10}, data {data_list}")

                                    insert = self.insert_data_to_db(
                                        table_name_10,
                                        data_list,
                                        mssql_conn,
                                        fk_col_,
                                        table_name_10,
                                    )


# if __name__ == "__main__":
#     scheduler = DataTransfer()
#     postgres_data={"sim_management_bulk_change":[{"service_provider_id":1,"tenant_id":1,"device_status_id":1,"status":"NEW","change_request_type_id":1,"processed_by":"vyshnavi","created_by":"vtest"}],"sim_management_bulk_change_request":[{"bulk_change_id":20,"iccid":56789098764,"status":"NEW","change_request":"{\"UpdateStatus\":\"Activated\",\"IsIgnoreCurrentStatus\":false,\"PostUpdateStatusId\":4,\"Request\":{},\"IntegrationAuthenticationId\":0}"},{"bulk_change_id":20,"iccid":1223454556,"status":"NEW","change_request":"{\"UpdateStatus\":\"Activated\",\"IsIgnoreCurrentStatus\":false,\"PostUpdateStatusId\":4,\"Request\":{},\"IntegrationAuthenticationId\":0}"}]}

#     transfer_name='bulk_change'
#     # bc_id=scheduler.save_data_10(transfer_name,postgres_data)
#     # print(f"bd _id is {bc_id}")

#     bulk_change_id=2700 #data present in 10
#     id_20=7560

#     scheduler.save_data_20_from_10(bulk_change_id,id_20,transfer_name)
