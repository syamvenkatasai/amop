"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
@Author3 : Vyshnavi
Created Date: 01-11-24
"""

import psycopg2
from psycopg2 import sql
import re
from dotenv import load_dotenv
import os
from common_utils.logging_utils import Logging
from common_utils.db_utils import DB
from common_utils.email_trigger import send_email
import time
import pandas as pd
import json
from pytz import timezone
from common_utils.permission_manager import PermissionManager


logging = Logging(name="tenant_onboarding")


db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}


def validate_tenant_name(tenant_name):
    """
    Validates and formats the tenant name to conform to PostgreSQL database naming conventions.
    """
    # Convert to lowercase
    tenant_name = tenant_name.lower()

    # Replace invalid characters (anything that's not a letter, number, or underscore) with underscores
    tenant_name = re.sub(r"[^a-z0-9_]", "_", tenant_name)

    # Ensure the name doesn't start with a digit, add a prefix if needed
    if tenant_name[0].isdigit():
        tenant_name = "tenant_" + tenant_name

    # Ensure the name is <= 63 characters, which is the limit for PostgreSQL database names
    tenant_name = tenant_name[:63]

    return tenant_name


def test(data):
    common_utils_database = DB("common_utils", **db_config)
    data = common_utils_database.get_data("tenant", {}, ["tenant_name"]).to_list()
    response = {"flag": True, "data": data}
    return response


def create_tenant_database(conn, db_name):
    """
    Creates a PostgreSQL database with the given name.

    Args:
    - db_name: The name of the database to create.
    - conn: The psycopg2 connection object (connected to a valid database).

    Returns:
    - (db_name, True) if the database is created successfully.
    - (db_name, False) if the database already exists.
    """
    if not isinstance(conn, psycopg2.extensions.connection):
        raise ValueError(
            "The 'conn' parameter must be a valid psycopg2 connection object."
        )

    new_conn = psycopg2.connect(
        dbname="postgres",
        user=conn.info.user,
        password=conn.info.password,
        host=conn.info.host,
        port=conn.info.port,
    )

    # Enable autocommit so the CREATE DATABASE statement is executed immediately
    new_conn.autocommit = True
    cur = new_conn.cursor()

    try:
        # SQL query to create a new database
        create_db_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))

        # Execute the CREATE DATABASE query
        cur.execute(create_db_query)
        # conn.commit()
        logging.debug(f"Database '{db_name}' created successfully.")
        return db_name, True
    except psycopg2.errors.DuplicateDatabase:
        logging.warning(f"Database '{db_name}' already exists.")
        return db_name, False
    except Exception as e:
        logging.error(f"Error in creating tenant db: {e}")
        return db_name, False
    finally:
        # Close the cursor
        cur.close()
        new_conn.close()


def execute_script_by_query(conn, script_file_path):
    """
    Executes SQL statements from create_scripts.sql to create tables in the specified database.
    """
    # Read the SQL statements from the file
    # with open('create_table.sql', 'r') as file:
    with open(script_file_path, "r") as file:
        sql_commands = file.read()

    # Connect to the newly created database
    # conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    failed_tables = []

    try:
        # Split the SQL commands by semicolon and execute them one by one
        for command in sql_commands.split(";"):
            command = command.strip()  # Remove any extra whitespace
            if command:  # Ensure the command is not empty
                command = command + ";"
                try:
                    cur.execute(command)
                    conn.commit()
                    logging.info(f"Executed: ")  # {command}")
                except Exception as e:
                    failed_tables.append(command)  # Log failed command
                    conn.rollback()
                    logging.error(f"Failed to execute: {command}. Error: {e}")

        if failed_tables:
            return False, failed_tables  # Return False and the failed table commands
        return True, []  # Return True if all commands succeeded
    finally:
        # Close the cursor and connection
        cur.close()
        # conn.close()


def execute_script_single(conn, script_file_path):
    """
    Executes the SQL statements from the provided .sql file as a single transaction.

    Args:
    - conn: A psycopg2 connection object connected to the target database.
    - script_file_path: The path to the SQL script file.

    Returns:
    - True if the script is executed successfully, False otherwise.
    """
    try:
        # Read the SQL statements from the file
        with open(script_file_path, "r") as file:
            sql_commands = file.read()

        # Create a cursor
        cur = conn.cursor()

        try:
            # Execute the entire SQL script at once
            cur.execute(sql_commands)
            conn.commit()  # Commit the transaction
            logging.info("SQL script executed successfully.")
            return True
        except Exception as e:
            conn.rollback()  # Rollback the transaction on error
            logging.error(f"Failed to execute the SQL script. Error: {e}")
            return False
        finally:
            # Close the cursor
            cur.close()
    finally:
        # Close the connection
        conn.close()


def check_database_exists(postgres_conn, db_name):
    """
    Check if a database exists in PostgreSQL.

    Args:
    - postgres_conn: A psycopg2 connection object connected to the server.
    - db_name: Name of the database to check for existence.

    Returns:
    - True if the database exists, False otherwise.
    """
    try:
        # Create a cursor from the connection
        cursor = postgres_conn.cursor()

        # Execute a query to check if the database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        # Fetch one record (if any)
        result = cursor.fetchone()
        # Close the cursor
        cursor.close()
        # If result is found, the database exists
        return result is not None
    except Exception as e:
        logging.error(f"Error checking database existence: {e}")
        return False


def Creation_Superadmin_Acess(data, tenant_db_name):
    partner_name = data.get("partner_name")
    sub_tenant_name = data.get("sub_partner_name")
    tenant_database = data.get("db_name", "")
    insert_tenant_dict = {}
    # Assign tenant_name based on the presence of values in partner_name and sub_partner_name
    if partner_name and sub_tenant_name:
        tenant_name = sub_tenant_name
        parent_tenant_id = data.get("parent_tenant_id")
        insert_tenant_dict["parent_tenant_id"] = int(parent_tenant_id)
    else:
        tenant_name = partner_name
    database = DB("common_utils", **db_config)
    email_ids = data.get("email_ids", "")
    username = data.get("username", "superadmin")
    logo = data.get("logo", "")
    all_roles = database.get_data("master_roles", {}, ["role_name"])[
        "role_name"
    ].to_list()

    query_for_modules='''
    SELECT DISTINCT parent_module_name, id
    FROM public.module
    ORDER BY order_by ASC;
    '''
    unique_modules=database.execute_query(query_for_modules,True)['parent_module_name'].to_list()
    # Assuming all_modules is your list of module names
    all_modules = []
    seen = set()

    for module in unique_modules:
        if module not in seen:
            all_modules.append(module)
            seen.add(module)
    all_carrier_apis = database.get_data("master_carrier_apis").to_dict(
        orient="records"
    )
    all_amop_apis = database.get_data("master_amop_apis").to_dict(orient="records")


    insert_tenant_dict["time_zone"] = "America/Chicago"
    insert_tenant_dict["tenant_name"] = tenant_name
    insert_tenant_dict["email_ids"] = email_ids
    insert_tenant_dict["logo"] = logo
    insert_tenant_dict["is_active"] = True
    insert_tenant_dict["db_name"] = tenant_db_name
    tenant_id = database.insert_data(insert_tenant_dict, "tenant")
    try:
        common_utils_database = DB("common_utils", **db_config)
        email_ids = data.get("email_id")
        to_emails = ", ".join(email_ids)
        # Update session data
        session_data = {"to_mail": to_emails}

        common_utils_database.update_dict(
            "email_templates", session_data, {"template_name": "Partner creation"}
        )
        result = send_email("Partner creation", id=tenant_id , tenant_database=tenant_database  )
    except:
        pass
    logging.info(f" tenant_id is: {tenant_id} ")

    insert_dict_tenant_module = []
    logging.info(f" ####### all_modules is {all_modules} ")
    # List of modules that need to be active
    active_modules = ["Super admin"]

    # Loop through all modules and set is_active accordingly
    for module in all_modules:
        temp = {
            "module_name": module,
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "created_by": username,
            "modified_by": username,
            "is_active": module
            in active_modules,  # Set is_active to True for specified modules
        }
        insert_dict_tenant_module.append(temp)
    logging.info(f" ####### insert_dict_tenant_module is {insert_dict_tenant_module} ")
    dummy_id = database.insert_data(insert_dict_tenant_module, "tenant_module")

    insert_dict_roles = []
    logging.info(f" ####### all_roles is {all_roles} ")
    for role in all_roles:
        temp = {}
        temp["role_name"] = role
        temp["tenant_id"] = tenant_id
        temp["created_by"] = username
        temp["modified_by"] = username
        temp["bulk_change_type_id_list"] = []
        temp["is_active"] = False
        insert_dict_roles.append(temp)
    logging.info(f" ####### insert_dict_roles is {insert_dict_roles} ")
    data_id = database.insert_data(insert_dict_roles, "roles")

    insert_dict_amop_apis = []
    logging.info(f" ####### all_amop_apis is {all_amop_apis} ")
    for api_data in all_amop_apis:
        if "id" in api_data:
            api_data.pop("id")
        temp = api_data
        temp["partner"] = tenant_name
        temp["api_state"] = True
        temp["last_modified_by"] = username
        insert_dict_amop_apis.append(temp)
    logging.info(f" ####### insert_dict_amop_apis is {insert_dict_amop_apis} ")
    database_id = database.insert_data(insert_dict_amop_apis, "amop_apis")
    insert_dict_carier_apis = []
    logging.info(f" ####### all_carrier_apis is {all_carrier_apis} ")
    for api_data in all_carrier_apis:
        if "id" in api_data:
            api_data.pop("id")
        temp = api_data
        temp["partner"] = tenant_name
        temp["api_state"] = True
        insert_dict_carier_apis.append(temp)
    logging.info(f" ####### insert_dict_carier_apis is {insert_dict_carier_apis} ")
    # check_id=database.insert_data(insert_dict_carier_apis,'carrier_apis')
    return True


db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}
def create_tenant_db(data):
    """
    This function creates a tenant database for a new partner. It validates the tenant name, checks if the database
    already exists, and if not, creates the database, its tables, views, and functions. If everything is successful,
    it returns a success flag and the tenant's database name.
    """
    partner_name = data.get("partner_name")
    sub_tenant_name = data.get("sub_partner_name")

    # Assign tenant_name based on the presence of values in partner_name and sub_partner_name
    if partner_name and sub_tenant_name:
        tenant_name = sub_tenant_name
    else:
        tenant_name = partner_name
    #tenant_name = data.get("partner_name")
    if not tenant_name:
        logging.error(f"Error in creating tenant db: {e}")
        return False, tenant_name

    try:
        logging.info(f"Starting the tenant creation with {tenant_name}")
        validated_name = validate_tenant_name(tenant_name)
        logging.debug(f"validated name is {validated_name}")
        load_dotenv()
        hostname = os.getenv("HOST")  # Provide a default value for fallback
        port = int(os.getenv("PORT"))  # Convert to integer and provide a default
        user = os.getenv("USER")
        password = os.getenv("PASSWORD")
        postgres_conn = psycopg2.connect(
            dbname="postgres", user=user, password=password, host=hostname, port=port
        )
        check_db = check_database_exists(postgres_conn, validated_name)
        logging.debug(f"Check Db returned: {check_db}")
        if check_db is False:
            try:
                create_db, db_flag = create_tenant_database(
                    postgres_conn, validated_name
                )

            except Exception as e:
                logging.error(f"Error in creating tenant db: {e}")
                return False, validated_name

            if db_flag is True:
                create_tables_file_path = "tables_script.sql"
                try:
                    logging.info(f"Executing create tables script")
                    postgres_conn2 = psycopg2.connect(
                        dbname=validated_name,
                        user=user,
                        password=password,
                        host=hostname,
                        port=port,
                    )
                    logging.info(f"connection established {postgres_conn2}")
                    create_tables = execute_script_by_query(
                        postgres_conn2, create_tables_file_path
                    )
                    if create_tables:
                        create_views_file_path = "view_script.sql"
                        logging.info(f"Going to create views")
                        try:
                            create_views = execute_script_by_query(
                                postgres_conn2, create_views_file_path
                            )
                        except Exception as e:
                            logging.error(f"Error in creating views: {e}")
                            raise ValueError("Error in creating views")
                        try:
                            logging.info(f"going to create functions triggers ")
                            create_fn_file_path = "functions.sql"
                            create_fn = execute_script_single(
                                postgres_conn2, create_fn_file_path
                            )
                        except Exception as e:
                            logging.error(f"Error in creating functions: {e}")
                            raise ValueError(f"Error in creating functions {e}")
                    else:
                        raise ValueError("Something went wrong while Creating Tables")
                except Exception as e:
                    logging.error(f"Error in creating tables: {e}")
            else:
                db_flag = False

        else:
            logging.info(f"DB already exists check once")
            db_flag, create_tables, create_views, create_fn = False, False, False, False
        if db_flag is True:
            try:
                if create_tables and create_views and create_fn_file_path:
                    logging.info(f"Everything is successfully created:")
                    return_flag = True
                    tenant_db_name = validated_name
                else:
                    raise ValueError(
                        "DB was created but some issues occurred during the table/view/function creation process."
                    )
            except ValueError as ve:
                logging.error(f"Caught ValueError in DB creation process: {ve}")
                return_flag = False
                tenant_db_name = validated_name
        else:
            logging.error(f"Error creating db, check logs")
            return_flag = False
            tenant_db_name = validated_name
    except Exception as e:
        logging.error(f"Error in creating tenant: {e}")

    # creation of acess to superadmin module
    Creation_Superadmin_Acess(data, tenant_db_name)

    return {"flag": return_flag, "tenant_db_name": tenant_db_name}


def create_provider_commands(server_name, target):  # partner becoming provider funtion
    """
    This function handles the process of making a partner tenant into a provider for a target tenant. It retrieves
    the partner and target tenant database names, creates the necessary foreign data wrapper (FDW) commands, and
    executes them to set up the provider connection in the target database.
    Returns a success flag and the created schema name.
    """
    # logging.info(f"server_name is {server_name} \n target_db is {target}")
    load_dotenv()
    host = os.getenv("LOCAL_DB_HOST")
    port = os.getenv("LOCAL_DB_PORT")
    user = os.getenv("LOCAL_DB_USER")
    password = os.getenv("LOCAL_DB_PASSWORD")
    # postgres_conn=psycopg2.connect(dbname=target, user=user, password=password, host=host, port=port)
    commands = []
    create_fdw = f"CREATE EXTENSION IF NOT EXISTS postgres_fdw;"
    create_fdw_server = f"""CREATE SERVER provider_{server_name}_schema
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '{host}', dbname '{server_name}');"""
    create_fdw_user = f"""CREATE USER MAPPING FOR current_user
    SERVER provider_{server_name}_schema
    OPTIONS (user '{user}', password '{password}');
    """
    create_schema = f"CREATE SCHEMA provider_{server_name}_schema;"
    import_fdw_tables = f"""IMPORT FOREIGN SCHEMA public
    FROM SERVER provider_{server_name}_schema
    INTO provider_{server_name}_schema;"""
    commands.append(create_fdw)
    commands.append(create_fdw_server)
    commands.append(create_fdw_user)
    commands.append(create_schema)
    commands.append(import_fdw_tables)
    return commands


def create_provider_main(data):  # partner becoming provider funtion
    ui_partner_name = data.get("partner_name")
    ui_target_name = data.get("service_provider_to")

    try:
        partner, target = get_tenant_names(ui_partner_name, ui_target_name)
        logging.debug(f"Make the {partner} provider for {target}")

        if partner and target:
            commands_list = create_provider_commands(partner, target)
            # for command in commands_list:
            #     logging.info(command)
            # Execute commands in the database connection
            conn = None

            try:
                conn = psycopg2.connect(
                    dbname=target,
                    user=os.getenv("LOCAL_DB_USER"),
                    password=os.getenv("LOCAL_DB_PASSWORD"),
                    host=os.getenv("LOCAL_DB_HOST"),
                    port=os.getenv("LOCAL_DB_PORT"),
                )
                cursor = conn.cursor()
                for command in commands_list:
                    try:
                        cursor.execute(command)
                    except psycopg2.Error as e:
                        if "already exists" in str(e):
                            logging.debug(
                                f"Schema provider_{partner}_schema already exists."
                            )
                            return_flag = False
                            return_schema_name = f"provider_{partner}_schema"
                            return {
                                "flag": True,
                                "return_flag": return_flag,
                                "message": f"Schema {return_schema_name} already exists.",
                            }
                        else:
                            return {
                                "flag": False,
                                "return_flag": return_flag,
                                "message": f"Schema {return_schema_name} already exists.",
                            }
                conn.commit()

                return_flag = True
                return_schema_name = f"provider_{partner}_schema"
                try:
                    update_tenant_table = f"update tenant set partner_provider_schema={return_schema_name} and service_provider_to={ui_target_name} and service_proider_status=True where tenant_name=%s"
                    cursor.execute(update_tenant_table, (ui_partner_name,))
                    conn.commit
                except Exception as e:
                    logging.error(f"Error in making partner provider check log")

            except Exception as e:
                logging.error(f"An error occurred: {e}")
                return_flag = False
                return_schema_name = f"provider_{partner}_schema"
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

        else:
            return_flag = False
            return_schema_name = None
            raise ValueError(f"Error while getting tenant names")
    except Exception as e:
        logging.error(f"Error in making partner provider check log")
        return_flag = False
        return_schema_name = None
    response = {
        "flag": True,
        "return_flag": return_flag,
        "return_schema_name": return_schema_name,
    }
    return response


def get_tenant_names(provider_partner, target):  # partner becoming provider funtion
    """
    This function retrieves the tenant names and associated database names for two provided tenants from the 'tenant' table
    in the 'common_utils' database. It extracts the database names of the 'provider_partner' and 'target' and returns them.
    """
    load_dotenv()
    logging.debug(f"UI names: {provider_partner}, {target}")
    # Initialize variables
    source_server, target_db = None, None
    # Connect to the database
    try:
        with psycopg2.connect(
            dbname="common_utils",
            user=os.getenv("LOCAL_DB_USER"),
            password=os.getenv("LOCAL_DB_PASSWORD"),
            host=os.getenv("LOCAL_DB_HOST"),
            port=os.getenv("LOCAL_DB_PORT"),
        ) as conn:

            with conn.cursor() as cursor:
                # Prepare and execute the query
                query = "SELECT tenant_name, db_name FROM tenant WHERE tenant_name IN (%s, %s)"
                cursor.execute(query, (provider_partner, target))
                results = cursor.fetchall()
                # Convert results to a dictionary
                tuple_dict = {key: value for key, value in results}
                # Assign appropriate values to source_server and target_db
                source_server = tuple_dict.get(provider_partner)
                target_db = tuple_dict.get(target)

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"An error occurred: {error}")
        source_server, target_db = None, None
        # raise
    finally:
        logging.info("Database connection closed.")

    # Return the extracted values
    return source_server, target_db


def drop_provider_commands(schema_name):  # partner becoming provider funtion
    """
    This function generates a list of SQL commands to drop all objects related to a foreign data wrapper (FDW),
    including the schema, user mapping, FDW server, and the FDW extension itself for a given schema.
    """
    # List to store the generated SQL commands
    commands = []
    # Drop the schema and all its objects (tables, views, etc.) using CASCADE
    drop_schema = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;"
    commands.append(drop_schema)

    # Drop the user mapping associated with the foreign server for the current user
    drop_fdw_user = (
        f"DROP USER MAPPING IF EXISTS FOR current_user SERVER {schema_name};"
    )
    commands.append(drop_fdw_user)

    # Drop the foreign data wrapper (FDW) server
    drop_fdw_server = f"DROP SERVER IF EXISTS {schema_name} CASCADE;"
    commands.append(drop_fdw_server)

    # Drop the postgres_fdw extension if no other objects depend on it
    drop_fdw_extension = "DROP EXTENSION IF EXISTS postgres_fdw CASCADE;"
    commands.append(drop_fdw_extension)

    # Return the list of SQL drop commands
    return commands


def remove_provider(data):
    """
    This function removes all the foreign data wrapper (FDW) related objects (schema, user mapping, FDW server, and extension)
    for a given tenant by first querying the 'tenant' table to get the database name and provider schema, then connecting to
    the appropriate database to execute the necessary drop commands.
    """
    tenant = data.get("partner_name")
    # Load environment variables
    load_dotenv()

    # Flag to track success of the removal process
    return_flag = True
    try:
        conn = None

        # Try connecting to the 'common_utils' database
        try:
            conn = psycopg2.connect(
                dbname="common_utils",
                user=os.getenv("LOCAL_DB_USER"),
                password=os.getenv("LOCAL_DB_PASSWORD"),
                host=os.getenv("LOCAL_DB_HOST"),
                port=os.getenv("LOCAL_DB_PORT"),
            )
        except Exception as e:
            # Log connection error
            logging.error(f"An error occurred while connecting:common_utils {e}")

        if not conn:
            # If connection fails, set flag to False and raise an error
            return_flag = False
            raise ValueError("Cannot connect to common_utils db")

        # Create a cursor to execute the query
        cursor = conn.cursor()

        # Query to get the associated database name and schema for the given tenant
        schema_query = (
            "SELECT db_name, partner_provider_schema FROM tenant WHERE tenant_name = %s"
        )

        # Execute the query with the tenant name
        cursor.execute(schema_query, (tenant,))

        # Fetch the result as a list of tuples
        results = cursor.fetchall()

        if not results:
            raise ValueError(f"Unable to fetch provider schema for {tenant}")

        # Convert the result into a dictionary
        tuple_dict = {key: value for key, value in results}
        # Extract the database name and schema name from the dictionary
        db_name, schema_name = next(iter(tuple_dict.items()))
        # Try to connect to the target database where the schema and FDW objects reside
        try:
            conn2 = psycopg2.connect(
                dbname=db_name,
                user=os.getenv("LOCAL_DB_USER"),
                password=os.getenv("LOCAL_DB_PASSWORD"),
                host=os.getenv("LOCAL_DB_HOST"),
                port=os.getenv("LOCAL_DB_PORT"),
            )
        except Exception as e:
            logging.error(f"An error occurred while connecting:{db_name} {e}")

        if not conn2:
            raise ValueError(f"Cannot connect to {db_name} db")

        # Generate the drop commands using the drop_provider_commands function
        commands_list = drop_provider_commands(schema_name)
        if not commands_list:
            raise ValueError(f"Unable to fetch DROP Commands")

        # Create a cursor for executing drop commands in the target database
        cursor2 = conn2.cursor()

        # Iterate over the list of commands and execute each one
        for command in commands_list:
            logging.info(f"Executing command: {command}")
            try:
                cursor2.execute(command)  # Execute the command
                conn2.commit()  # Commit the transaction
            except Exception as e:
                # If any command fails, rollback and log the error
                logging.error(f"Error while executing command {command}: {e}")
                return_flag = False
                conn2.rollback()
                break  # Stop further execution in case of an error
        remove_from_common_utils = f"update tenant set partner_provider_schema='' and service_provider_to='' and service_proider_status=False where tenant_name=%s"
        cursor.execute(remove_from_common_utils, (tenant,))
        conn.commit
    except Exception as e:
        # If there's an error in the overall process, log it and set return_flag to False
        return_flag = False
        logging.error(f"Error while removing provider: {e}")

    finally:
        cursor.close()
        cursor2.close()
        # Close the connection
        conn.close()
        conn2.close()

    # Return the final status of the operation
    return return_flag


def create_tenant_db_service_provider(data):
    logging.info(f"Request data recieved")
    try:
        create_tenant_db(data)
    except Exception as e:
        logging.exception
    create_provider_main(data)
    message = "Partner Created Successfully"
    response = {"flag": True, "message": message}
    return response


def get_headers_mapping(
    tenant_database,
    module_list,
    role,
    user,
    tenant_id,
    sub_parent_module,
    parent_module,
    data,
    common_utils_database,
):
    """
    Description: The  function retrieves and organizes field mappings,headers,and module features
    based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields,and
    compiles features into a structured dictionary for each module.
    """
    # logging.info("Starting to retrieve headers mapping for modules: %s", module_list)
    ##Database connection
    # database = DB(tenant_database, **db_config)
    # common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    feature_module_name = data.get("feature_module_name", "")
    user_name = data.get("username") or data.get("user_name") or data.get("user")
    tenant_name = data.get("tenant_name") or data.get("tenant")
    try:
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": tenant_name}, ["id"]
        )["id"].to_list()[0]
    except Exception as e:
        logging.exception(f"Getting exception at fetching tenant id {e}")
    ret_out = {}
    # Iterate over each module name in the provided module list
    for module_name in module_list:
        # logging.debug("Processing module: %s", module_name)
        out = common_utils_database.get_data(
            "field_column_mapping", {"module_name": module_name}
        ).to_dict(orient="records")
        pop_up = []
        general_fields = []
        table_fileds = {}
        # Categorize the fetched data based on field types
        for data in out:
            if data["pop_col"]:
                pop_up.append(data)
            elif data["table_col"]:
                table_fileds.update(
                    {
                        data["db_column_name"]: [
                            data["display_name"],
                            data["table_header_order"],
                        ]
                    }
                )
            else:
                general_fields.append(data)
        # Create a dictionary to store categorized fields
        headers = {}
        headers["general_fields"] = general_fields
        headers["pop_up"] = pop_up
        headers["header_map"] = table_fileds
        try:
            final_features = []

            # Fetch all features for the 'super admin' role
            if role.lower() == "super admin":
                all_features = common_utils_database.get_data(
                    "module_features", {"module": feature_module_name}, ["features"]
                )["features"].to_list()
                if all_features:
                    final_features = json.loads(all_features[0])
            else:
                final_features = get_features_by_feature_name(
                    user_name, tenant_id, feature_module_name, common_utils_database
                )
                logging.info(
                    "Fetched features for user '%s': %s", user_name, final_features
                )

        except Exception as e:
            logging.info(f"there is some error {e}")
            pass
        # Add the final features to the headers dictionary
        headers["module_features"] = final_features
        ret_out[module_name] = headers
    # logging.info("Completed headers mapping retrieval.")
    return ret_out


def get_features_by_feature_name(
    user_name, tenant_id, feature_name, common_utils_database
):
    # Fetch user features from the database
    user_features_raw = common_utils_database.get_data(
        "user_module_tenant_mapping",
        {"user_name": user_name, "tenant_id": tenant_id},
        ["module_features"],
    )["module_features"].to_list()
    logging.debug("Raw user features fetched: %s", user_features_raw)

    # Parse the JSON string to a dictionary
    user_features = json.loads(
        user_features_raw[0]
    )  # Assuming it's a list with one JSON string

    # Initialize a list to hold features for the specified feature name
    features_list = []

    # Loop through all modules to find the specified feature name
    for module, features in user_features.items():
        if feature_name in features:
            features_list.extend(features[feature_name])
    logging.info("Retrieved features: %s", features_list)

    return features_list


def get_data_and_format(data, database, table_name, Environment=None, Partner=None):
    """
    Fetches data from a specified database table and formats it into a dictionary.

    Parameters:
    - database (DB): The database object used to execute the query.
    - table_name (str): The name of the table to fetch data from.
    - Environment (str, optional): An optional filter for the environment.
    - Partner (str, optional): An optional filter for the partner.

    Returns:
    - dict: A dictionary containing the table name as the key
      and a list of records as the value.
    """
    logging.info("Fetching data from table: %s", table_name)
    logging.info("Filters applied - Environment: %s, Partner: %s", Environment, Partner)

    mod_pages = data.get("mod_pages", {})
    pages = {}

    # Check if mod_pages exists and extract pagination info
    if "mod_pages" in data:
        start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
        end = data["mod_pages"].get("end") or 100  # Default to 100 if no value
        logging.debug(f"Starting page is {start} and ending page is {end}")
        limit = data.get("limit", 100)

        # Calculate pages
        pages["start"] = start
        pages["end"] = end

        # Build the count query based on filters
        count_query = (
            f"SELECT COUNT(*) FROM {table_name}"  # Directly inject the table_name here
        )
        count_params = []  # Start with an empty list for parameters

        # Add filters to the query if Environment or Partner is provided
        if Environment and Partner:
            count_query += f" WHERE env = '{Environment}' AND partner = '{Partner}'"
        elif Environment:
            count_query += f" WHERE env = '{Environment}'"

        elif Partner:
            count_query += f" WHERE partner = '{Partner}'"

        # Log the constructed query and parameters
        logging.debug("Executing count query: %s", count_query)
        logging.debug("With parameters: %s", count_params)

        try:
            # Ensure parameters are passed correctly
            count_result = database.execute_query(
                count_query, True
            )  # Ensure it's a tuple

            # Log the result of the count query
            logging.debug("Count query result: %s", count_result)

            # Check if the result is a DataFrame or if it returns a scalar value
            if isinstance(count_result, pd.DataFrame):
                if not count_result.empty:
                    # Extract the count from the DataFrame
                    count_value = count_result.iloc[0, 0]
                else:
                    raise ValueError("Count query returned an empty DataFrame.")
            elif isinstance(
                count_result, bool
            ):  # If it returns a boolean, something went wrong
                logging.error(
                    "Received a boolean result from the count query, something went wrong."
                )
                raise ValueError("Database query failed or returned unexpected result.")
            else:
                # Handle other possible return types (e.g., scalar value)
                logging.debug("Received a non-DataFrame result: %s", type(count_result))
                count_value = (
                    count_result  # Assuming count_result is directly the count value
                )

            pages["total"] = int(count_value)  # Store the count value

        except Exception as e:
            # Log the exception and raise a custom error
            logging.error("Error executing count query: %s", e)
            raise ValueError(f"Database query failed: {e}")

    # Fetch data based on the filters or no filters
    try:
        if Environment and Partner:
            logging.info("Fetching data with filters for Environment and Partner.")
            dataframe = database.get_data(
                table_name,
                {"env": Environment, "partner": Partner},
                None,
                {"last_modified_date_time": "desc"},
                [],
                None,
                mod_pages,
            )
        elif Environment:
            logging.info("Fetching data with filters for Environment and Partner.")
            dataframe = database.get_data(
                table_name,
                {"env": Environment},
                None,
                {"last_modified_date_time": "desc"},
                [],
                None,
                mod_pages,
            )
        elif Partner:
            logging.info("Fetching data with filters for Environment and Partner.")
            dataframe = database.get_data(
                table_name,
                {"partner": Partner},
                None,
                {"last_modified_date_time": "desc"},
                [],
                None,
                mod_pages,
            )

        else:
            logging.info("Fetching data without filters.")
            dataframe = database.get_data(
                table_name,
                None,
                None,
                {"last_modified_date_time": "desc"},
                [],
                None,
                mod_pages,
            )

        # Log the shape of the dataframe
        logging.debug("Fetched dataframe shape: %s", dataframe.shape)

        # Convert the dataframe to a list of dictionaries
        data_list = dataframe.to_dict(orient="records")
        logging.info(
            "Retrieved %d record(s) from table %s.", len(data_list), table_name
        )

        # Structure the data in the desired format
        formatted_data = {table_name: data_list}
        try:
            for record in formatted_data["amop_apis"]:
                for key, value in record.items():
                    if isinstance(
                        value, pd.Timestamp
                    ):  # Check if the value is a Timestamp
                        record[key] = value.strftime(
                            "%m-%d-%Y %H:%M:%S"
                        )  # Format Timestamp to string
        except:
            pass

    except Exception as e:
        logging.error("Error fetching data: %s", e)
        raise ValueError(f"Data fetching failed: {e}")

    return formatted_data, pages


def get_tenant_list(db, include_sub_tenants=False):
    logging.info("Fetching tenant list. Include sub-tenants: %s", include_sub_tenants)
    # common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    if include_sub_tenants:
        tenant_query_dataframe = db.get_data(
            "tenant",
            {"parent_tenant_id": "not Null", "is_active": True},
            ["tenant_name"],
        )
    else:
        tenant_query_dataframe = db.get_data(
            "tenant", {"parent_tenant_id": "Null", "is_active": True}, ["tenant_name"]
        )

    sorted_tenant_names = tenant_query_dataframe["tenant_name"].sort_values().to_list()

    return sorted_tenant_names


def get_superadmin_info(data):
    """
    Description:The get_superadmin_info function retrieves and formats
      superadmin-related data based on user permissions and requested modules,
    handling various sub-modules and tabs. It performs permission checks,
    database queries, and error logging, while measuring performance and updating audit records.
    """
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get("Partner", "")
    ##Retrieving the data
    request_received_at = data.get("request_received_at", "")
    session_id = data.get("session_id", "")
    ##database connection
    db = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    username = data.get("username", None)
    tenant_name = data.get("tenant_name", None)
    session_id = data.get("session_id", None)
    sub_parent_module = data.get("sub_parent_module", None)
    parent_module = data.get("parent_module", None)
    tenant_database = data.get('tenant_database','altaworx_central')

    # # Get tenant's timezone
    tenant_name = data.get("tenant_name", "")
    tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
    tenant_timezone = db.execute_query(tenant_timezone_query, params=[tenant_name])

    # Ensure timezone is valid
    if tenant_timezone.empty or tenant_timezone.iloc[0]["time_zone"] is None:
        raise ValueError("No valid timezone found for tenant.")

    tenant_time_zone = tenant_timezone.iloc[0]["time_zone"]
    match = re.search(
        r"\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)", tenant_time_zone
    )
    if match:
        tenant_time_zone = match.group(1).replace(
            " ", ""
        )  # Ensure it's formatted correctly
    # database Connection
    database = DB(tenant_database, **db_config)

    pages = {}
    try:
        role_name = data.get("role_name", None)
        data_dict_all = {}
        # Retrieve the main tenant ID based on the tenant name
        main_tenant_id = db.get_data("tenant", {"tenant_name": tenant_name}, ["id"])[
            "id"
        ].to_list()[0]
        # Check if the user is a superadmin
        if role_name and role_name.lower() != "super admin":
            message = "Module is not enabled for this user since he is not a superadmin"
            response_data = {"flag": True, "message": message}
            # logging.info(response_data)
        else:
            sub_module = data.get("sub_module", None)
            sub_tab = data.get("sub_tab", "")
            # Handle different sub-modules and tabs
            if sub_module and sub_module.lower() == "partner api":
                Environment = data.get("Environment", "")
                Partner = data.get("Selected_Partner", "")
                environment_list = ["SandBox", "UAT", "Prod"]
                partner_list = get_tenant_list(db)

                if sub_tab.lower() == "carrier apis":
                    # Fetch carrier API data
                    logging.info("Fetching carrier API data.")
                    carrier_api_data, pages = get_data_and_format(
                        data, db, "carrier_apis", Environment, Partner
                    )
                    data_dict_all = {
                        "Carrier_apis_data": carrier_api_data,
                        "Environment": environment_list,
                        "Partner": partner_list,
                    }
                    # Convert timestamps to a consistent format
                    # data_dict_all = convert_timestamps(data_dict_all)
                    headers_map = get_headers_mapping(
                        tenant_database,
                        ["carrier apis"],
                        role_name,
                        username,
                        main_tenant_id,
                        sub_parent_module,
                        parent_module,
                        data,
                        db,
                    )
                elif sub_tab.lower() == "amop apis":
                    # Fetch Amop API data
                    logging.info("Fetching Amop API data.")
                    amop_api_data, pages = get_data_and_format(
                        data, db, "amop_apis", Environment, Partner
                    )
                    data_dict_all = {
                        "amop_apis_data": amop_api_data,
                        "Environment": environment_list,
                        "Partner": partner_list,
                    }
                    # Convert timestamps to a consistent format
                    # data_dict_all = convert_timestamps(data_dict_all)
                    headers_map = get_headers_mapping(
                        tenant_database,
                        ["amop apis"],
                        role_name,
                        username,
                        main_tenant_id,
                        sub_parent_module,
                        parent_module,
                        data,
                        db,
                    )
            elif sub_module and sub_module.lower() == "partner modules":
                logging.info("Fetching partners and sub-partners.")
                # Extract 'sub_partner' and 'Partner' from the data dictionary
                flag = data.get("flag", "")
                # Initialize tenant_dict
                tenant_dict = {}
                # Fetch parent tenants
                parent_tenant_df = db.get_data(
                    "tenant",
                    {"parent_tenant_id": "Null", "is_active": True},
                    ["id", "tenant_name"],
                )
                # Fetch all active tenants
                all_tenants_df = db.get_data(
                    "tenant", {"is_active": True}, ["tenant_name", "parent_tenant_id"]
                )
                # Build the dictionary with parent-child relationships
                for _, parent_row in parent_tenant_df.iterrows():
                    parent_tenant_name = parent_row["tenant_name"]
                    parent_tenant_id = parent_row["id"]

                    # Filter and sort child tenants
                    child_tenants = sorted(
                        all_tenants_df[
                            all_tenants_df["parent_tenant_id"] == parent_tenant_id
                        ]["tenant_name"].tolist(),
                        key=lambda name: (
                            not name[0].isdigit(),
                            name,
                        ),  # False for digits, True for others
                    )
                    # Add to the dictionary
                    tenant_dict[parent_tenant_name] = child_tenants
                # Sort the tenant_dict keys
                tenant_dict = dict(
                    sorted(
                        tenant_dict.items(),
                        key=lambda item: (not item[0][0].isdigit(), item[0]),
                    )
                )

                if flag == "withoutparameters":
                    data_dict_all = {"partners_and_sub_partners": tenant_dict}
                else:
                    sub_partner = data.get("sub_partner", "")
                    partner = data.get("Selected_Partner", "")
                    # Determine the value to use for the query
                    query_value = sub_partner if sub_partner else partner

                    if query_value:
                        try:
                            tenant_query_dataframe = db.get_data(
                                "tenant", {"tenant_name": query_value}, ["id"]
                            )
                            tenant_id = tenant_query_dataframe.iloc[0]["id"]
                        except Exception as e:
                            # Log the exception for debugging
                            logging.warning(
                                f"An error occurred while executing the query: {e}"
                            )
                    else:
                        logging.info("No valid value provided for the query.")
                    ##roles dataframe for the tenant
                    role_dataframe = db.get_data(
                        "roles",
                        {"tenant_id": int(tenant_id)},
                        [
                            "id",
                            "role_name",
                            "is_active",
                            "created_by",
                            "modified_by",
                            "modified_date",
                        ],
                        {"id": "asc"},
                    )
                    roles_data = role_dataframe.to_dict(
                        orient="records"
                    )  # Convert to dictionary of lists
                    ##tenant_module
                    role_module_dataframe = db.get_data(
                        "tenant_module",
                        {"tenant_id": int(tenant_id)},
                        [
                            "id",
                            "module_name",
                            "is_active",
                            "modified_by",
                            "modified_date",
                        ],
                        {"id": "asc"},
                    )
                    # Convert to dictionary of lists
                    role_module_data = role_module_dataframe.to_dict(orient="records")

                    data_dict_all = {
                        "roles_data": roles_data,
                        "role_module_data": role_module_data,
                        "partners and sub partners": tenant_dict,
                    }
                data_dict_all = convert_timestamp_data(data_dict_all, tenant_time_zone)
                headers_map = get_headers_mapping(
                    tenant_database,
                    ["partner module", "role partner module"],
                    role_name,
                    username,
                    main_tenant_id,
                    "",
                    parent_module,
                    data,
                    db,
                )
            elif sub_module and sub_module.lower() == "partner creation":
                tenant_names = db.get_data(
                    "tenant", {"is_active": True}, ["tenant_name"]
                )["tenant_name"].to_list()
                parent_tenant_data = db.get_data(
                "tenant",
                {"parent_tenant_id": "Null", "is_active": True},
                ["id","tenant_name"],order={"tenant_name": "asc"},
                ).to_dict(orient="records")
                data_dict_all = {
                            "tenant_names": tenant_names,"parent_tenant_data":parent_tenant_data
                        }
                headers_map = "headers_map"

        response = {
            "flag": True,
            "data": serialize_data(data_dict_all),
            "headers_map": headers_map,
            "pages": pages,
        }
        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {
                "service_name": "tenant_onboarding",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "superadmin data",
                "module_name": "Super Admin",
                "request_received_at": request_received_at,
            }
            db.update_audit(audit_data_user_actions, "audit_user_actions")
        except Exception as e:
            logging.exception(f"exception is {e}")
        return response

    except Exception as e:
        logging.exception(f"Something went wrong and error is {e}")
        message = f"Something went wrong getting Super admin info {e}"
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {
                "service_name": "tenant_onboarding",
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": username,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "",
                "module_name": "Super Admin",
                "request_received_at": request_received_at,
            }
            db.log_error_to_db(error_data, "error_log_table")
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return {"flag": False, "message": message}


def update_superadmin_data(data):
    """
    updates module data for a specified module by checking user and tenant to
    get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view,
    handles errors, and logs relevant information.
    """
    logging.info("Starting update_superadmin_data function.")
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get("Partner", "")
    ##Restriction Check for the Amop API's
    try:
        logging.info("Checking permissions for user.")
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)

        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)

        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            logging.warning("Permission check failed.")
            return result
        else:
            logging.info("Permission check passed.")
            # Continue with other logic if needed
            pass
    except Exception as e:
        logging.info(f"got exception in the restriction")
    request_received_at = data.get("request_received_at", "")
    session_id = data.get("session_id", "")
    username = data.get("username", "")
    changed_data = data.get("changed_data", {})
    # logging.info(changed_data, 'changed_data')
    unique_id = changed_data.get("id", None)
    # logging.info(unique_id, 'unique_id')
    table_name = data.get("table_name", "")
    ##Database connection
    tenant_database = data.get("db_name", "")
    db = DB(tenant_database, **db_config)
    dbs = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        # Ensure unique_id is available
        if unique_id is not None:
            logging.info(f"Preparing to update data for unique_id: {unique_id}")
            # Prepare the update data
            update_data = {
                key: value
                for key, value in changed_data.items()
                if key != "unique_col" and key != "id"
            }
            # Perform the update operation
            dbs.update_dict(table_name, update_data, {"id": unique_id})
            logging.info("edited successfully")
            message = f"Data Edited Successfully"
            response_data = {"flag": True, "message": message}
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {
                "service_name": "Module Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response_data["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(changed_data),
                "module_name": "update_superadmin_data",
                "request_received_at": request_received_at,
            }
            dbs.update_audit(audit_data_user_actions, "audit_user_actions")
            return response_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {
                "service_name": "update_superadmin_data",
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": username,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "comments",
                "module_name": "Module Managament",
                "request_received_at": request_received_at,
            }
            dbs.log_error_to_db(error_data, "error_log_table")
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response


def convert_timestamp_data(df_dict, tenant_time_zone):
    """Convert timestamp columns in the provided dictionary list to the tenant's timezone."""
    # Create a timezone object
    target_timezone = timezone(tenant_time_zone)

    # List of timestamp columns to convert
    timestamp_columns = [
        "created_date",
        "modified_date",
        "deleted_date",
    ]  # Adjust as needed based on your data

    # Convert specified timestamp columns to the tenant's timezone
    for record in df_dict:
        for col in timestamp_columns:
            if col in record and record[col] is not None:
                # Convert to datetime if it's not already
                timestamp = pd.to_datetime(record[col], errors="coerce")
                if timestamp.tz is None:
                    # If the timestamp is naive, localize it to UTC first
                    timestamp = timestamp.tz_localize("UTC")
                # Now convert to the target timezone
                record[col] = timestamp.tz_convert(target_timezone).strftime(
                    "%m-%d-%Y %H:%M:%S"
                )  # Ensure it's a string
    return df_dict


def serialize_data(data):
    """Recursively convert pandas objects in the data structure to serializable types."""
    if isinstance(data, list):
        return [serialize_data(item) for item in data]
    elif isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    elif isinstance(data, pd.Timestamp):
        return data.strftime("%m-%d-%Y %H:%M:%S")  # Convert to string
    else:
        return data  # Return as is if not a pandas object
