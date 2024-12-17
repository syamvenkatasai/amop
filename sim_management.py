"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""

# Importing the necessary Libraries
import ast
import boto3
import requests
import os
import pandas as pd
from common_utils.email_trigger import send_email
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
import datetime
from datetime import time
from datetime import datetime, timedelta
import time
from io import BytesIO
from common_utils.data_transfer_main import DataTransfer
import json
import base64
import re
import pytds
from pytz import timezone
import numpy as np


# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}
logging = Logging(name="sim_management")

def path_func(data,path,access_token):

    if path == "/get_bulk_change_logs":
        result = get_bulk_change_logs(data)
    elif path == "/run_db_script":
        result = run_db_script(data)
    elif path == "/get_rev_assurance_data":
        result = get_rev_assurance_data(data)
    elif path == "/update_inventory_info":
        result = get_device_history(data)
    elif path == "/get_status_history":
        result = get_status_history(data)
    elif path == "/update_inventory_data":
        result = update_inventory_data(data)
    elif path == "/inventory_dropdowns_data":
        result = inventory_dropdowns_data(data)
    elif path == "/edit_cost_center":
        result = Edit_Cost_Center(data)
    elif path == "/update_rate_plan":
        result = Update_Carrier_Rate_Plan(data)
    elif path == "/update_username":
        result = Update_Username(data)
    elif path == "/sim_order_form_mail_trigger":
        result = sim_order_form_mail_trigger(data)
    elif path == "/update_sim_management_modules_data":
        result = update_sim_management_modules_data(data)
    elif path == "/get_bulk_change_history":
        result = get_bulk_change_history(data)
    elif path == "/optimization_dropdown_data":
        result = optimization_dropdown_data(data)
    elif path == "/customer_pool_row_data":
        result = customer_pool_row_data(data)
    elif path == "/get_new_bulk_change_data":
        result = get_new_bulk_change_data(data)
    elif path == "/update_bulk_change_data":
        result = update_bulk_change_data(data)
    elif path == "/download_bulk_upload_template":
        result = download_bulk_upload_template(data)
    elif path == "/bulk_import_data":
        result = bulk_import_data(data)
    elif path == "/customers_dropdown_inventory":
        result = customers_dropdown_inventory(data)
    elif path == "/add_service_line_dropdown_data":
        result = add_service_line_dropdown_data(data)
    elif path == "/submit_service_line_dropdown_data":
        result = submit_service_line_dropdown_data(data)
    elif path == "/add_service_product_dropdown_data":
        result = add_service_product_dropdown_data(data)
    elif path == "/submit_add_service_product_dropdown_data":
        result = submit_add_service_product_dropdown_data(data)
    elif path == "/assign_service_dropdown_data":
        result = assign_service_dropdown_data(data)
    elif path == "/deactivate_service_product":
        result = deactivate_service_product(data)
    elif path == "/manage_deactivated_sims":
        result = manage_deactivated_sims()
    elif path == "/import_bulk_data":
        result = import_bulk_data(data)
    elif path == "/get_inventory_data":
        result = get_inventory_data(data)
    elif path == "/bulk_upload_download_template":
        result = bulk_upload_download_template(data)
    elif path == "/update_features_pop_up_data":
        result = update_features_pop_up_data(data)
    elif path == "/statuses_inventory":
        result = statuses_inventory(data)
    elif path == "/carrier_rate_plan_list_view":
        if access_token:
            result = carrier_rate_plan_list_view_(data)
        else:
            result = carrier_rate_plan_list_view(data)
    elif path == "/get_sim_management_features":
        result = get_sim_management_features(data)
    elif path == "/get_optimization_pop_up_data":
        result = get_optimization_pop_up_data(data)
    elif path == "/start_optimization":
        result = start_optimization(data)
    elif path == "/qualification_data":
        result = qualification_data(data)
    else:
        result = {"error": "Invalid path or method"}
        logging.warning("Invalid path or method requested: %s", path)
    
    return result

def get_headers_mappings(
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
    ##Database connection
    ret_out = {}
    try:
        logging.info(f"Module name is :{module_list} and role is {role}")
        # common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        feature_module_name = data.get("feature_module_name", "")
        user_name = data.get("username") or data.get("user_name") or data.get("user")
        tenant_name = data.get("tenant_name") or data.get("tenant")
        parent_module_name = data.get("parent_module_name") or data.get("parent_module")
        try:
            tenant_id = common_utils_database.get_data(
                "tenant", {"tenant_name": tenant_name}, ["id"]
            )["id"].to_list()[0]
            logging.debug(f"tenant_id  is :{tenant_id}")
        except Exception as e:
            logging.exception(f"Getting exception at fetching tenant id {e}")
        # Iterate over each module name in the provided module list
        for module_name in module_list:
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
            logging.info(f"Got the header map here")
            try:
                final_features = []

                # Fetch all features for the 'super admin' role
                if role.lower() == "super admin":
                    all_features = common_utils_database.get_data(
                        "module_features",
                        {
                            "module": feature_module_name,
                            "parent_module_name": parent_module_name,
                        },
                        ["features"],
                    )["features"].to_list()
                    if all_features:
                        final_features = json.loads(all_features[0])
                else:
                    final_features = get_features_by_feature_name(
                        user_name,
                        tenant_id,
                        feature_module_name,
                        common_utils_database,
                        parent_module_name,
                    )

            except Exception as e:
                logging.warning(f"there is some error {e}")
                pass
            # Add the final features to the headers dictionary
            headers["module_features"] = final_features
            ret_out[module_name] = headers
    except Exception as e:
        logging.warning("there is some error %s", e)

    return ret_out


def get_features_by_feature_name(
    user_name, tenant_id, feature_name, common_utils_database, parent_module_name
):
    """
    Fetches features for a given user and tenant by feature name from the database.
    It first looks for the features under a specified parent module name.
    If no features are found under the parent module, it checks other modules.

    Args:
        user_name (str): The username for which features need to be retrieved.
        tenant_id (str): The tenant ID associated with the user.
        feature_name (str): The specific feature name to search for.
        common_utils_database (object): Database utility object to interact with the database.
        parent_module_name (str): The name of the parent module to search for features under.

    Returns:
        list: A list of features for the specified feature name, or an empty list if none are found.
    """

    features_list = []  # Initialize an empty list to store the retrieved features

    try:
        # Fetch user features from the database for the given user and tenant
        user_features_raw = common_utils_database.get_data(
            "user_module_tenant_mapping",  # Table to query
            {"user_name": user_name, "tenant_id": tenant_id},  # Query parameters
            ["module_features"],  # Columns to fetch
        )[
            "module_features"
        ].to_list()  # Convert the result to a list

        logging.debug("Raw user features fetched: %s", user_features_raw)

        # Parse the JSON string into a dictionary of user features
        user_features = json.loads(
            user_features_raw[0]
        )  # Assuming the result is a list with one JSON string

        # Initialize a list to hold features for the specified feature name
        features_list = []

        # First, check by parent_module_name for the specified feature
        for module, features in user_features.items():
            if module == parent_module_name and feature_name in features:
                features_list.extend(
                    features[feature_name]
                )  # Add features if found under parent module

        # If no features found under parent_module_name, check other modules
        if not features_list:
            for module, features in user_features.items():
                if feature_name in features:  # Check for the feature in other modules
                    features_list.extend(features[feature_name])

        logging.info(
            "Retrieved features: %s", features_list
        )  # Log the retrieved features

    except Exception as e:
        # Catch any exceptions and log a warning
        logging.warning("There was an error while fetching features: %s", e)

    return features_list  # Return the list of retrieved features


def get_sim_management_features(data):
    """
    Description: The  function retrieves and organizes field mappings,headers,and module features
    based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields,and
    compiles features into a structured dictionary for each module.
    """
    common_utils_database = DB("common_utils", **db_config)
    database = DB("altaworx_central", **db_config)
    parent_module_name = data.get("parent_module_name", "")
    feature_module_name = data.get("feature_module_name", "")
    user_name = data.get("username") or data.get("user_name") or data.get("user")
    tenant_name = data.get("tenant_name") or data.get("tenant")
    role = data.get("role", "")
    try:
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": tenant_name}, ["id"]
        )["id"].to_list()[0]
    except Exception as e:
        logging.exception(f"Getting exception at fetching tenant id {e}")
    ret_out = {}

    try:
        final_features = []

        # Fetch all features for the 'super admin' role
        if role.lower() == "super admin":
            all_features = common_utils_database.get_data(
                "module_features",
                {
                    "module": feature_module_name,
                    "parent_module_name": parent_module_name,
                },
                ["features"],
            )["features"].to_list()
            if all_features:
                final_features = json.loads(all_features[0])
        else:
            final_features = get_features_by_feature_name(
                user_name,
                tenant_id,
                feature_module_name,
                common_utils_database,
                parent_module_name,
            )
            logging.info(
                "Fetched features for user '%s': %s", user_name, final_features
            )

    except Exception as e:
        logging.info(f"there is some error {e}")
        pass
    response = {
        "flag": True,
        "module_features": final_features,
        "message": "Features Fetched Successfully",
    }
    return response


def get_device_history(data):
    """
    Description: Retrieves device history data from the database based on unique identifiers and columns provided in the input data.
    Validates the acccess token and logs the request, then fetches and returns the device history if the token is valid.
    """
    logging.info(f"Request Data Recieved")
    Partner = data.get("Partner", "")
    # Start time  and date calculation
    start_time = time.time()
    username = data.get("username", None)
    request_received_at = data.get("request_received_at", "")
    tenant_name = data.get("tenant_name", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)
    # database Connection
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:

        response_data = []
        # fetching the  unique identifiers and column name from the input data
        unique_ids = data.get("unique_ids", [])
        logging.debug(f"unique_ids are {unique_ids}")
        unique_column = data.get("unique_column", "")
        # If both unique IDs and the unique column are provided, fetch data from the database
        if unique_ids and unique_column:
            return_df = database.get_data(
                "sim_management_inventory_action_history",
                where={unique_column: unique_ids},
            ).to_dict(orient="records")
            response_data = return_df
        # Preparing a success message for the response
        message = "device histories data sent sucessfully"
        response = {"flag": True, "message": message, "Modules": response_data}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response_data["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "fetching the device history data",
                "module_name": "get_device_history",
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response

    except Exception as e:
        logging.exception(f"Something went wrong and error is {e}")
        message = "Something went wrong while getting device_history"
        # Preparing error data to log the error details in the database
        # Error Management
        error_data = {
            "service_name": "Module_api",
            "created_date": request_received_at,
            "error_messag": message,
            "error_type": e,
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": request_received_at,
        }
        common_utils_database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "message": message}


def get_status_history(data):
    """
    Retrieves the status history of a SIM management inventory item based on the provided ID.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_id' for querying the status history.

    Returns:
    - dict: A dictionary containing the status history data, header mapping,
    and a success message or an error message.
    """
    logging.info(f"Request data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    ##fetching the request data
    list_view_data_id = data.get("list_view_data_id", "")
    logging.debug(f"list_view_data_id is : {list_view_data_id}")
    session_id = data.get("session_id", "")
    username = data.get("username", "")
    Partner = data.get("Partner", "")
    role_name = data.get("role_name", "")
    list_view_data_id = int(list_view_data_id)
    request_received_at = data.get("request_received_at", None)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    tenant_database = data.get("db_name", "altaworx_central")
    try:
        ##Database connection
        database = DB(tenant_database, **db_config)
        # Fetch status history data from the database
        sim_management_inventory_action_history_dict = database.get_data(
            "sim_management_inventory_action_history",
            {"sim_management_inventory_id": list_view_data_id},
            [
                "service_provider",
                "iccid",
                "msisdn",
                "customer_account_number",
                "customer_account_name",
                "previous_value",
                "current_value",
                "date_of_change",
                "change_event_type",
                "changed_by",
            ],
        ).to_dict(orient="records")
        # Handle case where no data is returned
        if not sim_management_inventory_action_history_dict:
            ##Fetching the header map for the inventory status history
            headers_map = get_headers_mappings(
                tenant_database,
                ["inventory status history"],
                role_name,
                "username",
                "main_tenant_id",
                "sub_parent_module",
                "parent_module",
                data,
                common_utils_database,
            )
            message = "No status history data found for the provided SIM management inventory ID."
            response = {
                "flag": True,
                "status_history_data": [],
                "header_map": headers_map,
                "message": message,
            }
            return response

        # Helper function to serialize datetime objects to strings
        def serialize_dates(data):
            for key, value in data.items():
                if isinstance(value, datetime):
                    data[key] = value.strftime("%m-%d-%Y %H:%M:%S")
            return data

        # Apply date serialization to each record
        sim_management_inventory_action_history_dict = [
            serialize_dates(record)
            for record in sim_management_inventory_action_history_dict
        ]
        ##Fetching the header map for the inventory status history
        headers_map = get_headers_mappings(
            tenant_database,
            ["inventory status history"],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
            common_utils_database,
        )
        message = f"Status History data Fetched Successfully"
        # # Get tenant's timezone
        tenant_name = data.get("tenant_name", "")
        tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
        tenant_timezone = common_utils_database.execute_query(tenant_timezone_query, params=[tenant_name])
        try:
            # Ensure timezone is valid
            if tenant_timezone.empty or tenant_timezone.iloc[0]["time_zone"] is None:
                logging.warning("No valid time zone found")

            tenant_time_zone = tenant_timezone.iloc[0]["time_zone"]
            match = re.search(
                r"\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)", tenant_time_zone
            )
            if match:
                tenant_time_zone = match.group(1).replace(
                    " ", ""
                )  # Ensure it's formatted correctly
        except Exception:
            logging.warning("Exception in the time zone")
        sim_management_inventory_action_history_dict = convert_timestampdata(sim_management_inventory_action_history_dict, tenant_time_zone)
        # Prepare success response
        response = {
            "flag": True,
            "status_history_data": serialize_data(sim_management_inventory_action_history_dict),
            "header_map": headers_map,
            "message": message,
        }
        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": list_view_data_id,
                "module_name": "Reports",
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.exception(f"exception is {e}")
        return response
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = f"Unable to fetch the status history data"
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
                "comments": "",
                "module_name": "Module Managament",
                "request_received_at": request_received_at,
            }
            common_utils_database.log_error_to_db(error_data, "error_log_table")
        except Exception as e:
            logging.exception(f"exception is {e}")
        return response


def get_rev_assurance_data(data):
    """
    Retrieves the status history of a SIM management inventory item based on the provided ID.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_params' for querying the status history.

    Returns:
    - dict: A dictionary containing the List view data, header mapping, and a success message or an error message.
    """
    logging.info(f"Request Data Recieved")
    Partner = data.get("tenant_name", "")
    role_name = data.get("role_name", "")
    request_received_at = data.get("request_received_at", "")
    username = data.get("username", " ")
    session_id = data.get("session_id", False)
    variance = data.get("variance", False)

    table = data.get("table", "vw_rev_assurance_list_view_with_count")
    # Start time  and date calculation
    start_time = time.time()
    # Initialize the database connection
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    tenant_database = data.get("db_name", "altaworx_central")
    try:
        rev_assurance_data = []
        pages = {}
        if "mod_pages" in data:
            start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
            end = data["mod_pages"].get("end") or 100  # Default to 100 if no value
            logging.debug(f"starting page is {start} and ending page is {end}")
            limit = data.get("limit", 100)
            # Calculate pages
            pages["start"] = start
            pages["end"] = end
            count_params = [table]
            if variance == False:
                count_query = "SELECT COUNT(*) FROM %s where tenant_id=1" % table
            else:
                count_query = (
                    "SELECT COUNT(*) FROM %s where device_status <> rev_io_status and tenant_id=1"
                    % table
                )
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            pages["total"] = int(count_result)

        params = [start, end]
        if variance == False:
            query = """SELECT customer_name,customer_id,
                    service_number,
                    iccid,
                    customer_rate_plan_name,
                    service_provider,
                    device_status,
                    TO_CHAR(carrier_last_status_date::date, 'YYYY-MM-DD') AS carrier_last_status_date,
                    rev_io_status,
                    TO_CHAR(activated_date::date, 'YYYY-MM-DD') AS activated_date,
                    rev_active_device_count,
                    rev_total_device_count,
                    package_id,
                    rate,
                    device_status <> rev_io_status as variance,
                    description,
                    service_product_id,
                    rev_account_number
                    FROM vw_rev_assurance_list_view_with_count
                    where tenant_id=1
                    OFFSET %s LIMIT %s;
                    """
        else:
            query = """SELECT customer_name,customer_id,
                    service_number,
                    iccid,
                    customer_rate_plan_name,
                    service_provider,
                    device_status,
                    TO_CHAR(carrier_last_status_date::date, 'YYYY-MM-DD') AS carrier_last_status_date,
                    rev_io_status,
                    TO_CHAR(activated_date::date, 'YYYY-MM-DD') AS activated_date,
                    rev_active_device_count,
                    rev_total_device_count,
                    package_id,
                    rate,
                    device_status <> rev_io_status as variance,
                    description,
                    service_product_id,
                    rev_account_number
                    FROM vw_rev_assurance_list_view_with_count where device_status <> rev_io_status
                    AND tenant_id = 1
                    OFFSET %s LIMIT %s;
                    """
        rev_assurance_data = database.execute_query(query, params=params).to_dict(
            orient="records"
        )
        # Generate the headers mapping
        headers_map = get_headers_mappings(
            tenant_database,
            ["rev assurance"],
            role_name,
            "",
            "",
            "",
            "",
            data,
            common_utils_database,
        )
        # Prepare the response
        response = {
            "flag": True,
            "rev_assurance_data": rev_assurance_data,
            "header_map": headers_map,
            "pages": pages,
        }
        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "tenant_name": Partner,
                "session_id":session_id,
                "comments": "fetching the rev assurance data",
                "module_name": "get_rev_assurance_data",
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        # Error Management
        error_data = {
            "service_name": "Sim management",
            "created_date": request_received_at,
            "error_messag": message,
            "error_type": e,
            "user": username,
            "tenant_name": Partner,
            "comments": message,
            "session_id":session_id,
            "module_name": "get_rev_assurance_data",
            "request_received_at": request_received_at,
        }
        common_utils_database.log_error_to_db(error_data, "error_table")
        response = {"flag": False, "error": str(e)}
        return response


def add_service_line_dropdown_data(data):
    """
    Description:
    Retrieves add_service_line dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.

    Parameters:
        data (dict): A dictionary containing the input data such as 'username', 'tenant_name', 'service_provider',
                     'customer_name', 'session_id', 'module_name', and 'db_name'.

    Returns:
        dict: A response dictionary containing the success flag, message, and dropdown data. If an error occurs, an error message is returned.
    """
    logging.info(f"Request Data Received")
    # Start time and date calculation
    start_time = time.time()
    username = data.get("username", None)
    tenant_name = data.get("tenant_name", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)
    request_received_at = data.get("request_received_at", None)
    partner = data.get("Partner", None)
    # Database Connection
    tenant_database = data.get("db_name", "")
    try:
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    except Exception as db_exception:
        logging.exception(f"Failed to connect to database {db_exception}")
    try:
        response_data = {}
        service_provider = data.get("service_provider", None)
        rev_customer_name = data.get("customer_name", None)
        authentication_id_query=f'''
            SELECT integration_authentication_id FROM revcustomer r
	            INNER JOIN integration_authentication i on i.id = r.integration_authentication_id
                  where customer_name ='{rev_customer_name}' and r.is_active = True
                  AND r.is_deleted = False and i.tenant_id = 1
        '''
        authentication_ids=database.execute_query(authentication_id_query,True)['integration_authentication_id'].to_list()
        logging.debug(f"authentication_ids are {authentication_ids}")

        # Get customer rate plan dropdown data
        logging.debug("Fetching customer rate plan dropdown data...")
        response_data["customer_rate_plan_dropdown"] = sorted(
            set(
                database.get_data(
                    "customerrateplan",
                    {"service_provider_name": service_provider, "is_active": True},
                    ["rate_plan_name"],
                )["rate_plan_name"].to_list()
            )
        )

        # Get rev provider data
        logging.debug("Fetching rev provider data...")
        response_data["rev_provider"] = sorted(
            set(
                database.get_data(
                    "rev_provider",
                    {
                        "integration_authentication_id": authentication_ids,
                        "is_active": True,
                    },
                    ["description"],
                )["description"].to_list()
            )
        )

        # Get service type data
        logging.debug("Fetching service type data...")
        response_data["service_type"] = sorted(
            set(
                database.get_data(
                    "rev_service_type",
                    {
                        "integration_authentication_id": authentication_ids,
                        "is_active": True,
                    },
                    ["description", "service_type_id"],
                ).apply(lambda row: f"{row['description']}-{row['service_type_id']}", axis=1).to_list()
            )
        )
        # Get rev usage plan group data
        logging.debug("Fetching rev usage plan group data...")
        response_data["rev_usage_plan_group"] = sorted(
            set(
                database.get_data(
                    "rev_usage_plan_group",
                    {
                        "integration_authentication_id": authentication_ids,
                        "is_active": True,
                    },
                    ["description"],
                )["description"].to_list()
            )
        )

        # Dependent dropdowns
        response_data["rev_provider_id_map"] = {
            item["description"]: item["provider_id"]
            for item in sorted(
                database.get_data(
                    "rev_provider",
                    {"integration_authentication_id": authentication_ids, "is_active": True},
                    ["description", "provider_id"],
                ).to_dict(orient="records"),
                key=lambda x: x["description"]
            )
        }


        response_data["rate"] = form_depandent_dropdown_format(
            sorted(
                database.get_data(
                    "rev_product",
                    {
                        "integration_authentication_id": authentication_ids,
                        "is_active": True,
                    },
                    columns=["rate", "provider_id"],
                ).to_dict(orient="records"),
                key=lambda x: x["rate"]
            ),
            "provider_id",
            "rate",
        )
        # Step 2: Get the product list based on the authentication IDs
        product_id_list = database.get_data(
            'rev_product',
            {
                "integration_authentication_id": authentication_ids,
                "is_active": True,
            },
            ["description", "product_id"]
        ).to_dict(orient='records')

        # Step 3: Extract product IDs from product_id_list for the SQL query
        product_ids = [product['product_id'] for product in product_id_list]

        # Step 4: Format the product_ids into a string for the SQL query (comma-separated)
        product_ids_str = ', '.join(map(str, product_ids))

        # Step 5: Construct the SQL query to get the rates for these products
        rev_product_package_data_query = f"""
            SELECT product_id, rate
            FROM rev_package_product
            WHERE product_id IN ({product_ids_str})
        """

        # Step 6: Execute the query to get the rates
        rev_product_package_dataframe = database.execute_query(rev_product_package_data_query, True)

        # Ensure the query returns the data as a list of dictionaries (or handle it)
        if isinstance(rev_product_package_dataframe, str):
            # If it's a string, try to parse it (if it's JSON or CSV)
            import json
            rev_product_package_dataframe = json.loads(rev_product_package_dataframe)

        # Convert the DataFrame to a dictionary with 'records' orientation
        rev_product_package_dict = rev_product_package_dataframe.to_dict(orient='records')

        # Step 7: Map the rates to the products in product_id_list
        # Create a dictionary for fast lookup of rates by product_id
        rate_lookup = {rate_data['product_id']: rate_data.get('rate', '') for rate_data in rev_product_package_dict}

        # Step 8: Assign rates to products, or an empty string if the rate is missing
        for product in product_id_list:
            product_id = product.get('product_id')
            # Look up the rate using the product_id, if not found, default to an empty string
            product['rate'] = rate_lookup.get(product_id, '')

        # Step 9: Assign the final updated product list with rates to a variable
        updated_product_list = product_id_list
        response_data["rev_product_rate"] = updated_product_list

        package_data=database.get_data('rev_package',{"integration_authentication_id":authentication_ids},['package_id','description']).to_dict(orient='records')

        # Step 1: Extract the package_ids from package_data
        package_ids = [package['package_id'] for package in package_data]

        # Step 2: Construct the package_data_mapping query
        package_data_mapping_query = f'''
        SELECT description, rate, package_id FROM public.rev_package_product
        WHERE package_id IN ({','.join(map(str, package_ids))})
        AND integration_authentication_id IN ({','.join(map(str, authentication_ids))})
        AND is_active=True
        '''

        # Step 3: Fetch the subdescription (rate) from the database
        package_product_data = database.execute_query(package_data_mapping_query, True)

        # Debugging: Print the raw result of the query to inspect the structure
        #print("Raw package_product_data:", package_product_data)

        # Check if package_product_data is a Pandas DataFrame
        if isinstance(package_product_data, pd.DataFrame):
            # Step 4: Initialize a dictionary to hold the structured result
            result = []

            # Step 5: Process each unique package_id
            package_ids = package_product_data['package_id'].unique()

            for package_id in package_ids:
                # Extract the description for the current package_id
                package_desc = package_product_data[package_product_data['package_id'] == package_id]['description'].iloc[0]

                # Step 6: Create a list for subdescriptions and rates for the current package_id
                subdescription_data = []
                subdescription_columns = [col for col in package_product_data.columns if 'description' in col.lower()]

                for _, row in package_product_data[package_product_data['package_id'] == package_id].iterrows():
                    subdescription_data.append({
                        f"subdescription": row['description'],
                        "rate": row['rate']
                    })

                # Step 7: Create the structure for this package_id
                result.append([
                    {
                        "package_id": str(package_id),
                        "description": package_desc
                    },

                    subdescription_data
                ])
        else:
            logging.error(f"Error: The data is not a Pandas DataFrame.")
            result = []

        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "tenant_name": partner,
                "session_id":session_id,
                "comments": "Fetching the Add Service line data",
                "module_name": "add_service_line_dropdown_data",
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.warning(f"Exception is {e}")

        response_data["rev_package_rate"] = result
        logging.info(f"The drop down data is fetched")
        message = "add_service_line data sent successfully"
        response = {"flag": True, "message": message, "response_data": response_data}

        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response
    except Exception as e:
        logging.exception(f"Something went wrong and error is {e}")
        message = "Something went wrong while getting add service line"
        # Error Management
        error_data = {
            "service_name": "SIM management",
            "created_date": start_time,
            "error_messag": message,
            "error_type": e,
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": start_time,
        }
        database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "response_data": {}, "message": message}


def submit_service_line_dropdown_data(data):
    """
    Description:
        This function processes the data received for adding a service line dropdown. It validates the input data,
        retrieves necessary information from the database, creates bulk change and request records, and logs the process.
        If successful, it returns a success response with relevant data; otherwise, it handles errors and logs the details.

    Parameters:
        data (dict): The input data dictionary containing all the required information for submitting the service line dropdown.
            Expected keys:
            - tenant_name (str): The name of the tenant.
            - username (str): The username of the person making the request.
            - session_id (str): The session ID associated with the request.
            - module_name (str): The name of the module making the request.
            - service_provider (str): The service provider associated with the request.
            - db_name (str): The database name for the tenant.
            - submit_data (dict): A dictionary containing additional data for the service line, such as:
                - revio_product (str): The description of the RevIO product.
                - service_type (str): The description of the service type.
                - provider_name (str): The name of the service provider.
                - rev_usage_plan_group (str): The usage plan group description.
                - customer_name (str): The customer name.
                - revio_package (str): The package description.
                - iccid (list): A list of ICCIDs (SIM card identifiers).
                - description (str): A description of the service line.
                - rate (list): A list of rate information.
                - quantity (int): The quantity of service lines.
                - activation_date (str): The activation date of the service.
                - add_rate_plan (str): The additional rate plan.
                - rate_plan (str): The rate plan.
                - prorate (str): Whether prorate is applicable.

    Returns:
        dict: A response dictionary with the following keys:
            - flag (bool): True if the request was successfully processed, False if an error occurred.
            - message (str): A message indicating the success or failure of the operation.
            - data (dict): Contains information about the bulk change and request records created.
            - bulk_change_id_20 (int): The bulk change ID for the request.

    """
    logging.info(f"Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    tenant_name = data.get("tenant_name", None)
    username = data.get("username", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)
    service_provider = data.get("service_provider", None)
    # database Connection
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        submitted_data = data.get("submit_data", None)
        revio_product = submitted_data.get("revio_product", None)
        service_type = submitted_data.get("service_type", None)
        provider_name = submitted_data.get("provider_name", None)
        rev_usage_plan_group = submitted_data.get("rev_usage_plan_group", None)
        customer_name = submitted_data.get("customer_name", None)
        revio_package = submitted_data.get("revio_package", None)
        iccids = submitted_data.get("iccid", [])
        description = submitted_data.get("description", None)
        rate = submitted_data.get("rate", [])
        quantity = submitted_data.get("quantity", None)
        activation_date = submitted_data.get("activation_date", None)
        add_rate_plan = submitted_data.get("add_rate_plan", None)
        rate_plan = submitted_data.get("rate_plan", None)
        prorate = submitted_data.get("prorate", None)
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": tenant_name}, ["id"]
        )["id"].to_list()[0]
        logging.debug(f"tenant_id is {tenant_id}")
        Change_type_id = database.get_data(
            "sim_management_bulk_change_type",
            {"display_name": "Create Rev Service"},
            ["id"],
        )["id"].to_list()[0]
        try:
            rev_cust_id = database.get_data(
                "revcustomer",
                {"customer_name": customer_name, "is_active": True},
                ["id"],
            )["id"].to_list()[0]
        except:
            rev_cust_id = None
        try:
            customer_id = database.get_data(
                "customers", {"rev_customer_id": rev_cust_id, "is_active": True}, ["id"]
            )["id"].to_list()[0]
            customer_id = int(customer_id) if customer_id else None
        except:
            customer_id = None
        try:
            rev_product_id = database.get_data(
                "rev_product",
                {"description": revio_product, "is_active": True},
                ["product_id"],
            )["product_id"].to_list()[0]
            rev_product_id = int(rev_product_id) if rev_product_id else None
        except:
            rev_product_id = None
        try:
            rev_service_type_id = database.get_data(
                "rev_service_type",
                {"description": service_type, "is_active": True},
                ["service_type_id"],
            )["service_type_id"].to_list()[0]
            rev_service_type_id = (
                int(rev_service_type_id) if rev_service_type_id else None
            )
        except:
            rev_service_type_id = None
        try:
            rev_provider_id = database.get_data(
                "rev_provider",
                {"description": provider_name, "is_active": True},
                ["provider_id"],
            )["provider_id"].to_list()[0]
            rev_provider_id = int(rev_provider_id) if rev_provider_id else None
        except:
            rev_provider_id = None
        try:
            rev_usage_plan_group_id = database.get_data(
                "rev_usage_plan_group",
                {"description": rev_usage_plan_group, "is_active": True},
                ["usage_plan_group_id"],
            )["usage_plan_group_id"].to_list()[0]
            rev_usage_plan_group_id = (
                int(rev_usage_plan_group_id) if rev_usage_plan_group_id else None
            )
        except:
            rev_usage_plan_group_id = None
        try:
            integration_id = database.get_data(
                "revcustomer",
                {"customer_name": customer_name, "is_active": True},
                ["integration_authentication_id"],
            )["integration_authentication_id"].to_list()[0]
            integration_id = int(integration_id) if integration_id else None
        except:
            integration_id = None
        try:
            service_provider_id = database.get_data(
                "serviceprovider", {"service_provider_name": service_provider}, ["id"]
            )["id"].to_list()[0]
            service_provider_id = (
                int(service_provider_id) if service_provider_id else None
            )
        except:
            service_provider_id = None
        try:
            rev_customer_id = database.get_data(
                "revcustomer",
                {"customer_name": customer_name, "is_active": True},
                ["rev_customer_id"],
            )["rev_customer_id"].to_list()[0]
            rev_customer_id = int(rev_customer_id) if rev_customer_id else None
        except:
            rev_customer_id = None
        try:
            rev_package_id = database.get_data(
                "rev_package",
                {"description": revio_package, "is_active": True},
                ["package_id"],
            )["package_id"].to_list()[0]
            rev_package_id = int(rev_package_id) if rev_package_id else None
        except:
            rev_package_id = None
        return_dict = {}
        logging.info(rev_cust_id, "rev_cust_id")
        logging.info(customer_id, "customer_id")
        logging.info(rev_product_id, "rev_product_id")
        logging.info(rev_service_type_id, "rev_service_type_id")
        logging.info(rev_provider_id, "rev_provider_id")
        logging.info(rev_usage_plan_group_id, "rev_usage_plan_group_id")
        logging.info(rev_customer_id, "rev_customer_id")
        logging.info(rev_package_id, "rev_package_id")
        logging.info(integration_id, "integration_id")
        Device_Bulk_Change = {}
        Device_Bulk_Change["change_request_type_id"] = Change_type_id
        Device_Bulk_Change["service_provider_id"] = service_provider_id
        Device_Bulk_Change["tenant_id"] = tenant_id
        Device_Bulk_Change["status"] = "NEW"
        Device_Bulk_Change["created_by"] = username
        Device_Bulk_Change["is_active"] = True
        Device_Bulk_Change["is_deleted"] = False
        Device_Bulk_Change["service_provider"] = service_provider
        Device_Bulk_Change["change_request_type"] = "Create Rev Service"
        Device_Bulk_Change["modified_by"] = username
        Device_Bulk_Change["uploaded"] = len(iccids)
        Device_Bulk_Change["created_by"] = username
        Device_Bulk_Change["processed_by"] = username
        bulkchangeid = database.insert_data(
            Device_Bulk_Change, "sim_management_bulk_change"
        )
        return_dict["sim_management_bulk_change"] = [Device_Bulk_Change]
        bulkchange_df = database.get_data(
            "sim_management_bulk_change", Device_Bulk_Change
        )
        bulkchange_id = bulkchange_df["id"].to_list()[0]
        logging.info(bulkchange_id, "bulkchange_id")
        create_new_bulk_change_request_dict_all = []
        for iccid in iccids:
            try:
                device_id = int(
                    database.get_data(
                        "sim_management_inventory", {"iccid": iccid}, ["id"]
                    )["id"].to_list()[0]
                )
                logging.debug(f"device_id is :{device_id}")
            except:
                device_id = None
            create_new_bulk_change_request_dict = {}
            create_new_bulk_change_request_dict["iccid"] = iccid
            create_new_bulk_change_request_dict["bulk_change_id"] = bulkchange_id
            create_new_bulk_change_request_dict["tenant_id"] = tenant_id
            create_new_bulk_change_request_dict["created_by"] = username
            create_new_bulk_change_request_dict["status"] = "NEW"
            create_new_bulk_change_request_dict["device_id"] = device_id
            create_new_bulk_change_request_dict["request_created_by"] = username
            create_new_bulk_change_request_dict["is_active"] = True
            create_new_bulk_change_request_dict["is_deleted"] = False

            change_request = {
                "Number": iccid,
                "ICCID": iccid,
                "RevCustomerId": rev_customer_id,
                "DeviceId": device_id,
                "CreateRevService": True,
                "ServiceTypeId": rev_service_type_id,
                "RevPackageId": rev_package_id,
                "RevProductIdList": rev_product_id,
                "RateList": rate,
                "Prorate": prorate,
                "Description": description,
                "EffectiveDate": None,
                "AddCustomerRatePlan": add_rate_plan,
                "CustomerRatePlan": rate_plan,
                "CustomerRatePool": None,
                "IntegrationAuthenticationId": integration_id,
                "ProviderId": rev_provider_id,
                "ActivatedDate": activation_date,
                "UsagePlanGroupId": rev_usage_plan_group_id,
                "AddCarrierRatePlan": None,
                "CarrierRatePlan": None,
                "CommPlan": None,
                "JasperDeviceID": device_id,
                "SiteId": customer_id,
            }
            create_new_bulk_change_request_dict["change_request"] = json.dumps(
                change_request
            )
            create_new_bulk_change_request_dict_all.append(
                create_new_bulk_change_request_dict
            )
            change_request_id = database.insert_data(
                create_new_bulk_change_request_dict,
                "sim_management_bulk_change_request",
            )

        return_dict["sim_management_bulk_change_request"] = (
            create_new_bulk_change_request_dict_all
        )
        message = " Add Service Line data submitted sucessfully "
        response = {"flag": True, "message": message}
        response["data"] = return_dict
        response["bulk_chnage_id_20"] = bulkchange_id

        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response

    except Exception as e:
        logging.exception(f"Something went wrong and error is {e}")
        message = "Something went wrong while getting submitting add service line"
        # Error Management
        error_data = {
            "service_name": "SIM management",
            "created_date": start_time,
            "error_messag": message,
            "error_type": e,
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": start_time,
        }
        database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "message": message}


def add_service_product_dropdown_data(data):
    """
    Description:
        This function processes the input data to retrieve service product dropdown data from the database based on unique identifiers
        such as customer name and related service information. It validates the access token and logs the request.
        The function fetches details like customer information, service descriptions, and products, and formats the data for the dropdown.
        If the data is successfully retrieved, it returns the data with a success message. In case of an error,
        an error message is logged and returned.

    Parameters:
        data (dict): A dictionary containing all necessary input data for processing the service product dropdown:
            Expected keys:
            - username (str): The username of the person making the request.
            - tenant_name (str): The name of the tenant requesting the service.
            - session_id (str): The session ID associated with the request.
            - module_name (str): The module making the request.
            - db_name (str): The database name for the tenant.
            - customer_name (str): The customer name whose data is being queried.

    Returns:
        dict: A response dictionary with the following keys:
            - flag (bool): True if the request was successfully processed, False if an error occurred.
            - message (str): A message indicating the success or failure of the operation.
            - response_data (dict): Contains the retrieved data formatted for dropdowns.
                - rev_product (list): A list of unique product descriptions.
                - rev_product_id_map (dict): A mapping of product descriptions to provider IDs.
                - rate (dict): A mapping of provider IDs to rates.
    """
    # checking the access token valididty
    logging.info(f"Request Data recieved")
    # Start time and date calculation
    start_time = time.time()
    username = data.get("username", None)
    tenant_name = data.get("tenant_name", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)
    # database Connection
    tenant_database = data.get("db_name", "")
    partner = data.get("Partner", "")
    request_received_at = data.get("request_received_at", "")
    # database Connection
    try:
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    except Exception as db_exception:
        logging.exception(f"Failed to connect to database {db_exception}")
    # Check if customer_name is provided
    rev_customer_name = data.get("customer_name", None)
    if rev_customer_name is None:
        message = "customer name is required."
        return {"flag": False, "message": message}
    try:
        response_data = {}
        response_message = ""
        # Query 1: Get customer ID
        query1 = """SELECT rev_customer_id FROM revcustomer WHERE customer_name = %s LIMIT 1"""
        params = [rev_customer_name]
        rev_customer_data = database.execute_query(query1, params=params)
        if rev_customer_data.empty:
            response_message = "Data not provided for customer name"
            response_data["customer_data_error"] = response_message
        else:
            customer_id = rev_customer_data.iloc[0, 0]
            logging.debug(f"customer_id is {customer_id}")
            # Query 2: Get rev_customer_id
            query2 = """SELECT rev_customer_id FROM revcustomer WHERE id = %s"""
            params2 = [customer_id]
            customer_data = database.execute_query(query2, params=params2)
            if customer_data.empty:
                response_message = "Data not provided for customer id"
                response_data["customer_id_error"] = response_message
            else:
                customer_data = customer_data.iloc[0, 0]

                # Query 3: Get service information
                query3 = """SELECT rev_service_id, rev_service_type_id FROM rev_service WHERE rev_customer_id = %s"""
                params3 = [str(customer_id)]
                rev_service_data = database.execute_query(query3, params=params3)
                if rev_service_data.empty:
                    response_message = (
                        "Data not provided for service id and service type id"
                    )
                    response_data["service_info_error"] = response_message
                else:
                    rev_service_id = rev_service_data.iloc[0, 0]
                    logging.debug(f"rev_service_id is {rev_service_id}")
                    service_type_id = rev_service_data.iloc[0, 1]
                    service_type_id = int(service_type_id)
                    # Query 4: Get service description
                    query4 = """SELECT description FROM rev_service_type WHERE id = %s"""
                    params4 = [service_type_id]
                    rev_service_type_data = database.execute_query(
                        query4, params=params4
                    )
                    if rev_service_type_data.empty:
                        description = "–"  # Default value when description is not found
                        response_message = f"{customer_id} - Service id: {rev_service_id} - Service Type: {description} (description not provided)"
                    else:
                        description = rev_service_type_data
                        response_message = f"{customer_id} - Service id: {rev_service_id} - Service Type: {description}"
        response_data["customer_name"] = response_message
        # Fetch authentication IDs
        authentication_ids = database.get_data(
            "revcustomer",
            {"customer_name": rev_customer_name, "is_active": True},
            ["integration_authentication_id"],
        )["integration_authentication_id"].to_list()
        # Step 2: Get the product list based on the authentication IDs
        product_id_list = database.get_data(
            'rev_product',
            {
                "integration_authentication_id": authentication_ids,
                "is_active": True,
            },
            ["description", "product_id"]
        ).to_dict(orient='records')

        # Step 3: Extract product IDs from product_id_list for the SQL query
        product_ids = [product['product_id'] for product in product_id_list]

        # Step 4: Format the product_ids into a string for the SQL query (comma-separated)
        product_ids_str = ', '.join(map(str, product_ids))

        # Step 5: Construct the SQL query to get the rates for these products
        rev_product_package_data_query = f"""
            SELECT product_id, rate
            FROM rev_package_product
            WHERE product_id IN ({product_ids_str})
        """

        # Step 6: Execute the query to get the rates
        rev_product_package_dataframe = database.execute_query(rev_product_package_data_query, True)

        # Ensure the query returns the data as a list of dictionaries (or handle it)
        if isinstance(rev_product_package_dataframe, str):
            # If it's a string, try to parse it (if it's JSON or CSV)
            import json
            rev_product_package_dataframe = json.loads(rev_product_package_dataframe)

        # Convert the DataFrame to a dictionary with 'records' orientation
        rev_product_package_dict = rev_product_package_dataframe.to_dict(orient='records')

        # Step 7: Map the rates to the products in product_id_list
        # Create a dictionary for fast lookup of rates by product_id
        rate_lookup = {rate_data['product_id']: rate_data.get('rate', '') for rate_data in rev_product_package_dict}

        # Step 8: Assign rates to products, or an empty string if the rate is missing
        for product in product_id_list:
            product_id = product.get('product_id')
            # Look up the rate using the product_id, if not found, default to an empty string
            product['rate'] = rate_lookup.get(product_id, '')

        # Step 9: Assign the final updated product list with rates to a variable
        updated_product_list = product_id_list
        response_data["rev_product_rate"] = updated_product_list
        message = "add_service_product data sent sucessfully"
        response = {"flag": True, "message": message, "response_data": response_data}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "tenant_name": partner,
                "session_id":session_id,
                "comments": "fetching the add_service_product_dropdown_data",
                "module_name": "add_service_product_dropdown_data",
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        logging.exception(f"Something went wrong and the error is {e}")
        message = "Something went wrong while getting add service product"
        # Error Management
        error_data = {
            "service_name": "SIM management",
            "error_message": message,
            "error_type": str(e),
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
        }
        database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "message": message}


def submit_add_service_product_dropdown_data(data):
    """
    Description:
    Submits add_service_product dropdown data to the database based on unique identifiers and columns provided in the input data.
    The function validates the required fields, retrieves necessary IDs from the database, and makes an API call to an external service.
    If the API response is successful, it inserts the data into the database and returns a success message.
    If the API call fails or any exception occurs during the process, the function logs the error and returns an error message.

    Parameters:
    - data (dict): A dictionary containing input data, which includes customer name, product name, rate, quantity, prorate,
                   effective date, description, and other details such as tenant name, username, and session ID.

    Returns:
    - dict: A dictionary containing the status of the operation ('flag': True/False), a message indicating the result
            ('message'), and any additional response data ('response_data') if applicable.
    """
    # logging.info(f"Request Data: {data}")

    # Start time  and date calculation
    start_time = time.time()
    tenant_name = data.get("tenant_name", None)
    username = data.get("username", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)
    # Database Connection
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        customer_name = data.get("submit_data", {}).get("customer_name", None)
        product_name = data.get("submit_data", {}).get("product_name", None)
        rate = data.get("submit_data", {}).get("rate", None)
        quantity = data.get("submit_data", {}).get("quantity", None)
        prorate = data.get("submit_data", {}).get("prorate", None)
        effective_date = data.get("submit_data", {}).get("effective_date", None)
        description = data.get("submit_data", {}).get("description", None)

        if not customer_name or not product_name or rate is None:
            return {
                "flag": False,
                "message": "Customer name, product name, and rate are required.",
            }
        rev_product_id = database.get_data(
            "rev_product",
            {"description": product_name, "is_active": True},
            ["product_id"],
        )["product_id"].to_list()
        rev_customer_id = database.get_data(
            "revcustomer",
            {"customer_name": customer_name, "is_active": True},
            ["customer_name"],
        )["customer_name"].to_list()
        logging.debug(f"rev_customer_id is :{rev_customer_id}")
        rev_add_service_product = {
            "rev_customer_id": rev_customer_id,
            "product_id": rev_product_id,
            "rate": rate,
            "quantity": quantity,
            "prorate": prorate,
            "created_by": username,
            "is_active": True,
            "is_deleted": False,
            "effective_date": effective_date,
            "description": description,
        }
        # Prepare data for API request
        # url = 'https://api.revioapi.com/v1/ServiceProduct'
        url = os.getenv("SERVICEPRODUCT", " ")
        headers = {
            "Ocp-Apim-Subscription-Key": "04e3d452d3ba44fcabc0b7085cdde431",
            "Authorization": "Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r",
        }
        params = {
            "customer_id": rev_customer_id,
            "product_id": rev_product_id,
            "rate": rate,
            "quantity": quantity,
            "generate_proration": prorate,
            "effective_date": effective_date,
            "description": description,
        }
        # Call the API and check response
        api_response = requests.get(url, headers=headers, params=params)
        if api_response.status_code == 200:
            # Only insert into database if the API call is successful
            data_rev_service_product = {
                "customer_id": rev_customer_id,
                "product_id": rev_product_id,
                "rate": rate,
                "quantity": quantity,
                "prorate": prorate,
                "created_by": username,
                "is_active": True,
                "is_deleted": False,
                "created_date": effective_date,
                "description": description,
            }
            # Insert data into the database
            product_id = database.insert_data(
                "rev_service_product", data_rev_service_product
            )
            message = "Add Service product data submitted successfully."
            response_data = {
                "flag": True,
                "message": message,
                "response": api_response.json(),
            }
        else:
            # API call failed, return error message
            raise Exception(
                f"Failed to retrieve data from client API: {api_response.status_code} - {api_response.text}"
            )
        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response_data
    except Exception as e:
        logging.exception(f"Something went wrong and the error is {e}")
        message = "Something went wrong while submitting add service line"
        # Error Management
        error_data = {
            "service_name": "SIM management",
            "created_date": start_time,
            "error_message": message,
            "error_type": str(e),
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": start_time,
        }
        database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "message": message}


def assign_service_dropdown_data(data):
    """
    Description: Retrieves add_service_product dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    """
    logging.info(f"Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    username = data.get("username", None)
    tenant_name = data.get("tenant_name", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)
    # database Connection
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        response_data = {}
        rev_customer_name = data.get("customer_name", None)
        try:
            rev_customer_id = database.get_data(
                "revcustomer",
                {"customer_name": rev_customer_name, "is_active": True},
                ["id"],
            )["id"].to_list()[0]
            rev_customer_id_real = database.get_data(
                "revcustomer",
                {"customer_name": rev_customer_name, "is_active": True},
                ["rev_customer_id"],
            )["rev_customer_id"].to_list()[0]
            logging.debug(
                f"rev_customer_id is {rev_customer_id} and rev_customer_id_real is {rev_customer_id_real}"
            )
        except:
            return {"flag": True, "message": "No data", "response_data": response_data}
        rev_service_query = f"""SELECT CONCAT(rs.number,' - Service_id:',rs.rev_service_id,' - Service_type:',rst.description) AS combined_output,rs.rev_service_id FROM public.rev_service as rs join rev_service_type as rst on rst.service_type_id = rs.rev_service_type_id
                            where rev_customer_id ='{rev_customer_id}' ORDER BY rs.id"""
        rev_service_df = database.execute_query(rev_service_query, True)
        response_data["rev_service"] = list(
            set(rev_service_df["combined_output"].to_list())
        )
        response_data["rev_service_id_map"] = rev_service_df.to_dict(orient="records")
        response_data["rev_service_id_map"] = {
            item["combined_output"]: item["rev_service_id"]
            for item in response_data["rev_service_id_map"]
        }
        # dependent dropdowns
        package_query = f"""SELECT distinct CONCAT(rp.description,' - package_id:',rp.package_id) AS combined_output,
                            rsp.service_id  from
                            revcustomer as rc join rev_service as rs on rs.rev_customer_id = rc.id
                            join rev_service_product as rsp on rs.rev_service_id =rsp.service_id
                            join rev_package AS rp ON rsp.package_id = rp.package_id::integer
                            WHERE rsp.is_active = TRUE
                                AND rsp.is_deleted = FALSE
                                AND rsp.integration_authentication_id = 1
                                AND rsp.status = 'ACTIVE'
                                AND rsp.package_id != 0
                                AND rc.rev_customer_id='{rev_customer_id_real}' """
        response_data["rev_package"] = database.execute_query(
            package_query, True
        ).to_dict(orient="records")
        response_data["rev_package"] = form_depandent_dropdown_format(
            response_data["rev_package"], "service_id", "combined_output"
        )
        package_query = f"""SELECT CONCAT(rp.description,' - product_id:',rsp.service_product_id) AS combined_output,
                            rsp.service_id  from
                            revcustomer as rc join rev_service as rs on rs.rev_customer_id = rc.id
                            join rev_service_product as rsp on rs.rev_service_id =rsp.service_id
                            join rev_product AS rp ON rsp.product_id = rp.product_id::integer
                            WHERE rsp.is_active = TRUE
                                AND rsp.is_deleted = FALSE
                                AND rsp.integration_authentication_id = 1
                                AND rsp.status = 'ACTIVE'
                                AND rsp.package_id != 0
                                AND rc.rev_customer_id='{rev_customer_id_real}' """
        response_data["rev_product"] = database.execute_query(
            package_query, True
        ).to_dict(orient="records")
        response_data["rev_product"] = form_depandent_dropdown_format(
            response_data["rev_product"], "service_id", "combined_output"
        )
        message = "add_service_product data sent sucessfully"
        response = {"flag": True, "message": message, "response_data": response_data}
        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response
    except Exception as e:
        logging.exception(f"Something went wrong and error is {e}")
        message = "Something went wrong while getting assign service line"
        # Error Management
        error_data = {
            "service_name": "SIM management",
            "created_date": start_time,
            "error_messag": message,
            "error_type": e,
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": start_time,
        }
        database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "message": message}


def submit_assign_service_data(data):
    """
    Description:
    Retrieves add_service_product dropdown data from the database based on unique identifiers and columns provided in the input data.
    The function fetches data for services, packages, and products associated with a customer, formats it for dependent dropdowns,
    and returns the data if found. If no data is found, it returns a "No data" message.
    If any error occurs during the process, the error is logged, and a failure message is returned.

    Parameters:
    - data (dict): A dictionary containing input data, including customer name, tenant name, session ID, and other relevant
                   details such as username and module name. The customer name is used to retrieve the associated service data.

    Returns:
    - dict: A dictionary containing:
        - 'flag' (bool): Status of the operation (True if successful, False if failed).
        - 'message' (str): A message indicating the outcome of the operation.
        - 'response_data' (dict): A dictionary containing the retrieved dropdown data for services, packages, and products
                                  or an empty dictionary if no data is found.
    """
    logging.info(f"Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    tenant_name = data.get("tenant_name", None)
    username = data.get("username", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)
    # database Connection
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    try:
        submit_data = data.get("submit_data", {})
        customer_id = database.get_data(
            "revcustomer",
            {
                "customer_name": submit_data.get("customer_name", None),
                "is_active": True,
            },
            ["rev_customer_id"],
        )["rev_customer_id"].to_list()[0]
        logging.debug(f"customer_id is {customer_id}")
        service_numbers = submit_data.get("service_number", None)
        effective_date = submit_data.get("effective_date", None)
        quantity = submit_data.get("quantity", 0)
        package_id = None
        if submit_data["revio_package"]:
            match2 = re.search(r"package_id:(\d+)", submit_data["revio_package"])
            if match2:
                package_id = match2.group(1)

        Service_id = None
        if submit_data["service_type"]:
            match2 = re.search(r"Service_id:(\d+)", submit_data["service_type"])
            if match2:
                Service_id = match2.group(1)

        product_ids = []
        if submit_data["revio_product"]:
            for product_id in submit_data["revio_product"]:
                match2 = re.search(r"product_id:(\d+)", product_id)
                if match2:
                    product_id_ = match2.group(1)
                    product_ids.append(product_id_)

        try:
            hit_all_apis(
                customer_id,
                Service_id,
                service_numbers,
                package_id,
                product_ids,
                effective_date,
                quantity,
            )
        except:
            pass

        message = " Assign Service data submitted sucessfully "
        response = {"flag": True, "message": message}
        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response
    except Exception as e:
        logging.exception(f"Something went wrong and error is {e}")
        message = "Something went wrong while getting submitting assign service line"
        # Error Management
        error_data = {
            "service_name": "SIM management",
            "created_date": start_time,
            "error_messag": message,
            "error_type": e,
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": start_time,
        }
        database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "message": message}


def hit_all_apis(
    customer_id,
    Service_id,
    service_numbers,
    package_id,
    product_ids,
    effective_date,
    quantity,
):
    """
    Description:
    This function interacts with external APIs to manage service products and inventory items for a given customer.
    It retrieves service product data based on service ID, package ID, and product IDs, checks the status of inventory items,
    creates new inventory items if they do not exist, assigns inventory items to services, and updates service product details
    like quantity and effective date.

    Parameters:
    - customer_id (int): The unique identifier for the customer.
    - Service_id (int): The unique identifier for the service.
    - service_numbers (list): A list of service numbers (identifiers) for which inventory actions need to be performed.
    - package_id (int): The unique identifier for the package associated with the service products.
    - product_ids (list or None): A list of service product IDs to filter the relevant products. If None, all products are considered.
    - effective_date (str): The effective date to update for the service products.
    - quantity (int): The quantity to update for the service products.

    Returns:
    - bool: True if the process completes successfully, indicating that the inventory and service products have been handled.
            Returns False in case of failure (though no explicit False is currently returned in this implementation).
    """
    try:
        logging.info(f"Function recieved here")
        # BASE_URL = "https://api.revioapi.com/v1"
        BASE_URL = os.getenv("REVIOAPI", " ")
        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": "04e3d452d3ba44fcabc0b7085cdde431",
            "Authorization": "Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r",
        }
        search_inventory_url = f"{BASE_URL}/ServiceProduct?search.service_id={Service_id}&search.status=ACTIVE"
        search_inventory_response = requests.get(search_inventory_url, headers=headers)
        logging.info(
            f"Search Inventory Response: {search_inventory_response.status_code}"
        )
        service_product_response = search_inventory_response.json()
        service_products = service_product_response["records"]
        service_products_by_package_id = [
            str(item["service_product_id"])
            for item in service_products
            if str(item["package_id"]) == str(package_id)
        ]
        logging.debug(service_products_by_package_id, "service_products_by_package_id")
        did_service_products = (
            service_products_by_package_id if product_ids is None else list(product_ids)
        )
        inventory_list = []
        for identifier in service_numbers:
            get_search_inventory_url = (
                f"{BASE_URL}/InventoryItem?search.identifier={identifier}"
            )
            get_search_inventory_response = requests.get(
                get_search_inventory_url, headers=headers
            )
            logging.debug(
                f"Get Search Inventory Response: {get_search_inventory_response.status_code}"
            )
            logging.debug(get_search_inventory_response.json())
            inventory_items = get_search_inventory_response.json()
            if inventory_items["record_count"] <= 0:
                create_inventory_url = f"{BASE_URL}/InventoryItem"
                new_inventory_data = {
                    "inventory_type_id": 20,
                    "identifier": identifier,
                    "customer_id": customer_id,
                    "status": "AVAILABLE",
                }
                create_inventory_response = requests.post(
                    create_inventory_url, json=new_inventory_data, headers=headers
                )
                logging.debug(
                    f"Create Inventory Response: {create_inventory_response.status_code}"
                )
                logging.debug(create_inventory_response.json())
                logging.debug("added")
                if (
                    create_inventory_response.status_code == 200
                    or create_inventory_response.status_code == 201
                ):
                    inventory_list.append(identifier)
            if inventory_items["records"]:
                for record in inventory_items["records"]:
                    if record["status"] != "ASSIGNED":
                        inventory_list.append(identifier)

        get_dids_url = f"{BASE_URL}/ServiceInventory?search.service_id={Service_id}"
        get_dids_response = requests.get(get_dids_url, headers=headers)
        logging.info(f"Get DIDs Response: {get_dids_response.status_code}")
        dids_response = get_dids_response.json()
        dids_count = dids_response["record_count"]

        assign_inventory_url = f"{BASE_URL}/InventoryItem/assignService"
        assign_inventory_data = {
            "service_id": Service_id,
            "identifiers": inventory_list,
        }  # Data required for assigning
        assign_inventory_response = requests.patch(
            assign_inventory_url, json=assign_inventory_data, headers=headers
        )
        logging.debug(
            f"Assign Inventory Response: {assign_inventory_response.status_code}"
        )
        logging.info(assign_inventory_response.json())
        if (
            assign_inventory_response.status_code == 200
            or assign_inventory_response.status_code == 201
        ):
            for service_product in service_products:
                if str(service_product["service_product_id"]) in did_service_products:
                    patch_data = [
                        {"op": "replace", "path": "/quantity", "value": quantity},
                        {
                            "op": "replace",
                            "path": "/effective_date",
                            "value": effective_date,
                        },
                    ]
                    # Serialize to JSON format
                    json_patch = json.dumps(patch_data)
                    update_request_headers = {
                        "Content-Type": "application/json-patch+json",  # Equivalent to 'APPLICATION_JSON_PATCH'
                        "Ocp-Apim-Subscription-Key": "04e3d452d3ba44fcabc0b7085cdde431",
                        "Authorization": "Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r",
                    }
                    service_pro_id = int(service_product["service_product_id"])
                    update_product_url = f"{BASE_URL}/ServiceProduct/{service_pro_id}"
                    requests.patch(
                        update_product_url,
                        headers=update_request_headers,
                        data=json_patch,
                    )
    except Exception as e:
        logging.error("here we have an error", e)

    return True


def get_bulk_change_logs(data):
    """
    Retrieves the status history of a SIM management inventory item based on the provided ID.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_id' for querying the status history.

    Returns:
    - dict: A dictionary containing the status history data, header mapping, and a success message or an error message.
    """
    logging.info(f"Request Data Recieved")
    Partner = data.get("Partner", "")
    request_received_at = data.get("request_received_at", "")
    username = data.get("username", " ")
    tenant_name = data.get("tenant_name", "")
    session_id = data.get("session_id", " ")
    # Start time  and date calculation
    start_time = time.time()
    # Initialize the database connection
    tenant_database = data.get("db_name", "altaworx_central")
    # database Connection
    database = DB(tenant_database, **db_config)
    role_name = data.get("role_name", "")
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    tenant_database = data.get("db_name", "altaworx_central")
    try:
        # Fetch the list_view_data_id from the input data
        list_view_data_id = data.get("list_view_data_id", "")
        if not list_view_data_id:
            raise ValueError("list_view_data_id is required")
        list_view_data_id = int(list_view_data_id)
        logging.debug(f"list_view_data_id is {list_view_data_id}")
        request_id = data.get("requested_id", "")
        request_id = int(request_id)
        bulk_change_data = {}
        result = database.get_data(
            "sim_management_bulk_change_request",
            {"bulk_change_id": list_view_data_id, "id": request_id},
            ["iccid", "tenant_name"],
        ).to_dict(orient="records")

        iccid_data = result[0]
        try:
            service = database.get_data(
                "sim_management_bulk_change",
                {"id": list_view_data_id, "is_active": True},
                ["service_provider"],
            )["service_provider"].to_list()[0]
            # Remove duplicates based on the 'service_provider' field
            # Check if ICCID exists in sim_management_inventory with active status
            # Fetch inventory data for ICCID with 'Active' status
            inventory_result = database.get_data(
                "sim_management_inventory",
                {"iccid": iccid_data["iccid"], "sim_status": "Active"},
                ["iccid", "service_provider_display_name"],
            )

            if (
                inventory_result is False
                or not isinstance(inventory_result, pd.DataFrame)
                or inventory_result.empty
            ):
                # If ICCID is not found, add a note for missing ICCID
                bulk_change_data["note"] = (
                    f"The selected ICCID {iccid_data['iccid']} is not found."
                )
                print(bulk_change_data["note"])
            else:
                # Convert inventory data to dictionary
                inventory_dict = inventory_result.to_dict(orient="records")

                if inventory_dict:
                    # Check for service provider in the inventory data
                    service_provider = inventory_dict[0].get(
                        "service_provider_display_name"
                    )

                    # Compare service provider from inventory and bulk change data
                    if service_provider == service:
                        # Both ICCID and service provider are valid
                        bulk_change_data["service_provider"] = service_provider
                    else:
                        # Service provider mismatch
                        bulk_change_data["note"] = (
                            f"The selected ICCID {iccid_data['iccid']} does not have a matching service provider."
                        )
        except:
            pass

        # try:
        #     bulk_change_data.update(database.get_data('sim_management_bulk_change_request',{'bulk_change_id':list_view_data_id,'id':request_id}).to_dict(orient='records')[0])
        # except:
        #     pass

        try:
            bulk_change_data.update(
                database.get_data(
                    "sim_management_bulk_change_log",
                    {"bulk_change_id": list_view_data_id},
                ).to_dict(orient="records")[0]
            )
        except:
            pass

        for key, value in bulk_change_data.items():
            if type(value) != str:
                bulk_change_data[key] = str(value)
        # Generate the headers mapping
        headers_map = get_headers_mappings(
            tenant_database,
            ["bulkchange_logs"],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
            common_utils_database,
        )
        # Prepare the response
        response = {
            "flag": True,
            "get_bulk_change_logs": [bulk_change_data],
            "header_map": headers_map,
        }
        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "fetching the bulkchange log history data",
                "module_name": "get_bulk_change_logs",
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response

    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        # Error Management
        error_data = {
            "service_name": "Sim Management",
            "created_date": request_received_at,
            "error_messag": message,
            "error_type": e,
            "user": username,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": message,
            "module_name": "get_bulk_change_logs",
            "request_received_at": request_received_at,
        }
        common_utils_database.log_error_to_db(error_data, "error_table")
        response = {"flag": False, "error": str(e)}
        return response


def form_depandent_dropdown_format(data, main_col, sub_col):
    logging.info(f"Function reached upto here")
    result = {}
    # Iterate over each item in the list
    for item in data:
        main_col_value = item[main_col]
        sub_col_value = item[sub_col]

        # If the service provider is already a key in the dictionary, append the change type to the list
        if main_col_value in result:
            result[main_col_value].append(sub_col_value)
        else:
            # If the service provider is not a key, create a new list with the change type
            result[main_col_value] = [sub_col_value]
    # Sort the change types for each service provider
    for key in result:
        result[key] = sorted(result[key])  # Sort the list of change types

    return result


def form_depandent_dropdown_format_bulk_change(data, main_col, sub_col):
    logging.info(f"Function reached upto here")
    result = {}

    # Iterate over each item in the list
    for item in data:
        main_col_value = item[main_col]
        sub_col_value = item[sub_col]

        # If the service provider is already a key in the dictionary, add the change type to the set (to avoid duplicates)
        if main_col_value in result:
            result[main_col_value].add(sub_col_value)  # Use set to eliminate duplicates
        else:
            # If the service provider is not a key, create a new set with the change type
            result[main_col_value] = {
                sub_col_value
            }  # Initialize as a set to ensure uniqueness

    # Convert the sets back to sorted lists
    for key in result:
        result[key] = sorted(result[key])  # Sort the set and convert it back to a list

    return result


def get_new_bulk_change_data(data):
    """
    Description:
        Retrieves device history data from the database based on unique identifiers
        and columns provided in the input data. Validates the access token and logs
        the request, then fetches and returns the device history if the token is valid.

    Parameters:
        data (dict): Input data containing necessary parameters for querying the database
                     and determining which data needs to be returned.

    Returns:
        dict: Response data containing success or failure status, a message, and the requested data.
    """
    # logging.info(f"Request Data: {data}")
    # Start time calculation to measure the duration of the function execution
    start_time = time.time()

    # Extract necessary parameters from the input data
    username = data.get("username", None)
    tenant_name = data.get("tenant_name", None)
    request_received_at = data.get("request_received_at", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)

    # Database connection parameters
    tenant_database = data.get("db_name", "altaworx_central")
    database = DB(tenant_database, **db_config)

    try:
        # Initialize response data container
        response_data = {}

        # Extract specific change-related parameters
        service_provider = data.get("service_provider", None)
        Change_type = data.get("change_type", None)
        logging.debug(f"Change_type is {Change_type}")
        Create_Service_Product = data.get("Create Service/Product", None)

        def sorted_unique(data_list):
            """Helper function to sort a list and remove any None values."""
            return sorted(filter(None, set(data_list)))

        # Handle the case when "Create Service/Product" flag is True
        if Create_Service_Product:
            # Fetch dropdown data for various categories
            response_data["rev_customer_dropdown"] = sorted_unique(
                database.get_data(
                    "revcustomer",
                    {"is_active": True},
                    concat_columns=["customer_name", "rev_customer_id"],
                )["concat_column"].to_list()
            )
            response_data["rev_service_type"] = sorted_unique(
                database.get_data(
                    "rev_service_type",
                    {"is_active": True},
                    concat_columns=["description", "service_type_id"],
                )["concat_column"].to_list()
            )
            response_data["rev_product"] = sorted_unique(
                database.get_data(
                    "rev_product",
                    {"is_active": True},
                    concat_columns=["description", "product_id"],
                )["concat_column"].to_list()
            )

            message = "New Bulk change popups data sent successfully"
            response = {"flag": True, "message": message, "Modules": response_data}
            return response

        # If no service provider is specified, return service provider-change type mappings
        if not service_provider:
            response_data["new_change_SP_CT_map"] = sorted(
                database.get_data(
                    "sim_management_bulk_change_type_service_provider",
                    {"is_active": True},
                    ["change_type", "service_provider"],
                ).to_dict(orient="records"),
                key=lambda x: x["service_provider"] or "",
            )
            response_data["new_change_SP_CT_map"] = (
                form_depandent_dropdown_format_bulk_change(
                    response_data["new_change_SP_CT_map"],
                    "service_provider",
                    "change_type",
                )
            )
        else:
            # Retrieve screen sequence for the given service provider and change type
            response_data["screen_names_seq"] = sorted(
                database.get_data(
                    "bulk_change_popup_screens",
                    {"service_provider": service_provider, "change_type": Change_type},
                    ["screen_names_seq"],
                )["screen_names_seq"].to_list()
            )
            if response_data["screen_names_seq"]:
                response_data["screen_names_seq"] = response_data["screen_names_seq"][0]
            dropdown_data = {}

            # Process different change types and prepare respective dropdown data
            if Change_type == "Assign Customer":
                dropdown_data["rev_customer_dropdown"] = sorted_unique(
                    database.get_data(
                        "revcustomer",
                        {"is_active": True},
                        concat_columns=["customer_name", "rev_customer_id"],
                    )["concat_column"].to_list()
                )
                dropdown_data["customer_rate_plan_dropdown"] = sorted_unique(
                    database.get_data(
                        "customerrateplan",
                        {"service_provider_name": service_provider, "is_active": True},
                        concat_columns=["rate_plan_name", "rate_plan_code"],
                    )["concat_column"].to_list()
                )
                dropdown_data["customer_rate_pool_dropdown"] = sorted_unique(
                    database.get_data(
                        "customer_rate_pool",
                        {"is_active": True, "service_provider_name": service_provider},
                        concat_columns=["name", "id"],
                    )["concat_column"].to_list()
                )
                dropdown_data["rev_service_type"] = sorted_unique(
                    database.get_data(
                        "rev_service_type",
                        {"is_active": True},
                        concat_columns=["description", "service_type_id"],
                    )["concat_column"].to_list()
                )
                dropdown_data["rev_product"] = sorted_unique(
                    database.get_data(
                        "rev_product",
                        {"is_active": True},
                        concat_columns=["description", "product_id"],
                    )["concat_column"].to_list()
                )
                dropdown_data["rev_provider"] = sorted(
                    database.get_data(
                        "rev_provider",
                        {"is_active": True},
                        ["description", "provider_id"],
                    ).to_dict(orient="records"),
                    key=lambda x: x["description"] or "",
                )
                dropdown_data["rev_package"] = form_depandent_dropdown_format(
                    database.get_data(
                        "rev_package",
                        {"is_active": True},
                        columns=["provider_id"],
                        concat_columns=["description", "package_id"],
                    ).to_dict(orient="records"),
                    "provider_id",
                    "concat_column",
                )

            # More conditions for other change types...
            elif Change_type == "Activate New Service":
                dropdown_data["rev_customer_dropdown"] = sorted_unique(
                    database.get_data(
                        "revcustomer",
                        {"is_active": True},
                        concat_columns=["customer_name", "rev_customer_id"],
                    )["concat_column"].to_list()
                )
                dropdown_data["state"] = sorted(
                    [
                        "Alabama",
                        "Alaska",
                        "Arizona",
                        "Arkansas",
                        "California",
                        "Colorado",
                        "Connecticut",
                        "District of Columbia",
                        "Delaware",
                        "Florida",
                        "Georgia",
                        "Hawaii",
                        "Idaho",
                        "Illinois",
                        "Indiana",
                        "Iowa",
                        "Kansas",
                        "Kentucky",
                        "Louisiana",
                        "Maine",
                        "Maryland",
                        "Massachusetts",
                        "Michigan",
                        "Minnesota",
                        "Mississippi",
                        "Missouri",
                        "Montana",
                        "Nebraska",
                        "Nevada",
                        "New Hampshire",
                        "New Jersey",
                        "New Mexico",
                        "New York",
                        "North Carolina",
                        "North Dakota",
                        "Ohio",
                        "Oklahoma",
                        "Oregon",
                        "Pennsylvania",
                        "Rhode Island",
                        "South Carolina",
                        "South Dakota",
                        "Tennessee",
                        "Texas",
                        "Utah",
                        "Vermont",
                        "Virginia",
                        "Washington",
                        "West Virginia",
                        "Wisconsin",
                        "Wyoming",
                    ]
                )
                dropdown_data["customer_rate_plan_group"] = sorted_unique(
                    database.get_data(
                        "mobility_device_usage_aggregate",
                        {"is_active": True, "service_provider": service_provider},
                        ["data_group_id"],
                    )["data_group_id"].to_list()
                )
                result = database.get_data(
                    "mobility_feature",
                    {"service_provider_name": service_provider, "is_active": True},
                    concat_columns=["friendly_name", "soc_code"],
                )
                if (
                    result
                    and "concat_column" in result
                    and isinstance(result, pd.DataFrame)
                ):
                    dropdown_data["features_codes"] = sorted_unique(
                        result["concat_column"].to_list()
                    )
                else:
                    logging.error(
                        f"Invalid result returned for mobility_feature query: {result}"
                    )
                    dropdown_data["features_codes"] = []
                dropdown_data["customer_rate_plan_dropdown"] = sorted_unique(
                    database.get_data(
                        "customerrateplan",
                        {"service_provider_name": service_provider, "is_active": True},
                        concat_columns=["rate_plan_name", "rate_plan_code"],
                    )["concat_column"].to_list()
                )
                dropdown_data["customer_rate_pool_dropdown"] = sorted_unique(
                    database.get_data(
                        "mobility_device_usage_aggregate",
                        {"is_active": True, "service_provider": service_provider},
                        ["pool_id"],
                    )["pool_id"].to_list()
                )

            elif Change_type == "Change Carrier Rate Plan":
                dropdown_data["carrier_rate_plan_dropdown"] = sorted_unique(
                    database.get_data(
                        "carrier_rate_plan",
                        {"service_provider": service_provider, "is_active": True},
                        coalesce_columns=["friendly_name", "rate_plan_code"],
                    )["coalesce_column"].to_list()
                )
                dropdown_data["comm_plan_dropdown"] = sorted_unique(
                    database.get_data(
                        "sim_management_communication_plan",
                        {"service_provider_name": service_provider, "is_active": True},
                        ["communication_plan_name"],
                    )["communication_plan_name"].to_list()
                )
                dropdown_data["optimization_group_dropdown"] = sorted_unique(
                    database.get_data(
                        "optimization_group",
                        {"service_provider_name": service_provider, "is_active": True},
                        ["optimization_group_name"],
                    )["optimization_group_name"].to_list()
                )

            elif Change_type in ["Change Customer Rate Plan", "Change ICCID/IMEI"]:
                dropdown_data["customer_rate_plan_dropdown"] = sorted_unique(
                    database.get_data(
                        "customerrateplan",
                        {"service_provider_name": service_provider, "is_active": True},
                        concat_columns=["rate_plan_name", "rate_plan_code"],
                    )["concat_column"].to_list()
                )
                dropdown_data["customer_rate_pool_dropdown"] = sorted_unique(
                    database.get_data(
                        "customer_rate_pool",
                        {"is_active": True, "service_provider_name": service_provider},
                        concat_columns=["name", "id"],
                    )["concat_column"].to_list()
                )

            elif Change_type == "Update Device Status":
                integration_id = database.get_data(
                    "serviceprovider",
                    {"service_provider_name": service_provider},
                    ["integration_id"],
                )["integration_id"].to_list()[0]
                dropdown_data["Device_Status_dropdown"] = sorted_unique(
                    database.get_data(
                        "device_status",
                        {
                            "integration_id": integration_id,
                            "allows_api_update": True,
                            "is_active": True,
                        },
                        ["display_name"],
                    )["display_name"].to_list()
                )
                # # Remove "Active" if it exists in the Device_Status_dropdown list
                # if "Active" in dropdown_data["Device_Status_dropdown"]:
                #     dropdown_data["Device_Status_dropdown"].remove("Active")

                if service_provider in [
                    "Verizon - ThingSpace PN",
                    "Verizon - ThingSpace IoT",
                ]:
                    dropdown_data["carrier_rate_plan_dropdown"] = sorted_unique(
                        database.get_data(
                            "carrier_rate_plan",
                            {"service_provider": service_provider, "is_active": True},
                            coalesce_columns=["friendly_name", "rate_plan_code"],
                        )["coalesce_column"].to_list()
                    )
                    dropdown_data["state"] = sorted(
                        [
                            "Alabama",
                            "Alaska",
                            "Arizona",
                            "Arkansas",
                            "California",
                            "Colorado",
                            "Connecticut",
                            "District of Columbia",
                            "Delaware",
                            "Florida",
                            "Georgia",
                            "Hawaii",
                            "Idaho",
                            "Illinois",
                            "Indiana",
                            "Iowa",
                            "Kansas",
                            "Kentucky",
                            "Louisiana",
                            "Maine",
                            "Maryland",
                            "Massachusetts",
                            "Michigan",
                            "Minnesota",
                            "Mississippi",
                            "Missouri",
                            "Montana",
                            "Nebraska",
                            "Nevada",
                            "New Hampshire",
                            "New Jersey",
                            "New Mexico",
                            "New York",
                            "North Carolina",
                            "North Dakota",
                            "Ohio",
                            "Oklahoma",
                            "Oregon",
                            "Pennsylvania",
                            "Rhode Island",
                            "South Carolina",
                            "South Dakota",
                            "Tennessee",
                            "Texas",
                            "Utah",
                            "Vermont",
                            "Virginia",
                            "Washington",
                            "West Virginia",
                            "Wisconsin",
                            "Wyoming",
                        ]
                    )
                    dropdown_data["public_ip"] = sorted(["Restricted", "Unrestricted"])
                    dropdown_data["reason_code"] = sorted(["General Admin/Maintenance"])

            response_data["dropdown_data"] = dropdown_data

        # Message indicating success
        message = "New Bulk change popups data sent successfully"
        response = {"flag": True, "message": message, "response_data": response_data}

        # End time calculation to measure function duration
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))

        return response

    except Exception as e:
        # Log the error and prepare an error response
        logging.exception(f"Something went wrong and error is: {e}")
        message = "Something went wrong while getting New Bulk change popups"
        # Log error to the database
        error_data = {
            "service_name": "SIM management",
            "created_date": request_received_at,
            "error_message": message,
            "error_type": str(e),
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": request_received_at,
        }
        database.log_error_to_db(error_data, "error_table")

        # Return the error response
        return {"flag": False, "message": message}


# Initialize the SNS client for sending alerts
sns_client = boto3.client("sns")


def send_sns_email(subject, message):
    """Send an email via SNS when an alert is triggered."""
    response = sns_client.publish(
        TopicArn="arn:aws:sns:us-east-1:YOUR_SNS_TOPIC_ARN",
        Message=message,
        Subject=subject,
    )
    # logging.info("SNS publish response:", response)
    return response


def bulk_change_lambda_caller(bulk_change_id="2833"):
    logging.info("bulk_change_lambda_caller function is called")
    # Assume the role from Account B
    sts_client = boto3.client("sts")
    assumed_role = sts_client.assume_role(
        RoleArn="arn:aws:iam::130265568833:role/LambdainvocationfromotherAWS",
        RoleSessionName="LambdaInvokeSession",
    )
    # Use the temporary credentials to invoke the Lambda in Account B
    credentials = assumed_role["Credentials"]
    lambda_client = boto3.client(
        "lambda",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name="us-east-1",
    )
    sqs_client = boto3.client(
        "sqs",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name="us-east-1",
    )
    # queue_url = "https://sqs.us-east-1.amazonaws.com/130265568833/DeviceBulkChange_TEST"
    queue_url = os.getenv("DEVICEBULKCHANGE_TEST", "")
    action_flag = "queue"

    message_attributes = {
        "BulkChangeId": {"StringValue": bulk_change_id, "DataType": "String"},
        "AdditionBulkChangeId": {"StringValue": "0", "DataType": "String"},
    }
    message_body = "Not used"
    try:
        if action_flag == "Lambda":
            response = lambda_client.invoke(
                FunctionName="arn:aws:lambda:us-east-1:130265568833:function:lambdascsharp",
                InvocationType="RequestResponse",
            )
            # Check if the response indicates a failure
            if response["StatusCode"] != 200:
                raise Exception(
                    f"Lambda invocation failed with status code: {response['StatusCode']}"
                )

            # logging.info("response---", response)
            return response["Payload"].read().decode("utf-8")

        elif action_flag == "queue":
            # Send the message to the SQS queue
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                DelaySeconds=0,
                MessageAttributes=message_attributes,
            )
            logging.info("Message sent to SQS queue:", response)

            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "Payload sent successfully to SQS queue",
                        "response": response,
                    }
                ),
            }
    except Exception as e:
        # Send alert if invocation fails
        subject = "Alert: Lambda Invocation Failed"
        message = f"An error occurred while invoking the Lambda function: {str(e)}"
        send_sns_email(subject, message)
        logging.info(f"Alert sent: {message}")

        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Invocation failed", "message": str(e)}),
        }


def run_db_script(data_all):
    """
    This function orchestrates the execution of a series of database operations
    related to bulk change processing. It interacts with multiple systems, such as
    the database, data transfer system, and invokes AWS Lambda for further processing.

    Parameters:
        data_all (dict): Input dictionary containing data required for bulk change processing,
                          including the bulk change ID for different steps and database name.

    Returns:
        dict: A response dictionary indicating the status of the bulk change operation.
    """
    # Log that the DB script function is invoked
    logging.info("run db script is working here")

    # Extract relevant data from the input dictionary
    data = data_all["data"]
    bulk_chnage_id_20 = data_all["bulk_chnage_id_20"]
    bulk_change_id=bulk_chnage_id_20
    #return {"bulk_change_id":bulk_change_id}

    # Get the database name from the input data or default to 'altaworx_central'
    tenant_database = data.get("db_name", "altaworx_test")

    # Establish a database connection using the provided database name and configuration
    database = DB(tenant_database, **db_config)

    try:
        # Create an instance of DataTransfer class to handle data operations
        data_transfer = DataTransfer()

        # Save data to 'bulk_change' table in the database (step 1)
        bulk_change_id_10 = data_transfer.save_data_to_10("bulk_change", data)
    except Exception as e:
        # Log an exception if an error occurs during data transfer (step 1)
        logging.exception(f"Error occurred while calling Data Transfer: {e}")

    try:
        # Initialize an empty list for bulk change ID
        bulk_change_id = []

        # Call the Lambda function to handle further processing (step 2)
        bulk_change_lambda_caller(str(bulk_change_id_10))
    except Exception as e:
        # Log warning and return failure if the Lambda invocation fails
        logging.warning(f"An error occurred while calling Lambda: {e}")
        return False

    try:
        # Log the successful invocation of the Lambda function
        logging.info(f"After lambda calling")

        # Save additional data from step 10 to step 20 in the database (step 3)
        get_data_From_10 = data_transfer.save_data_20_from_10(
            bulk_change_id_10, bulk_chnage_id_20, "bulk_change"
        )
        '''
        Updating the tables for errors and success messages
        '''
        #bulk_change_status = database.get_data('sim_management_bulk_change_request', {"bulk_change_id": bulk_change_id}, ["status"])["status"].to_list()
        bulk_change_status_query=f"select status from sim_management_bulk_change_request where bulk_change_id={bulk_chnage_id_20}"
        bulk_change_status=database.execute_query(bulk_change_status_query,True)['status'].to_list()
        print(bulk_change_status)
        # Initialize counters for 'ERROR' and 'PROCESSED'
        error_count = 0
        processed_count = 0
        failed_count=0
        # Iterate through the list and count occurrences
        for status in bulk_change_status:
            if status == "ERROR":
                error_count += 1
            elif status == "PROCESSED":
                processed_count += 1
            elif status == "API_FAILED":
                failed_count += 1

        # Check and print based on the counts
        # Check and print based on the counts
        if error_count > 0:
            database.update_dict("sim_management_bulk_change_request", {"errors":error_count}, {"bulk_change_id": bulk_chnage_id_20})
            database.update_dict("sim_management_bulk_change", {"errors":error_count}, {"id": bulk_chnage_id_20})

        if processed_count > 0:
            database.update_dict("sim_management_bulk_change_request", {"sucess":processed_count}, {"bulk_change_id": bulk_chnage_id_20})
            database.update_dict("sim_management_bulk_change", {"success":processed_count}, {"id": bulk_chnage_id_20})
        if failed_count > 0:
            database.update_dict("sim_management_bulk_change_request", {"errors":failed_count}, {"bulk_change_id": bulk_chnage_id_20})
            database.update_dict("sim_management_bulk_change", {"errors":failed_count}, {"id": bulk_chnage_id_20})
        print(failed_count,'failed_countfailed_count')

        # Return a success response indicating the bulk change process was successful
        response = {"flag": True, "message": "Bulk change lambda called successfully"}
        return response
    except Exception as e:
        # Log an exception if the data transfer to step 20 fails
        logging.exception(f"Error in Data Transfer save_data_20_from_10: {e}")

        # Return a failure response indicating the bulk change process failed
        response = {"flag": False, "message": "Bulk change lambda call failed"}
        return response


def update_bulk_change_data(data):
    """
    Updates the bulk change data for a specified module by checking user and tenant
    details, querying the database for column mappings and view names, and constructing
    a SQL query to fetch data from the appropriate view. Handles errors, logs relevant
    information, and inserts the updated data into the database.

    Parameters:
        data (dict): Input dictionary containing the necessary data, including request details,
                     changed data, session information, and tenant-related information.

    Returns:
        dict: A response dictionary indicating the status of the bulk change request and
              providing details on the inserted data.
    """

    logging.info(f"Request Data Received")

    # Extract necessary fields from the input data
    request_received_at = data.get("request_received_at", "")
    session_id = data.get("session_id", "")
    username = data.get("username", "")
    changed_data = data.get("changed_data", {})

    # Database connections for tenant and common utilities
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    dbs = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

    # Start time for performance tracking
    start_time = time.time()

    try:
        # Initialize response data and retrieve tenant-related details
        response_data = {}
        tenant_name = data.get("tenant_name", None)
        tenant_id = dbs.get_data("tenant", {"tenant_name": tenant_name}, ["id"])[
            "id"
        ].to_list()[0]

        # Retrieve change type and corresponding ID
        Change_type = data.get("change_type", None)
        logging.debug(f"Change_type is: {Change_type}")
        Change_type_id = database.get_data(
            "sim_management_bulk_change_type", {"display_name": Change_type}, ["id"]
        )["id"].to_list()[0]
        logging.debug(f"Change_type_id is: {Change_type_id}")

        # Retrieve service provider and corresponding ID
        service_provider = data.get("service_provider", None)
        service_provider_id = database.get_data(
            "serviceprovider", {"service_provider_name": service_provider}, ["id"]
        )["id"].to_list()[0]

        # User and change data fields
        modified_by = username
        created_by = username
        changed_data = data.get("changed_data", None)

        # Determine ICCIDs based on the change type
        if Change_type == "Assign Customer":
            iccids = changed_data.get("iccids", [])
        elif Change_type == "Archive":
            iccids = data.get("iccids", [])
        else:
            iccids = data.get("iccids", [])

        # Handle empty ICCID list
        uploaded = len(iccids)
        if uploaded == 0:
            iccids = [None]

        # Prepare the dictionary to create a new bulk change record
        return_dict = {}
        create_new_bulk_change_dict = {
            "service_provider": service_provider,
            "change_request_type_id": str(Change_type_id),
            "change_request_type": Change_type,
            "service_provider_id": str(service_provider_id),
            "modified_by": modified_by,
            "status": "NEW",
            "uploaded": str(uploaded),
            "is_active": "True",
            "is_deleted": "False",
            "created_by": created_by,
            "processed_by": created_by,
            "tenant_id": str(tenant_id),
        }

        return_dict["sim_management_bulk_change"] = [create_new_bulk_change_dict]

        # Insert new bulk change data into the database
        change_id = database.insert_data(
            create_new_bulk_change_dict, "sim_management_bulk_change"
        )

        # Fetch the inserted bulk change record
        bulkchange_df = database.get_data(
            "sim_management_bulk_change", {"id": change_id}
        )
        # bulkchange_id = bulkchange_df["id"].to_list()[0]
        bulkchange_id = change_id
        logging.debug(bulkchange_id, "bulkchange_id")

        # Prepare the change request data based on the change type
        change_request = changed_data
        if Change_type == "Archive":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
        elif Change_type == "Change Customer Rate Plan":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
        elif Change_type == "Update Device Status":
            change_request["UpdateStatus"] = change_request.get("UpdateStatus", "")
            change_request["Request"] = {}
        # Additional conditions for other change types
        elif Change_type == "Assign Customer":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
        elif Change_type == "Change Carrier Rate Plan":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
        elif Change_type == "Edit Username/Cost Center":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
        elif Change_type == "Change ICCID/IMEI":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
        elif Change_type == "Activate New Service":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)

        # Prepare bulk change request data for each ICCID
        create_new_bulk_change_request_dict = []
        for iccid in iccids:
            temp = {
                "iccid": iccid,
                "bulk_change_id": str(bulkchange_id),
                "tenant_name": tenant_name,
                "created_by": created_by,
                "processed_by": created_by,
                "status": "NEW",
            }
            try:
                temp["device_id"] = str(
                    database.get_data(
                        "sim_management_inventory", {"iccid": iccid}, ["id"]
                    )["id"].to_list()[0]
                )
            except:
                temp["device_id"] = None
            temp["is_active"] = "True"
            temp["is_deleted"] = "False"
            temp["change_request"] = json.dumps(change_request)
            create_new_bulk_change_request_dict.append(temp)

        # Insert bulk change request data into the database
        request_id = database.insert_data(
            create_new_bulk_change_request_dict, "sim_management_bulk_change_request"
        )
        return_dict["sim_management_bulk_change_request"] = (
            create_new_bulk_change_request_dict
        )

        # Format the bulk change record for the response
        bulk_change_row = bulkchange_df.to_dict(orient="records")[0]
        bulk_change_row["modified_date"] = str(bulk_change_row["modified_date"])
        bulk_change_row["created_date"] = str(bulk_change_row["created_date"])
        bulk_change_row["processed_date"] = str(bulk_change_row["processed_date"])

        # Prepare the success response data
        response_data = {
            "flag": True,
            "message": "Successful New Bulk Change Request Has Been Inserted",
            "bulkchange_id": bulk_change_row,
        }

        # End time for performance tracking
        end_time = time.time()
        end_time_str = f"{time.strftime('%m-%d-%Y %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
        time_consumed = f"{end_time - start_time:.4f}"

        # Log user actions for auditing
        audit_data_user_actions = {
            "service_name": "Sim Management",
            "created_date": start_time,
            "created_by": username,
            "status": str(response_data["flag"]),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": json.dumps(changed_data),
            "module_name": "update_bulk_change_data",
            "request_received_at": request_received_at,
        }
        dbs.update_audit(audit_data_user_actions, "audit_user_actions")

        # Add the data to the response
        response_data["data"] = return_dict
        response_data["bulk_chnage_id_20"] = bulkchange_id
        return response_data
    except Exception as e:
        # Log the exception and return failure response
        logging.exception(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}

        # Capture and log the error details in the database
        error_type = str(type(e).__name__)
        try:
            error_data = {
                "service_name": "Sim Management",
                "created_date": start_time,
                "error_message": message,
                "error_type": error_type,
                "users": username,
                "session_id": session_id,
                "tenant_name": tenant_name,
                "comments": "",
                "module_name": "update_bulk_change_data",
                "request_received_at": request_received_at,
            }
            dbs.log_error_to_db(error_data, "error_log_table")
        except:
            pass

        return response


def get_bulk_change_history(data):
    """
    Description: Fetches the bulk change history from the database for a given `list_view_data_id`.
    Converts date fields to a serialized string format and returns the data along with header mappings.

    Parameters:
    - data (dict): A dictionary containing the request data, including `list_view_data_id`.

    Returns:
    - response (dict): A dictionary containing a flag for success, the bulk change history data, and header mappings.
    """
    logging.info(f"Request Data Recieved")
    Partner = data.get("Partner", "")
    request_received_at = data.get("request_received_at", "")
    username = data.get("username", " ")
    session_id = data.get("session_id", " ")
    # Start time  and date calculation
    start_time = time.time()
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        # Initialize the database connection
        tenant_database = data.get("db_name", "altaworx_central")
        role_name = data.get("role_name", "")
        # database Connection
        database = DB(tenant_database, **db_config)

        # Fetch the list_view_data_id from the input data
        list_view_data_id = data.get("list_view_data_id", "")
        list_view_data_id = int(list_view_data_id)
        logging.debug(f"list_view_data_id is :{list_view_data_id}")
        # Query the database and fetch the required data
        columns = [
            "created_by",
            "iccid",
            "status",
            "processed_date",
            "processed_by",
            "created_date",
            "id",
            "change_request",
            "status_details",
        ]
        records = database.get_data(
            "sim_management_bulk_change_request",
            {"bulk_change_id": list_view_data_id},
            columns,
        )
        # Convert the DataFrame to a list of dictionaries
        sim_management_bulk_change_history_dict = records.to_dict(orient="records")

        # Define a function to serialize dates
        def serialize_dates(records):
            for record in records:
                if "processed_date" in record and record["processed_date"]:
                    record["processed_date"] = record["processed_date"].strftime(
                        "%m-%d-%Y %H:%M:%S"
                    )
                if "created_date" in record and record["created_date"]:
                    record["created_date"] = record["created_date"].strftime(
                        "%m-%d-%Y %H:%M:%S"
                    )
            return records

        # Serialize dates in all records
        sim_management_bulk_change_history_dict = serialize_dates(
            sim_management_bulk_change_history_dict
        )
        # Generate the headers mapping
        headers_map = get_headers_mappings(
            tenant_database,
            ["Bulk Change History"],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
            common_utils_database,
        )

        # # Get tenant's timezone
        tenant_name = data.get("tenant_name", "")
        tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
        tenant_timezone = common_utils_database.execute_query(tenant_timezone_query, params=[tenant_name])
        try:
            # Ensure timezone is valid
            if tenant_timezone.empty or tenant_timezone.iloc[0]["time_zone"] is None:
                logging.warning("No valid time zone found")

            tenant_time_zone = tenant_timezone.iloc[0]["time_zone"]
            match = re.search(
                r"\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)", tenant_time_zone
            )
            if match:
                tenant_time_zone = match.group(1).replace(
                    " ", ""
                )  # Ensure it's formatted correctly
        except Exception:
            logging.warning("Exception in the time zone")

        # Convert timestamps to string format before returning
        sim_management_bulk_change_history_dict = convert_timestampdata(sim_management_bulk_change_history_dict, tenant_time_zone)

        # Prepare the response
        response = {
            "flag": True,
            "status_history_data": serialize_data(sim_management_bulk_change_history_dict),
            "header_map": headers_map,
        }
        try:
            # Preparing audit data to log user actions
            # End time calculation
            end_time = time.time()
            end_time_str = f"{time.strftime('%m-%d-%Y %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
            time_consumed = f"{end_time - start_time:.4f}"

            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "fetching the bulk change  history data",
                "module_name": "get_bulk_change__history",
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while fetching the bulk change history data"
        # Error Management
        error_data = {
            "service_name": "Module_api",
            "created_date": request_received_at,
            "error_messag": message,
            "error_type": e,
            "user": username,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": message,
            "module_name": "get_status_history",
            "request_received_at": request_received_at,
        }
        common_utils_database.log_error_to_db(error_data, "error_table")
        response = {"flag": False, "error": str(e)}
        return response


def inventory_dropdowns_data(data):
    """
    Retrieves dropdown data based on the given parameters and returns a response
    with rate plan and communication plan lists.

    Parameters:
    - data (dict): Dictionary containing the dropdown type and list view data ID.

    Returns:
    - dict: A dictionary with a flag indicating success and lists of rate plans
    and communication plans.
    """
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    # Extract parameters from the input data
    dropdown = data.get("dropdown", "")
    logging.info("Retrieving service provider ID.")
    # Retrieve service provider ID based on the list view data ID
    service_provider_id = data.get("service_provider_id", "")
    logging.info(f"Service provider ID retrieved: {service_provider_id}")
    try:
        if dropdown == "Carrier Rate Plan":
            # Retrieve rate plans for the specified service provider
            rate_plan_list_data = database.get_data(
                "carrier_rate_plan",
                {
                    "service_provider_id": service_provider_id
                },  # Use the retrieved service_provider_id
                ["rate_plan_code"],
            )

            # Convert the DataFrame column to a list
            rate_plan_list = rate_plan_list_data["rate_plan_code"].to_list()
            logging.info(rate_plan_list)

            # Check if there are any rate plans to process
            if not rate_plan_list:
                logging.info("No rate plans found.")
            else:
                # Prepare the query to get all communication plans for the rate plans in one go
                placeholders = ", ".join(["%s"] * len(rate_plan_list))
                query = f"""
                SELECT carrier_rate_plan_name, communication_plan_name
                FROM public.smi_communication_plan_carrier_rate_plan
                WHERE carrier_rate_plan_name IN ({placeholders});
                """

                # Execute the query with the rate_plan parameters
                df = database.execute_query(query, params=rate_plan_list)

                # Initialize a dictionary with all rate plan keys and empty lists
                communication_plans_dict = {plan: [] for plan in rate_plan_list}

                # Check if df is a DataFrame and has results
                if isinstance(df, pd.DataFrame) and not df.empty:
                    for _, row in df.iterrows():
                        plan_name = row["carrier_rate_plan_name"]
                        comm_plan_name = row["communication_plan_name"]
                        communication_plans_dict[plan_name].append(comm_plan_name)
                else:
                    logging.info("No results found or query failed.")

                # Now you can work with the communication_plans_dict as needed
                logging.info(communication_plans_dict)
                # Prepare and return the response
                message = "Dropdown data fetched successfully"
                response = {
                    "flag": True,
                    "communication_rate_plan_list": communication_plans_dict,
                    "message": message,
                }
                return response
        else:
            try:
                service_provider_name=database.get_data('serviceprovider',{"id":service_provider_id},['service_provider_name'])['service_provider_name'].to_list()[0]
                rate_plan_query = f"select distinct rate_plan_name from customerrateplan where service_provider_name ='{service_provider_name}' and is_active=True"
                rate_plan_list_data = database.execute_query(rate_plan_query, True)
                # Convert the DataFrame column to a list
                rate_plan_list = rate_plan_list_data["rate_plan_name"].to_list()
                rate_pool_list_data = database.get_data(
                    "customer_rate_pool",
                    {
                        "service_provider_id": service_provider_id
                    },  # Use the retrieved service_provider_id
                    ["name"],
                )

                # Convert the DataFrame column to a list
                rate_pool_list = rate_pool_list_data["name"].to_list()

                # Prepare and return the response
                message = "Dropdown data fetched successfully"
                response = {
                    "flag": True,
                    "rate_pool_list": rate_pool_list,
                    "rate_plan_list": rate_plan_list,
                    "message": message,
                }
                return response

            except Exception as e:
                # Handle any exceptions that occur in the else block
                logging.exception(
                    "Error occurred while fetching rate plan or rate pool data."
                )
                message = "Error occurred while fetching rate plan or rate pool data."
                response = {
                    "flag": False,
                    "rate_pool_list": [],
                    "rate_plan_list": [],
                    "message": message,
                }
                return response
    except Exception as e:
        logging.exception(e)
        # Prepare and return the response
        message = "Dropdown data fetched successfully"
        response = {
            "flag": True,
            "communication_rate_plan_list": {},
            "message": message,
        }
        return response


def base64_encode(plain_text):
    # Convert the plain text to bytes, then encode it in Base64
    plain_text_bytes = plain_text.encode("utf-8")
    base64_encoded = base64.b64encode(plain_text_bytes).decode("utf-8")
    return base64_encoded


def base64_decode(base64_encoded_data):
    # Decode the Base64 string to bytes
    base64_encoded_bytes = base64.b64decode(base64_encoded_data)
    # Convert the bytes back to a UTF-8 string
    decoded_string = base64_encoded_bytes.decode("utf-8")
    return decoded_string


def update_username_for_iccid(
    main_url, iccid, account_custom9, api_username, api_password
):
    """
    Updates the username for a given ICCID via an HTTP PUT request.
    """
    # Encode the authorization credentials
    auth_header = base64_encode(f"{api_username}:{base64_decode(api_password)}")

    logging.info(auth_header)
    # Define URL and headers
    url = f"{main_url}/{iccid}"
    headers = {"Authorization": f"Basic {auth_header}", "Accept": "application/json"}

    # Define the request payload
    data = {"accountCustom9": account_custom9}

    # Send the request and handle the response
    try:
        response = requests.put(url, headers=headers, json=data)

        if response.status_code in [200, 202]:  # Success responses
            logging.info(f"Username for ICCID {iccid} updated successfully.")
            return {"flag": True}
        else:
            logging.info(
                f"Failed to update username for ICCID {iccid}: {response.status_code} - {response.text}"
            )
            return {
                "flag": False,
                "message": f" Failed to update username for ICCID {iccid}: {response.status_code} - {response.text} ",
            }
    except Exception as e:
        logging.info(f"Error while updating username for ICCID {iccid}: {str(e)}")
        return {"flag": False, "message": f" Failed to send message due to: {str(e)}"}


def send_revio_request(service_id, username, password, token, main_url, cost_center):
    """
    Sends a GET request to the Revio API for a specific service ID.
    """
    # Prepare authentication and headers
    auth_token = base64_encode(f"{username}:{base64_decode(password)}")
    subscription_key = base64_decode(token)
    logging.info(auth_token, subscription_key)

    url = f"{main_url}/{service_id}"
    headers = {
        "Authorization": f"Basic {auth_token}",
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Accept": "application/json",
    }

    # Send the request and handle the response
    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:

            logging.info("Request successful:", response.json())

            if "CostCenter1" in response.json():
                # Define the request payload
                data = {"CostCenter1": cost_center}

                # Send the request and handle the response
                response = requests.put(url, headers=headers, json=data)

                if response.status_code in [200, 202]:  # Success responses
                    logging.info(
                        f"CostCenter1 for ICCID {service_id} updated successfully."
                    )
                    return {"flag": True}
                else:
                    logging.info(
                        f"Failed to update CostCenter1 for ICCID {service_id}: {response.status_code} - {response.text}"
                    )
                    return {
                        "flag": False,
                        "message": f" Failed to update CostCenter1 for ICCID {service_id}: {response.status_code} - {response.text} ",
                    }

        else:
            logging.info(
                f"Failed with status code {response.status_code}: {response.text}"
            )
            return {
                "flag": False,
                "message": f"Failed with status code {response.status_code}: {response.text}",
            }
    except Exception as e:
        logging.info(f"Error during request: {str(e)}")
        return {"flag": False, "message": f" Failed to send message due to: {str(e)}"}


def send_rate_plan_update_request(
    main_url,
    token_url,
    session_url,
    service_provider,
    iccid,
    eid,
    plan_uuid,
    rate_plan,
    communication_plan,
    username,
    password,
    client_id=None,
    client_secret=None,
):
    """
    Sends a request to update the rate plan for a device based on the service provider.
    """
    headers = {"Accept": "application/json"}
    data = {}
    url = ""

    # Handle provider-specific logic
    if service_provider in ["1", "TMobile", "8", "Rogers"]:
        url = f"{main_url}/{iccid}"
        auth_header = base64_encode(f"{username}:{base64_decode(password)}")
        headers["Authorization"] = f"Basic {auth_header}"
        data = {
            "ratePlan": rate_plan,
            "communicationPlan": communication_plan,
            "iccid": iccid,
        }
    elif service_provider == "11":
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        url = f"{main_url}?requestId=AssignRatePlan_{timestamp}"
        headers.update(
            {
                "ApiKey": base64_decode(base64_encode(username)),
                "ApiSecret": base64_decode(password),
            }
        )
        data = {"entries": [{"eid": eid, "planUuid": plan_uuid}]}
    elif service_provider in ["12", "4", "5"]:
        # OAuth and session token handling
        auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        token_response = requests.post(
            token_url,
            headers={
                "Authorization": f"Basic {auth_header}",
                "Accept": "application/json",
            },
            data={"grant_type": "client_credentials"},
        )
        access_token = token_response.json().get("access_token")
        logging.info(f"access_token is {access_token}")
        session_response = requests.post(
            session_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
            json={"username": username, "password": base64_decode(password)},
        )
        logging.info(f"session_response is {session_response}")
        session_token = session_response.json().get("sessionToken")
        logging.info(f"session_token is {session_token}")
        url = main_url
        headers.update(
            {"Authorization": f"Bearer {access_token}", "VZ-M2M-Token": session_token}
        )
        data = {
            "devices": [{"deviceIds": [{"kind": "iccid", "id": iccid}]}],
            "servicePlan": rate_plan,
        }
    else:
        return True

    # Send the request and handle the response
    try:
        logging.info(f"Headers: {headers}")
        logging.info(f"Data: {data}")
        logging.info(f"URL: {url}")
        response = (
            requests.post(url, headers=headers, json=data)
            if service_provider == "11"
            else requests.put(url, headers=headers, json=data)
        )

        if response.status_code in [200, 201, 202]:
            logging.info(f"Successfully updated rate plan for ICCID {iccid}")
            return {"flag": True}
        else:
            logging.info(
                f"Failed to update rate plan for ICCID {iccid}: {response.status_code} - {response.text}"
            )
            return {
                "flag": False,
                "message": f"Failed with status code {response.status_code}: {response.text}",
            }
    except Exception as e:
        logging.info(
            f"Error sending request for service provider {service_provider}: {str(e)}"
        )
        return {"flag": False, "message": f" Failed to send message due to: {str(e)}"}


def hit_lamdba(queue_url, messagebody, messageattributes):
    """
    Sends a message to an AWS SQS queue with details to process.
    """

    try:
        # Assume role to get temporary credentials
        sts_client = boto3.client("sts")
        assumed_role = sts_client.assume_role(
            RoleArn="arn:aws:iam::130265568833:role/LambdainvocationfromotherAWS",
            RoleSessionName="LambdaInvokeSession",
        )

        # Extract temporary credentials
        credentials = assumed_role["Credentials"]

        # Create SQS client with temporary credentials
        sqs_client = boto3.client(
            "sqs",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name="us-east-1",
        )

        # Prepare the SQS request body
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=messagebody,
            DelaySeconds=0,
            MessageAttributes=messageattributes,
        )

        # Print the response from SQS
        logging.info("SQS Message Sent:", response)

        return {"flag": True}
    except Exception as e:
        logging.info(f" exception is {e}")
        return {"flag": False, "message": f" Failed to send message due to: {str(e)}"}


def Edit_Cost_Center(data):
    data["change_event_type"] = "Edit Cost Center"

    z_access_token = data.get("z_access_token", "")
    changed_data = data.get("changed_data", "")
    print(f"changed_data", changed_data)
    if z_access_token:
        if (
            not changed_data
            or len(changed_data) > 1
            or "cost_center" not in changed_data
        ):
            return {"message": "Invalid details", "status_code": 400}

    return update_inventory_data(data)


def Update_Carrier_Rate_Plan(data):
    data["change_event_type"] = "Update Carrier Rate Plan"

    z_access_token = data.get("z_access_token", "")
    changed_data = data.get("changed_data", "")
    if z_access_token:
        if not changed_data or len(changed_data) > 2:
            return {"message": "Invalid details", "status_code": 400}
        else:
            for key in changed_data:
                if key not in ["carrier_rate_plan", "communication_plan"]:
                    return {"message": "Invalid details", "status_code": 400}

    return update_inventory_data(data)


def Update_Username(data):
    data["change_event_type"] = "Update Username"

    z_access_token = data.get("z_access_token", "")
    changed_data = data.get("changed_data", "")
    if z_access_token:
        if not changed_data or len(changed_data) > 1 or "username" not in changed_data:
            return {"message": "Invalid details", "status_code": 400}

    return update_inventory_data(data)


def update_inventory_data(data):
    """
    Updates inventory data in a database and handles associated workflows like API calls and email notifications.

    Parameters:
        data (dict): Dictionary containing all required inputs for processing and updating inventory data.

    Returns:
        dict: A response indicating success or failure of the operation.

    Workflow:
    1. Validate user permissions using the `PermissionManager` class.
    2. Retrieve relevant data from the database based on input parameters.
    3. Based on `change_event_type`, perform specific actions:
        - For "Edit Cost Center", make API calls to update RevIO services.
        - For "Update Status", trigger a Lambda function for file processing.
        - For "Update Features", trigger a Lambda function for feature updates.
        - For "Update Carrier Rate Plan", send an API request to update the rate plan.
        - For "Update Username", send an API request to update the username.
    4. Log the change in the database and update history.
    5. Send email notifications for specific change event types.
    6. Audit user actions and log the time consumed for processing.
    7. Handle exceptions by logging errors in the database and returning a failure response.
    """
    logging.info(f"Request Data Recieved")
    z_access_token = data.get("z_access_token", "")
    ui_token = data.get("access_token", "")
    logging.info(f"z_access_token Recieved is {z_access_token}")
    request_received_at = data.get("request_received_at", "")
    dbs = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    carrier_api_status=data.get('carrier_api_status',False)

    if z_access_token:
        try:
            username = data.get("client_id", "")
            change_event_type = data.get("change_event_type", "")
            iccid = str(data.get("iccid", ""))
            unique_id = data.get("iccid", "")
            Partner = data.get("Partner", "")
            changed_data = data.get("changed_data", "")
            logging.info(f"changed_data Recieved is {changed_data}")

            service_account = dbs.get_data(
                "service_accounts", {"client_id": username}
            ).to_dict(orient="records")[0]
            logging.info(f"service_account  is {service_account}")
            role = service_account["role"]
            tenant = service_account["tenant"]
            table_name = "sim_management_inventory"
            tenant_database = dbs.get_data(
                "tenant", {"tenant_name": tenant}, ["db_name"]
            )["db_name"].to_list()[0]
            session_id = data.get("session_id", "client_call")
            db = DB(tenant_database, **db_config)
            service_provider = db.get_data(
                "sim_management_inventory",
                {"iccid": iccid},
                ["service_provider_display_name"],
            )["service_provider_display_name"].to_list()[0]

            history = {}
            old_data = {}
            template_name = ""
        except:
            return {"message": "Invalid details", "status_code": 400}

    else:
        Partner = data.get("Partner", "")
        session_id = data.get("session_id", "")
        username = data.get("username", "")
        changed_data = data.get("changed_data", {})
        # logging.info(changed_data, 'changed_data')
        unique_id = changed_data.get("id", None)
        old_data = data.get("old_data", {})
        ##Database connection
        role = data.get("role_name", "")
        tenant_database = data.get("db_name", "")
        template_name = data.get("template_name", "")
        table_name = data.get("table_name", "")
        iccid = old_data.get("iccid", None)
        eid = old_data.get("eid", None)
        # database Connection
        history = data.get("history", "")
        service_provider = old_data.get("service_provider_display_name", "")
        change_event_type = data.get("change_event_type", "")
        logging.info(f"Change Event Type: {change_event_type}")

        db = DB(tenant_database, **db_config)

    # Start time  and date calculation
    start_time = time.time()

    try:
        # Ensure unique_id is available
        processd_flag = False
        response_api_lambda = {}
        if unique_id is not None:
            if carrier_api_status:
                try:
                    serviceprovider_data = db.get_data(
                        "serviceprovider",
                        {"service_provider_name": service_provider},
                        ["integration_id", "id"],
                    )
                    integration_id = serviceprovider_data["integration_id"].to_list()[0]
                    service_provider_id = serviceprovider_data["id"].to_list()[0]
                    authentication_type = db.get_data(
                        "integration", {"id": integration_id}, ["authentication_type"]
                    )["authentication_type"].to_list()

                    if authentication_type and change_event_type != "Edit Cost Center":
                        authentication_type = authentication_type[0]
                        autentication_details = db.get_data(
                            "integration_authentication",
                            {
                                "authentication_type": authentication_type,
                                "integration_id": integration_id,
                            },
                        ).to_dict(orient="records")[0]
                        username_api = autentication_details["username"]
                        password = autentication_details["password"]
                        client_id = autentication_details["oauth2_client_id"]
                        client_secret = autentication_details["oauth2_client_secret"]
                        token = autentication_details["token_value"]

                    elif change_event_type == "Edit Cost Center":
                        autentication_details = db.get_data(
                            "integration_authentication",
                            {"authentication_type": 13, "integration_id": 3},
                        ).to_dict(orient="records")[0]
                        username_api = autentication_details["username"]
                        password = autentication_details["password"]
                        client_id = autentication_details["oauth2_client_id"]
                        client_secret = autentication_details["oauth2_client_secret"]
                        token = autentication_details["token_value"]
                except Exception as e:
                    logging.exception(f"Exception is {e} and carrier_api_status is {carrier_api_status}")
                    if carrier_api_status:
                        logging.exception(f"carrier_api_status is {carrier_api_status} so raising an exception here")
                        raise ValueError(f"""carrier Authentication credentials are not present for the requested details,
                                        Please contact your administrator.""")

                try:
                    service_provider_id = str(service_provider_id)
                    if change_event_type == "Edit Cost Center":

                        query = f"""select smi.device_id, smi.mobility_device_id,dt.rev_service_id as m_rev_service_id,mdt.rev_service_id as d_rev_service_id from sim_management_inventory smi
                        left join device_tenant dt on dt.device_id =smi.device_id
                        left join mobility_device_tenant mdt on mdt.mobility_device_id =smi.mobility_device_id
                        where iccid ='{iccid}'"""
                        service_id_data = db.execute_query(query, True)
                        m_rev_service_id = service_id_data["m_rev_service_id"].to_list()[0]
                        d_rev_service_id = service_id_data["d_rev_service_id"].to_list()[0]
                        main_url = dbs.get_data(
                            "inventory_actions_urls",
                            {
                                "service_provider_id": service_provider_id,
                                "action": "Edit Cost Center",
                            },
                            ["main_url"],
                        )["main_url"].to_list()[0]
                        cost_center = changed_data.get("cost_center")
                        if m_rev_service_id:
                            response_api_lambda = send_revio_request(
                                m_rev_service_id,
                                username_api,
                                password,
                                token,
                                main_url,
                                cost_center,
                            )
                            if response_api_lambda["flag"]:
                                processd_flag = True
                        if d_rev_service_id:
                            response_api_lambda = send_revio_request(
                                d_rev_service_id,
                                username_api,
                                password,
                                token,
                                main_url,
                                cost_center,
                            )
                            if response_api_lambda["flag"]:
                                processd_flag = True

                    elif change_event_type == "Update Carrier Rate Plan":

                        eid = db.get_data(
                            "sim_management_inventory", {"iccid": iccid}, ["eid"]
                        )["eid"].to_list()
                        if eid:
                            eid = eid[0]

                        carrier_rate_plan = changed_data.get("carrier_rate_plan", None)
                        communication_plan = changed_data.get("communication_plan", None)

                        plan_uuid = db.get_data(
                            "carrier_rate_plan",
                            {"rate_plan_code": carrier_rate_plan},
                            ["plan_uuid"],
                        )["plan_uuid"].to_list()[0]

                        url_df = dbs.get_data(
                            "inventory_actions_urls",
                            {
                                "service_provider_id": service_provider_id,
                                "action": change_event_type,
                            },
                            ["main_url", "token_url", "session_url"],
                        )
                        main_url = url_df["main_url"].to_list()[0]
                        token_url = url_df["token_url"].to_list()[0]
                        session_url = url_df["session_url"].to_list()[0]
                        response_api_lambda = send_rate_plan_update_request(
                            main_url,
                            token_url,
                            session_url,
                            service_provider_id,
                            iccid,
                            eid,
                            plan_uuid,
                            carrier_rate_plan,
                            communication_plan,
                            username,
                            password,
                            client_id,
                            client_secret,
                        )
                        if response_api_lambda["flag"]:
                            processd_flag = True

                    elif change_event_type == "Update Username":

                        account_custom9 = changed_data.get("username", None)
                        main_url = dbs.get_data(
                            "inventory_actions_urls",
                            {
                                "service_provider_id": service_provider_id,
                                "action": change_event_type,
                            },
                            ["main_url"],
                        )["main_url"].to_list()[0]
                        response_api_lambda = update_username_for_iccid(
                            main_url, iccid, account_custom9, username_api, password
                        )
                        if response_api_lambda["flag"]:
                            processd_flag = True

                    elif change_event_type == "Update Status":

                        queue_url = dbs.get_data(
                            "inventory_actions_urls",
                            {
                                "service_provider_id": service_provider_id,
                                "action": "Update Status",
                            },
                            ["queue_url"],
                        )["queue_url"].to_list()[0]
                        file_id = db.get_data(
                            "device_status_uploaded_file_detail",
                            {"iccid": iccid},
                            ["uploaded_file_id"],
                        )["uploaded_file_id"].to_list()[0]
                        username_api = username  # Username initiating the status change

                        MessageBody = f"File to work is {str(file_id)}"
                        MessageAttributes = {
                            "FileId": {"DataType": "String", "StringValue": str(file_id)},
                            "ModifiedBy": {"DataType": "String", "StringValue": username},
                        }

                        response_api_lambda = hit_lamdba(
                            queue_url, MessageAttributes, MessageBody
                        )
                        if response_api_lambda["flag"]:
                            processd_flag = True

                    elif change_event_type == "Update features":

                        file_id = db.get_data(
                            "device_status_uploaded_file_detail",
                            {"iccid": iccid},
                            ["uploaded_file_id"],
                        )["uploaded_file_id"].to_list()[0]
                        queue_url = dbs.get_data(
                            "inventory_actions_urls",
                            {
                                "service_provider_id": service_provider_id,
                                "action": "Update features",
                            },
                            ["queue_url"],
                        )["queue_url"].to_list()[0]

                        entity_id = ""  # Replace with the actual Entity ID
                        MessageAttributes = {
                            "MobilityLineConfigurationQueueId": {
                                "DataType": "String",
                                "StringValue": entity_id,
                            }
                        }
                        MessageBody = "Not used"
                        response_api_lambda = hit_lamdba(
                            queue_url, MessageAttributes, MessageBody
                        )
                        if response_api_lambda["flag"]:
                            processd_flag = True
                    else:
                        processd_flag = True
                except Exception as e:
                    logging.exception(f"Exception in apis and error is {e}")

            # Prepare the update data
            if not processd_flag and carrier_api_status:
                if 'message' in response_api_lambda:
                    raise ValueError(f"{response_api_lambda['message']}")
                else:
                    logging.exception(f"carrier_api_status is {carrier_api_status}  so raising an exception here")
                    raise ValueError(f"""carrier Authentication credentials are not present for the requested details,
                                    Please contact your administrator.""")

            update_data = {
                key: value
                for key, value in changed_data.items()
                if key != "unique_col" and key != "id"
            }
            # Perform the update operation
            if z_access_token:
                db.update_dict(table_name, update_data, {"iccid": unique_id})
            else:
                db.update_dict(table_name, update_data, {"id": unique_id})
            logging.info("edited successfully")
            message = f"Data Edited Successfully"

            # Example usage
            db = DB(database=tenant_database, **db_config)

            try:
                logging.info("history", history)
                # Call the method
                db.insert_data(history, "sim_management_inventory_action_history")
            except Exception as e:
                logging.exception(f"Exception is {e}")

            # Check if the change event type is "Update Status" or "Update Username"
            if change_event_type in ["Update Status", "Update Username"]:
                try:
                    # Send email notification if change_event_type matches
                    to_emails, cc_emails, subject, body, from_email, partner_name = (
                        send_email(template_name, id=unique_id, tenant_database=tenant_database  )
                    )
                    dbs.update_dict(
                        "email_templates",
                        {"last_email_triggered_at": request_received_at},
                        {"template_name": template_name},
                    )

                    # Auditing the email notifications
                    email_audit_data = {
                        "template_name": template_name,
                        "email_type": "Application",
                        "partner_name": partner_name,
                        "email_status": "success",
                        "from_email": from_email,
                        "to_email": to_emails,
                        "cc_email": cc_emails,
                        "action": "Email triggered",
                        "comments": "update inventory data",
                        "subject": subject,
                        "body": body,
                        "role": role,
                    }
                    dbs.update_audit(email_audit_data, "email_audit")
                    logging.info("Email notification audited successfully.")
                except Exception as e:
                    logging.exception(
                        f"An error occurred during email notification: {e}"
                    )

            # Preparing success response
            response_data = {
                "flag": True,
                "message": "Data Updated successfully",
                "iccid": iccid,
            }

            # Audit log
            end_time = time.time()
            time_consumed = int(float(f"{end_time - start_time:.4f}"))
            audit_data_user_actions = {
                "service_name": "Module Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response_data["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(changed_data),
                "module_name": "update_inventory_data",
                "request_received_at": request_received_at,
            }
            dbs.update_audit(audit_data_user_actions, "audit_user_actions")
            return response_data
    except Exception as e:
        logging.exception(f"Error: {e}")
        # Error response and logging
        if z_access_token:
            response = {
                "message": str(e),
                "status_code": 500,
            }
        else:
            response = {"flag": False, "message": "Unable to save the data"}
        error_type = type(e).__name__
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
                "comments": "",
                "module_name": "Module Managament",
                "request_received_at": request_received_at,
            }
            dbs.log_error_to_db(error_data, "error_log_table")
        except:
            pass
        return response

def sim_order_form_mail_trigger(data):
    """
    Description: Triggers an email with SIM order form details and inserts the order data into the database.

    Parameters:
    - data (dict): A dictionary containing the SIM order form details and other related information.

    Returns:
    - response (dict): A dictionary indicating the success or failure of the operation.
    """
    logging.info(f"Request Data Receieved")
    Partner = data.get("Partner", "")
    request_received_at = data.get("request_received_at", "")
    username = data.get("username", " ")
    module_name = data.get("module_name", None)
    session_id = data.get("session_id", None)
    template_name = data.get("template_name", "Sim Order Form")
    role = data.get("role", "")
    start_time = time.time()
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        sim_order_data = data.get("sim_order_data", {})
        logging.debug(f"Sim order data is {sim_order_data}")
        sim_order_data["sim_info"] = json.dumps(sim_order_data["sim_info"])
        sim_order_id = database.insert_data(sim_order_data, "sim_order_form")
        # Sending email
        result = send_email(template_name, username=username, data_dict=sim_order_data, tenant_database=tenant_database  )
        logging.debug(f"Mail sent successfully")
        if isinstance(result, dict) and result.get("flag") is False:
            logging.debug(f"result is {result}")
            message = "Failed to send email."
        else:
            to_emails, cc_emails, subject, body, from_email, partner_name = result
            if not to_emails or not subject:
                raise ValueError("Email template retrieval returned invalid results.")

            common_utils_database.update_dict(
                "email_templates",
                {"last_email_triggered_at": request_received_at},
                {"template_name": template_name},
            )
            query = """
                SELECT parents_module_name, sub_module_name, child_module_name, partner_name
                FROM email_templates
                WHERE template_name = %s
            """

            params = [template_name]
            # Execute the query and fetch the result
            email_template_data = common_utils_database.execute_query(
                query, params=params
            )
            if not email_template_data.empty:
                # Unpack the results
                (
                    parents_module_name,
                    sub_module_name,
                    child_module_name,
                    partner_name,
                ) = email_template_data.iloc[0]
            else:
                # If no data is found, assign default values or log an error
                parents_module_name = ""
                sub_module_name = ""
                child_module_name = ""
                partner_name = ""

            # Email audit logging
            email_audit_data = {
                "template_name": template_name,
                "email_type": "Application",
                "partner_name": partner_name,
                "email_status": "success",
                "from_email": from_email,
                "to_email": to_emails,
                "cc_email": cc_emails,
                "comments": "update sim order data",
                "subject": subject,
                "body": body,
                "role": role,
                "action": "Email triggered",
                "parents_module_name": parents_module_name,
                "sub_module_name": sub_module_name,
                "child_module_name": child_module_name,
            }
            common_utils_database.update_audit(email_audit_data, "email_audit")

        return {"flag": True, "message": "Email sent successfully"}

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        error_data = {
            "service_name": "Sim Management",
            "created_date": start_time,
            "error_messag": message,
            "error_type": str(e),
            "user": username,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": message,
            "module_name": "sim_order_form_mail_trigger",
            "request_received_at": start_time,
        }
        try:
            common_utils_database.log_error_to_db(error_data, "error_table")
        except Exception as db_error:
            logging.warning(f"Error inserting data into error_table: {db_error}")
        response = {"flag": False, "error": str(e)}
        return response


def convert_booleans(data):
    for key, value in data.items():
        if isinstance(value, str) and value.lower() == "true":
            data[key] = True
        elif isinstance(value, str) and value.lower() == "false":
            data[key] = False
        elif isinstance(value, dict):  # Recursively process nested dictionaries
            convert_booleans(value)
    return data  # Return the modified dictionary


def update_sim_management_modules_data(data):
    """
    Updates module data for a specified module by checking user and tenant to get the features,
    constructing and executing a SQL query to fetch data from the appropriate view, handles errors,
    and logs relevant information.

    Parameters:
        data (dict): A dictionary containing the data to be updated, including:
            - 'Partner' (str): The partner name.
            - 'request_received_at' (str): The timestamp when the request was received.
            - 'session_id' (str): The session ID associated with the request.
            - 'username' (str): The username of the person making the request.
            - 'module' (str): The module name (e.g., 'Feature Codes', 'Optimization Group').
            - 'changed_data' (dict): A dictionary containing the existing data that has been changed.
            - 'new_data' (dict): A dictionary containing the new data to be added or updated.
            - 'table_name' (str): The name of the database table to update.
            - 'action' (str): The action to perform, either 'create' or 'update'.
            - 'db_name' (str): The name of the tenant database.

    Returns:
        dict: A response dictionary indicating the success or failure of the operation, with:
            - 'flag' (bool): True if the operation was successful, False otherwise.
            - 'message' (str): A message indicating the outcome of the operation.
    """

    logging.info(f"Request Data Recieved")
    data = convert_booleans(data)
    Partner = data.get("Partner", "")
    request_received_at = data.get("request_received_at", "")
    session_id = data.get("session_id", "")
    username = data.get("username", "")
    module_name = data.get("module", "")
    logging.debug(module_name, "module_name")
    changed_data = data.get("changed_data", {})
    new_data = data.get("new_data", {})
    new_data = {k: v for k, v in new_data.items() if v != ""}
    # Start time  and date calculation
    start_time = time.time()
    unique_id = changed_data.get("id", None)
    logging.debug(unique_id, "unique_id")
    table_name = data.get("table_name", "")
    action = data.get("action", "")
    try:
        # if module_name == "Feature Codes":
        #     if isinstance(new_data["feature_codes"], str):
        #         try:
        #             new_data["feature_codes"] = json.loads(new_data["feature_codes"])
        #         except json.JSONDecodeError:
        #             logging.info(
        #                 "Error: feature_codes could not be converted to a list"
        #             )
        #             new_data["feature_codes"] = []

        if module_name == "Optimization Group":
            if isinstance(new_data["rate_plans_list"], str):
                try:
                    new_data["rate_plans_list"] = json.loads(
                        new_data["rate_plans_list"]
                    )
                except json.JSONDecodeError:
                    logging.info(
                        "Error: rate_plans_list could not be converted to a list"
                    )
                    new_data["rate_plans_list"] = []

        new_data["rate_plans_list"] = json.dumps(new_data["rate_plans_list"])

    except Exception as e:
        logging.warning(f"An error occurred while processing the data: {e}")

    try:
        if module_name == "Optimization Group":
            try:
                # Convert rate_plans_list from string to list if needed
                if isinstance(changed_data["rate_plans_list"], str):
                    changed_data["rate_plans_list"] = ast.literal_eval(
                        changed_data["rate_plans_list"]
                    )
                # Ensure it's a list
                if not isinstance(changed_data["rate_plans_list"], list):
                    changed_data["rate_plans_list"] = []
            except (ValueError, SyntaxError):
                logging.warning(
                    "Error: rate_plans_list could not be converted to a list"
                )
                changed_data["rate_plans_list"] = []

        # Convert carrier_rate_plans to JSON string if needed for database storage
        if module_name == "Optimization Group":
            changed_data["rate_plans_list"] = json.dumps(
                changed_data["rate_plans_list"]
            )
    except:
        pass

    if module_name == "Communication plans":
        try:
            # Convert carrier_rate_plans from string to list if needed
            if isinstance(changed_data["carrier_rate_plans"], str):
                changed_data["carrier_rate_plans"] = ast.literal_eval(
                    changed_data["carrier_rate_plans"]
                )
            # Ensure it's a list
            if not isinstance(changed_data["carrier_rate_plans"], list):
                changed_data["carrier_rate_plans"] = []
        except Exception as e:
            logging.warning(
                "Error: carrier_rate_plans could not be converted to a list"
            )
            changed_data["carrier_rate_plans"] = []

    # Convert carrier_rate_plans to JSON string if needed for database storage
    if module_name == "Communication plans":
        changed_data["carrier_rate_plans"] = json.dumps(
            changed_data["carrier_rate_plans"]
        )
    # try:
    #     if module_name == "Feature Codes":
    #         try:
    #             # Convert carrier_rate_plans from string to list if needed
    #             if isinstance(changed_data["feature_codes"], str):
    #                 changed_data["feature_codes"] = ast.literal_eval(
    #                     changed_data["feature_codes"]
    #                 )
    #             # Ensure it's a list
    #             if not isinstance(changed_data["feature_codes"], list):
    #                 changed_data["feature_codes"] = []
    #         except Exception as e:
    #             logging.warning(
    #                 "Error: carrier_rate_plans could not be converted to a list"
    #             )
    #             changed_data["feature_codes"] = []

    #     # Convert carrier_rate_plans to JSON string if needed for database storage
    #     if module_name == "Feature Codes":
    #         changed_data["feature_codes"] = json.dumps(changed_data["feature_codes"])
    # except Exception as e:
    #     logging.warning(f"Error is {e}")
    tenant_database = data.get("db_name", "")
    # database Connection
    db = DB(database=tenant_database, **db_config)
    dbs = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    # Start time  and date calculation
    start_time = time.time()
    try:
        feature_codes = new_data.get('feature_codes', '')

        if isinstance(feature_codes, str):
            try:
                # Attempt to parse the string as a JSON list
                feature_codes_list = json.loads(feature_codes)
                if isinstance(feature_codes_list, list):
                    new_data['feature_codes'] = ', '.join(feature_codes_list)
            except json.JSONDecodeError:
                # Handle the case where it's not a valid JSON string
                pass
        if isinstance(new_data.get('service_provider_name'), list):
            new_data['service_provider_name'] = ', '.join(new_data['service_provider_name'])
        if isinstance(new_data.get('automation_rule'), list):
            new_data['automation_rule'] = ', '.join(new_data['automation_rule'])
        if action == 'create':
            new_data = {k: v for k, v in new_data.items() if v is not None and v != "None"}
            insert_id=db.insert_data(new_data, table_name)
        else:
            feature_codes_changed = changed_data.get('feature_codes', '')
            if isinstance(feature_codes_changed, str):
                try:
                    # Attempt to parse the string as a JSON list
                    feature_codes_list = json.loads(feature_codes_changed)
                    if isinstance(feature_codes_list, list):
                        changed_data['feature_codes'] = ', '.join(feature_codes_list)
                except json.JSONDecodeError:
                    # Handle the case where it's not a valid JSON string
                    pass
            if isinstance(changed_data.get("service_provider_name"), list):
                # Join the list into a string, separated by commas
                changed_data["service_provider_name"] = ", ".join(
                    changed_data["service_provider_name"]
                )
            elif isinstance(changed_data.get("service_provider_name"), str):
                # If it's already a string, remove square brackets, backslashes, and double quotes
                changed_data["service_provider_name"] = (
                    changed_data["service_provider_name"]
                    .replace("[", "")
                    .replace("]", "")
                    .replace("\\", "")
                    .replace('"', "")
                )
            if isinstance(changed_data.get("automation_rule"), list):
                # Join the list into a string, separated by commas
                changed_data["automation_rule"] = ", ".join(
                    changed_data["automation_rule"]
                )
            elif isinstance(changed_data.get("automation_rule"), str):
                # If it's already a string, remove square brackets, backslashes, and double quotes
                changed_data["automation_rule"] = (
                    changed_data["automation_rule"]
                    .replace("[", "")
                    .replace("]", "")
                    .replace("\\", "")
                    .replace('"', "")
                )
            # Ensure unique_id is available
            if unique_id is not None:
                # Filter out values that are None or "None"
                changed_data = {
                    k: v
                    for k, v in changed_data.items()
                    if v is not None and v != "None"
                }

                # Prepare the update data excluding unique columns
                update_data = {
                    key: value
                    for key, value in changed_data.items()
                    if key != "unique_col" and key != "id"
                }

                # Perform the update operation
                db.update_dict(table_name, update_data, {"id": unique_id})

        logging.info("Action Done successfully")
        message = f"{action} Successfully"
        response_data = {"flag": True, "message": message}

        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        audit_data_user_actions = {
            "service_name": "Sim Management",
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
        logging.info(f"An error occurred: {e}")
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
                "comments": "",
                "module_name": "Module Managament",
                "request_received_at": request_received_at,
            }
            dbs.log_error_to_db(error_data, "error_log_table")
        except:
            pass
        return response


def format_timestamp(ts):
    # Check if the timestamp is not None
    if ts is not None:
        # Convert a Timestamp or datetime object to the desired string format
        return ts.strftime("%b %d, %Y, %I:%M %p")
    else:
        # Return a placeholder or empty string if the timestamp is None
        return " "


def optimization_dropdown_data(data):
    """
    The optimization_dropdown_data function fetches dropdown data
    for service providers, including unique customer names and their
    corresponding billing periods, based on active service providers
    from the database. It structures the response into two parts:
    service_provider_customers and service_provider_billing_periods,
    handling errors and returning appropriate fallback data in case of an issue.
    """
    logging.info("Request Data Received")
    tenant_database = data.get("db_name", "")
    try:
        # Database connection
        database = DB(tenant_database, **db_config)
    except Exception as db_exception:
        logging.error("Failed to connect to the database: %s", str(db_exception))
        return {"error": "Database connection failed.", "details": str(db_exception)}
    try:
        # List of service provider names with their IDs
        serviceproviders = database.get_data(
            "serviceprovider", {"is_active": True}, ["id", "service_provider_name"]
        )
        service_provider_list = serviceproviders.to_dict(
            orient="records"
        )  # List of dicts containing both id and service_provider_name
        service_provider_list = sorted(
            service_provider_list, key=lambda x: x["service_provider_name"]
        )
        logging.debug(f"Successfully fetched the service providers")

        # Initialize dictionaries to store separate data
        service_provider_customers = {}
        service_provider_billing_periods = {}

        # Iterate over each service provider
        for service_provider in service_provider_list:
            service_provider_id = service_provider["id"]
            service_provider_name = service_provider["service_provider_name"]

            # Get customer data (including possible duplicates)
            query_customers = f"""SELECT DISTINCT customer_name, customer_id
              FROM optimization_customer_processing where serviceprovider='{service_provider}'
              """
            customers = database.execute_query(query_customers, True)

            # Create a set to filter unique customer names
            unique_customers = set()
            customer_list = []

            # Loop through each customer and add unique ones to the list
            for row in customers.to_dict(orient="records"):
                customer_name = row["customer_name"]
                if (
                    customer_name not in unique_customers
                ):  # Check if the customer is already added
                    unique_customers.add(customer_name)
                    customer_list.append(
                        {
                            "customer_id": row["customer_id"],
                            "customer_name": customer_name,
                        }
                    )

            # Get billing period data including start date, end date, and ID
            billing_periods = database.get_data(
                "billing_period",
                {"service_provider": service_provider_name, "is_active": True},
                ["id", "billing_cycle_start_date", "billing_cycle_end_date"],
                order={"billing_cycle_end_date": "desc"},
            )

            # Check and handle missing or invalid values in the billing_cycle_end_date column
            billing_periods["billing_cycle_end_date"] = pd.to_datetime(
                billing_periods["billing_cycle_end_date"], errors="coerce"
            )
            billing_periods["billing_cycle_end_date"] = billing_periods[
                "billing_cycle_end_date"
            ].apply(
                lambda date: (
                    (date - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    if pd.notna(date) and date.time() == pd.Timestamp("00:00:00").time()
                    else date
                )
            )

            # Filter out rows with NaT in billing_cycle_end_date
            billing_periods = billing_periods.dropna(subset=["billing_cycle_end_date"])

            # Initialize a list to hold the formatted billing periods
            formatted_billing_periods = []
            for period in billing_periods.to_dict(orient="records"):
                formatted_period = {
                    "id": period["id"],
                    "billing_period_start_date": format_timestamp(
                        period["billing_cycle_start_date"]
                    ),
                    "billing_period_end_date": format_timestamp(
                        period["billing_cycle_end_date"]
                    ),
                }
                formatted_billing_periods.append(formatted_period)

            # Add the service provider's ID, customer list, and formatted
            # billing periods to the dictionary
            service_provider_customers[service_provider_name] = {
                "id": service_provider_id,
                "customers": customer_list,
            }
            service_provider_billing_periods[service_provider_name] = (
                formatted_billing_periods
            )

        # Prepare the response
        response = {
            "flag": True,
            "service_provider_customers": service_provider_customers,
            "service_provider_billing_periods": service_provider_billing_periods,
        }
        return response

    except Exception as e:
        logging.exception(f"Exception occurred while fetching the data: {e}")
        message = f"Exception: {e}"
        # Prepare the response in case of an exception
        response = {
            "flag": False,
            "service_provider_customers": {},
            "service_provider_billing_periods": {},
            "message": message,
        }
        return response

def customer_pool_row_data(data):
    """
    Fetches and processes data for a specific customer rate pool from the database.
    It calculates the total data usage, total data allocation, and the percentage usage,
    and returns the results in a structured format.

    Parameters:
        data (dict): A dictionary containing data for the operation, including:
            - 'customer_rate_pool_name' (str): The name of the customer rate pool to retrieve data for.
            - 'table_name' (str): The name of the table to query for the customer rate pool data.
            - 'db_name' (str): The name of the tenant database to query.

    Returns:
        dict: A dictionary containing:
            - 'flag' (bool): Indicates whether the operation was successful.
            - 'customer_pool_data' (dict): Contains the following keys:
                - 'Name' (str): The customer rate pool name.
                - 'Total Data Allocation MB' (float): The total data allocation in megabytes.
                - 'Total Data Usage MB' (float): The total data usage in megabytes.
                - 'Percent Usage' (str): The percentage of data usage or 'N/A' if allocation is zero.
    """
    try:
        customer_rate_pool_name = data.get("customer_rate_pool_name", "")
        table_name = data.get("table_name", "")
        pool_row_data = {}

        # Fetch data from the database
        tenant_database = data.get("db_name", "")
        # database Connection
        database = DB(tenant_database, **db_config)
        # Columns to discard
        columns_to_discard = ["carrier_cycle_usage", "customer_cycle_usage"]
        customer_rate_pool_details_df = database.get_data(
            table_name, {"customer_pool": customer_rate_pool_name}, columns_to_discard
        )
        # Rename columns dynamically based on the discarded columns
        rename_mapping = {
            columns_to_discard[0]: "Total Data Usage MB",
            columns_to_discard[1]: "Total Data Allocation MB",
        }
        customer_rate_pool_details_df.rename(columns=rename_mapping, inplace=True)
        # Convert non-numeric and None values to 0 for summing
        customer_rate_pool_details_df["Total Data Usage MB"] = pd.to_numeric(
            customer_rate_pool_details_df["Total Data Usage MB"], errors="coerce"
        ).fillna(0)

        customer_rate_pool_details_df["Total Data Allocation MB"] = pd.to_numeric(
            customer_rate_pool_details_df["Total Data Allocation MB"], errors="coerce"
        ).fillna(0)

        # Calculate the sum for each column
        total_data_usage = customer_rate_pool_details_df["Total Data Usage MB"].sum()
        total_data_allocation = customer_rate_pool_details_df[
            "Total Data Allocation MB"
        ].sum()

        # Calculate the percentage usage or set it to 'N/A'
        if total_data_allocation != 0:
            percent_usage = round((total_data_usage / total_data_allocation) * 100, 2)
            percent_usage_str = f"{percent_usage}%"
        else:
            percent_usage_str = "N/A"

        # Prepare the result dictionary
        pool_row_data["Name"] = customer_rate_pool_name
        pool_row_data["Total Data Allocation MB"] = float(total_data_allocation)
        pool_row_data["Total Data Usage MB"] = float(total_data_usage)
        pool_row_data["Percent Usage"] = percent_usage_str

        response = {"flag": True, "customer_pool_data": pool_row_data}
        return response
    except Exception as e:
        logging.exception(f"Exception is {e}")
        response = {"flag": True, "customer_pool_data": {}}
        return response


def dataframe_to_blob_bulk_upload(
    data_frame, tenant_sheet_df=None ,service_provider_sheet_df=None
):
    """
    Description: The Function is used to convert the dataframe to blob
    """
    # Create a BytesIO buffer
    bio = BytesIO()

    # Use ExcelWriter within a context manager to ensure proper saving
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        data_frame.to_excel(writer, sheet_name="Template", index=False)
        try:
            if not tenant_sheet_df.empty:
                tenant_sheet_df.to_excel(writer, sheet_name="Tenants", index=False)
        except:
            pass
        if not service_provider_sheet_df.empty:
            service_provider_sheet_df.to_excel(
                writer, sheet_name="Service Providers", index=False
            )

    # Get the value from the buffer
    bio.seek(0)
    blob_data = base64.b64encode(bio.read())
    return blob_data


def capitalize_columns(df):
    """Capitalizes the column names of the DataFrame."""
    df.columns = df.columns.str.replace("_", " ").str.capitalize()
    return df


def download_bulk_upload_template(data):
    """
    Generates a bulk upload template for a specified module and table. The function fetches
    the columns for the specified table, removes unnecessary columns based on the module type,
    and organizes the data in the desired order for a bulk upload template. It also fetches and
    formats additional tenant and service provider data, then converts the generated template
    to a binary blob for download.

    Parameters:
        data (dict): A dictionary containing the following keys:
            - 'module_name' (str): The name of the module (e.g., 'Users', 'Customer Rate Pool').
            - 'table_name' (str): The name of the table for which to generate the bulk upload template.
            - 'db_name' (str): The name of the database for the tenant (e.g., 'tenant_database').

    Returns:
        dict: A response dictionary with the following structure:
            - 'flag' (bool): Indicates whether the operation was successful.
            - 'blob' (str, optional): A string representing the binary-encoded blob data of the template if successful.
            - 'message' (str, optional): An error message if the operation failed.
    """
    try:
        logging.info("Request Data Received")
        # Get columns for the specific table
        module_name = data.get("module_name", "")
        table_name = data.get("table_name", "")
        logging.debug(f"table_name is: {table_name} and module name is {module_name}")
        tenant_database = data.get("db_name", "")

        # Database Connection
        database = DB(tenant_database, **db_config)
        common_utils_database = DB("common_utils", **db_config)

        if module_name in ("Users"):
            columns_df = common_utils_database.get_table_columns(table_name)
        else:
            columns_df = database.get_table_columns(table_name)

        # Specify columns to remove if the module name is 'Customer Rate Pool'
        if module_name == "Customer Rate Pool":
            columns_to_remove = [
                "created_by",
                "created_date",
                "deleted_by",
                "deleted_date",
                "modified_by",
                "modified_date",
                "is_deleted",
                "is_active",
                "tenant_id",
                "service_provider_ids",
                "projected_usage",
                "service_provider_name",
            ]

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[
                ~columns_df["column_name"]
                .str.lower()
                .isin([col.lower() for col in columns_to_remove])
            ]
        elif module_name == "IMEI Master Table":
            columns_to_remove = [
                "created_date",
                "modified_by",
                "modified_date",
                "deleted_by",
                "deleted_date",
                "is_active",
                "service_provider",
                "tenant_id",
            ]

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[
                ~columns_df["column_name"]
                .str.lower()
                .isin([col.lower() for col in columns_to_remove])
            ]
        elif module_name == "customer rate plan":
            columns_to_remove = [
                "created_by",
                "created_date",
                "deleted_by",
                "deleted_date",
                "modified_by",
                "modified_date",
                "is_active",
                "is_deleted",
                "serviceproviderids",
                "service_provider_id",
                "surcharge_3g",
                "base_rate_per_mb",
                "tenant_id",
                "auto_change_rate_plan",
            ]

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[
                ~columns_df["column_name"]
                .str.lower()
                .isin([col.lower() for col in columns_to_remove])
            ]

            # Define the desired column order
            desired_order = [
                "optimization_type",
                "service_provider_id",
                "rate_plan_code",
                "rate_plan_name",
                "base_rate",
                "rate_charge_amt",
                "total_rate",
                "mb_included",
                "data_per_overage_charge",
                "overage_rate_cost",
                "min_plan_mb",
                "max_plan_mb",
                "allows_sim_pooling",
                "uses_bill_in_advance",
                "sms_rate",
                "automation_rule",
                "soc_code",
                "active_inactive_status",
                "#_of_tns",
            ]

            # Normalize column names in both lists to ensure compatibility
            columns_df["column_name"] = (
                columns_df["column_name"].str.strip().str.lower()
            )
            desired_order = [col.lower() for col in desired_order]

            # Filter and reorder columns based on the desired order
            available_columns = columns_df["column_name"].values
            missing_columns = [
                col for col in desired_order if col not in available_columns
            ]

            # Add missing columns with default values (empty DataFrame columns)
            if missing_columns:
                for col in missing_columns:
                    columns_df = pd.concat(
                        [columns_df, pd.DataFrame({"column_name": [col]})],
                        ignore_index=True,
                    )

            # Reorder columns based on the desired order
            columns_df["column_name"] = pd.Categorical(
                columns_df["column_name"], categories=desired_order, ordered=True
            )
            columns_df = columns_df.sort_values("column_name")
        elif module_name == "Customer Groups":
            columns_to_remove = [
                "created_by",
                "created_date",
                "deleted_by",
                "deleted_date",
                "modified_by",
                "modified_date",
                "is_active",
                "is_deleted",
            ]

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[
                ~columns_df["column_name"]
                .str.lower()
                .isin([col.lower() for col in columns_to_remove])
            ]
        elif module_name == "Email Templates":
            columns_to_remove = [
                "created_by",
                "created_date",
                "modified_by",
                "modified_date",
                "last_email_triggered_at",
                "email_status",
                "attachments",
            ]

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[
                ~columns_df["column_name"]
                .str.lower()
                .isin([col.lower() for col in columns_to_remove])
            ]
        elif module_name == "Users":
            columns_to_remove = [
                "created_date",
                "created_by",
                "modified_date",
                "modified_by",
                "deleted_date",
                "deleted_by",
                "is_active",
                "is_deleted",
                "last_modified_by",
                "module_name",
                "module_id",
                "sub_module_name",
                "sub_module_id",
                "module_features",
                "migrated",
                "temp_password",
                "mode",
                "theme",
                "customer_group",
                "customers",
                "service_provider",
                "city",
                "access_token",
                "user_id",
            ]

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[
                ~columns_df["column_name"]
                .str.lower()
                .isin([col.lower() for col in columns_to_remove])
            ]
        # Remove the 'id' column if it exists
        columns_df = columns_df[columns_df["column_name"] != "id"]

        # Capitalize column names
        columns_df["column_name"] = (
            columns_df["column_name"].str.replace("_", " ").str.capitalize()
        )

        # Create an empty DataFrame with the column names as columns
        result_df = pd.DataFrame(columns=columns_df["column_name"].values)

        # Fetch tenant and service provider data
        if module_name != "IMEI Master Table":

            tenant_sheet_df = common_utils_database.get_data(
                "tenant", {"is_active": True}, ["id", "tenant_name"]
            )
            service_provider_sheet_df = database.get_data(
                "serviceprovider", {"is_active": True}, ["id", "service_provider_name"]
            )
            # Capitalize the column names for tenant and service provider DataFrames
            tenant_sheet_df = capitalize_columns(tenant_sheet_df)
            service_provider_sheet_df = capitalize_columns(service_provider_sheet_df)
            # Convert all DataFrames to a blob
            blob_data = dataframe_to_blob_bulk_upload(
                result_df, tenant_sheet_df, service_provider_sheet_df
            )

        else:
            #removing tenant_tab
            service_provider_sheet_df = database.get_data(
                "serviceprovider", {"is_active": True}, ["id", "service_provider_name"]
            )
            # Capitalize the column names for tenant and service provider DataFrames
            service_provider_sheet_df = capitalize_columns(service_provider_sheet_df)
            # Convert all DataFrames to a blob
            blob_data = dataframe_to_blob_bulk_upload(
                result_df, service_provider_sheet_df=service_provider_sheet_df
            )


        response = {"flag": True, "blob": blob_data.decode("utf-8")}
        return response
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {"flag": False, "message": f"Failed to download the Template !! {e}"}
        return response


def dataframe_to_blob(data_frame):
    """
    Description:The Function is used to convert the dataframe to blob
    """
    # Create a BytesIO buffer
    bio = BytesIO()

    # Use ExcelWriter within a context manager to ensure proper saving
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        data_frame.to_excel(writer, index=False)

    # Get the value from the buffer
    bio.seek(0)
    blob_data = base64.b64encode(bio.read())
    return blob_data


def bulk_import_data(data):
    """
    Handles the bulk import of data from a base64-encoded Excel file into a specified database table.
    The function validates and decodes the provided blob data, processes it into a DataFrame, checks
    column consistency between the uploaded data and the database schema, and inserts the data into
    the relevant table. If any discrepancies or errors are encountered, appropriate error messages
    are returned.

    Parameters:
        data (dict): A dictionary containing the following keys:
            - 'username' (str): The username of the person performing the import.
            - 'insert_flag' (str, optional): Indicates the method of insertion. Default is 'append'.
            - 'table_name' (str): The name of the table where data will be inserted.
            - 'db_name' (str): The name of the tenant database.
            - 'blob' (str): A base64-encoded Excel file containing the data to be imported.
            - 'request_received_at' (str): Timestamp indicating when the request was received.

    Returns:
        dict: A response dictionary with the following structure:
            - 'flag' (bool): Indicates whether the operation was successful or not.
            - 'message' (str): A message providing additional information about the result,
              such as success or specific error details.
    """
    logging.info(f"Request data Recieved")
    username = data.get("username", None)
    insert_flag = data.get("insert_flag", "append")
    table_name = data.get("table_name", "")
    created_by = data.get("username", "")
    created_date = data.get("request_received_at", "")
    # Initialize the database connection
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    # Check if blob data is provided
    blob_data = data.get("blob")
    if not blob_data:
        message = "Blob data not provided"
        response = {"flag": False, "message": message}
        return response
    try:
        # Extract and decode the blob data
        blob_data = blob_data.split(",", 1)[1]
        blob_data += "=" * (-len(blob_data) % 4)  # Padding for base64 decoding
        file_stream = BytesIO(base64.b64decode(blob_data))
        # Read the data into a DataFrame
        uploaded_dataframe = pd.read_excel(file_stream, engine="openpyxl")
        if uploaded_dataframe.empty:
            response = {
                "flag": False,
                "message": "Uploaded Excel has no data please add the data",
            }
            return response
        logging.info("Uploaded DataFrame:\n", uploaded_dataframe)
        uploaded_dataframe.columns = uploaded_dataframe.columns.str.replace(
            " ", "_"
        ).str.lower()
        uploaded_dataframe["created_by"] = created_by
        uploaded_dataframe["created_date"] = created_date
        uploaded_dataframe["action"] = "Template created"
        # Perform the insertion
        common_utils_database.insert(
            uploaded_dataframe, table_name, if_exists="append", method="multi"
        )
        message = "Upload operation is done"
        # Get and normalize DataFrame columns
        # columns_ = [col.strip().lower() for col in uploaded_dataframe.columns]
        columns_ = [
            col.strip().lower().replace(" ", "_") for col in uploaded_dataframe.columns
        ]
        logging.debug("Normalized Columns from DataFrame:", columns_)
        # Get column names from the database table
        columns_df = common_utils_database.get_table_columns(table_name)
        logging.debug("Fetched Columns from Database:\n", columns_df)
        # Remove the 'id' column if it exists
        columns_df = columns_df[columns_df["column_name"] != "id"]
        columns_to_remove = [
            "attachments",
            "modified_date",
            "modified_by",
            "email_status",
            "last_email_triggered_at",
            "reports_name",
        ]
        # Filter out the columns to remove
        columns_df = columns_df[~columns_df["column_name"].isin(columns_to_remove)]
        # Normalize database columns for comparison
        column_names = [col.strip().lower() for col in columns_df["column_name"]]
        logging.debug("Normalized Columns from Database Query:", column_names)
        # Compare the column names (ignoring order)
        if sorted(columns_) != sorted(column_names):
            logging.info("Column mismatch detected.")
            logging.info(
                "Columns in DataFrame but not in Database:",
                set(columns_) - set(column_names),
            )
            logging.info(
                "Columns in Database but not in DataFrame:",
                set(column_names) - set(columns_),
            )
            message = "Columns didn't match"
            response = {"flag": False, "message": message}
            return response
        # Return success response
        response = {"flag": True, "message": message}
        return response
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        message = f"An error occurred during the import: {str(e)}"
        response = {"flag": False, "message": message}
        return response


def customers_dropdown_inventory(data):
    """
    Fetches the customer names associated with a given service provider, either by service provider name or ID,
    depending on the specified module.

    Parameters:
        data (dict): A dictionary containing the following keys:
            - 'module_name' (str): The module name (e.g., 'SimManagement Inventory').
            - 'service_provider' (str): The name of the service provider (required if 'module_name' is not 'SimManagement Inventory').
            - 'service_provider_id' (str): The ID of the service provider (required if 'module_name' is 'SimManagement Inventory').
            - 'db_name' (str): The name of the tenant database to connect to.

    Returns:
        dict: A dictionary with the following keys:
            - 'flag' (bool): Indicates whether the operation was successful.
            - 'customer_name' (list): A list of customer names associated with the service provider.
              If no customers are found, an empty list is returned.
            - 'message' (str, optional): An error message if the operation failed (e.g., no active service provider found).
    """
    module_name = data.get("module_name", "")
    service_provider_name = data.get("service_provider", "")
    service_provider_id = data.get("service_provider_id", "6")
    # Initialize DB connection
    try:
        tenant_database = data.get("db_name", "")
        # database Connection
        database = DB(tenant_database, **db_config)
        # Get the service provider name
        if module_name == "SimManagement Inventory":
            service_provider_query = database.get_data(
                "serviceprovider",
                {"is_active": True, "id": service_provider_id},
                ["id"],
            )
            if service_provider_query.empty:
                # Handle case where no service provider was found
                return {
                    "flag": False,
                    "message": "No active service provider found with the provided name.",
                }
            # Extract the service provider name
            service_provider_id = service_provider_query["id"].to_list()[0]
            # Get the tenant_id for the current service provider
            tenant_id_query = database.get_data(
                "service_provider_tenant_configuration",
                {"service_provider_id": service_provider_id},
                ["tenant_id"],
            )
            if tenant_id_query.empty:
                # Handle case where no tenant ID was found
                return {
                    "flag": False,
                    "message": "No tenant ID found for the service provider.",
                }
            tenant_id = tenant_id_query["tenant_id"].to_list()[0]
            # Get the customer names associated with the tenant_id
            customer_names_query = database.get_data(
                "customers", {"tenant_id": str(tenant_id)}, ["customer_name"]
            )
            # If no customers are found, return an empty list
            if customer_names_query.empty:
                customer_names = []
            else:
                customer_names = customer_names_query["customer_name"].to_list()
        else:
            service_provider_query = database.get_data(
                "serviceprovider",
                {"is_active": True, "service_provider_name": service_provider_name},
                ["service_provider_name"],
            )
            if service_provider_query.empty:
                # Handle case where no service provider was found
                return {
                    "flag": False,
                    "message": "No active service provider found with the provided name.",
                }
            # Extract the service provider name
            service_provider_name = service_provider_query[
                "service_provider_name"
            ].to_list()[0]
            # Get the tenant_id for the current service provider
            tenant_id_query = database.get_data(
                "service_provider_tenant_configuration",
                {"service_provider_name": service_provider_name},
                ["tenant_id"],
            )
            if tenant_id_query.empty:
                # Handle case where no tenant ID was found
                return {
                    "flag": False,
                    "message": "No tenant ID found for the service provider.",
                }
            tenant_id = tenant_id_query["tenant_id"].to_list()[0]
            # Get the customer names associated with the tenant_id
            customer_names_query = database.get_data(
                "customers", {"tenant_id": str(tenant_id)}, ["customer_name"]
            )
            # If no customers are found, return an empty list
            if customer_names_query.empty:
                customer_names = []
            else:
                customer_names = customer_names_query["customer_name"].to_list()

        # Return the final response
        return {"flag": True, "customer_name": customer_names}
    except Exception as e:
        return {"flag": False, "customer_name": []}


def deactivate_service_product(data):
    """
    Deactivates service products for specified customers by making an API request and updating the database.
    This function validates the provided input data, calls an external API to deactivate the products,
    and updates the database with the new status of each service product.

    Parameters:
        data (dict): A dictionary containing the following keys:
            - 'username' (str): The username of the person initiating the request.
            - 'request_received_at' (str): The timestamp when the request was received.
            - 'session_id' (str): The session ID associated with the request.
            - 'Partner' (str): The name of the partner making the request.
            - 'db_name' (str): The name of the tenant database to connect to.
            - 'deactivate_data' (list): A list of dictionaries, each containing 'service_product_id', 'effective_date', and 'generate_proration' (optional).
            - 'customer_id' (list): A list of customer IDs corresponding to the service products to deactivate.

    Returns:
        dict: A dictionary with the following keys:
            - 'flag' (bool): Indicates whether the operation was successful.
            - 'data' (list): A list of results for each service product deactivation, containing the status and any error messages.
            - 'message' (str): A message indicating the success or failure of the operation.
            - 'total_selected_product' (int): The total number of service products processed.
            - 'RevCustomerId' (list): The list of customer IDs.
    """
    logging.info("Request data recieved ",data)
    z_access_token=data.get("z_access_token", "")
    start_time = time.time()
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    if z_access_token:
        try:
            username = data.get("client_id", "")
            request_received_at = datetime.now()
            service_account = database.get_data(
                "service_accounts", {"client_id": username}
            ).to_dict(orient="records")[0]
            logging.info(f"service_account  is {service_account}")
            tenant = service_account["tenant"]
            tenant_database = database.get_data(
                "tenant", {"tenant_name": tenant}, ["db_name"]
            )["db_name"].to_list()[0]
            db = DB(tenant_database, **db_config)
            deactivate_data_list = data.get("service_product_details", [])
            customer_ids = data.get("customer_ids", [])
        except Exception as e:
            print("here ",e)
            return {"message": "Invalid details", "status_code": 400}
    else:
        username = data.get("username", " ")
        # Start time  and date calculation
        request_received_at = data.get("request_received_at", " ")
        session_id = data.get("session_id", " ")
        Partner = data.get("Partner", " ")
        # Database connection
        tenant_database = data.get("db_name", "")
        # database Connection
        database = DB(tenant_database, **db_config)
        db = DB(tenant_database, **db_config)
        deactivate_data_list = data.get("deactivate_data", [])
        customer_ids = data.get("customer_id", [])

    try:
        # url = 'https://api.revioapi.com/v1/serviceProduct'
        url = os.getenv("SERVICEPRODUCT", " ")
        headers = {
            "Ocp-Apim-Subscription-Key": "04e3d452d3ba44fcabc0b7085cdde431",
            "Authorization": "Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r",
        }
        response_data = []
        success_flag = True
        message = ""
        total_selected_product = 0
        # Check that the number of customer IDs matches the number of deactivate data entries
        if len(customer_ids) != len(deactivate_data_list):
            return {"message": "Mismatch between the number of 'customer_id' and 'deactivate_data' entries.", "status_code": 400}
        # Validate that all service_product_id values are present
        for i, deactivate_data in enumerate(deactivate_data_list):
            service_product_id = deactivate_data.get("service_product_id")
            effective_date = deactivate_data.get("effective_date")
            generate_proration = deactivate_data.get("generate_proration", True)
            customer_id = str(
                customer_ids[i]
            )  # Match each service_product_id with the corresponding customer_id

            # Check if service_product_id is null or missing
            if not service_product_id:
                response_data.append(
                    {
                        "customer_id": customer_id,
                        "status": "Failed",
                        "error_message": "Service Product ID is missing or null.",
                    }
                )
                success_flag = False
                continue  # Skip to the next iteration

            # Make API request
            params = {
                "service_product_id": service_product_id,
                "effective_date": effective_date,
                "generate_proration": generate_proration,
            }

            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                response_data.append(
                    {"service_product_id": service_product_id, "status": "Success"}
                )
            else:
                response_data.append(
                    {
                        "service_product_id": service_product_id,
                        "status": "Failed",
                        "error_code": response.status_code,
                        "error_message": response.text,
                    }
                )
                success_flag = False

            # Assuming database update logic comes after API response
            update_data = {"status": "DISCONNECTED", "status_date": request_received_at}
            logging.debug(
                f"Updating the rev_Service_product table using this data{update_data}"
            )
            # Pass customer_id and service_product_id to the query conditions
            database.update_dict(
                "rev_service_product",
                update_data,
                {
                    "service_product_id": service_product_id,
                    "is_active": True,
                    "customer_id": customer_id,
                },
            )
            # Increment the count of processed service products
            total_selected_product += 1
        # Construct the response
        if success_flag:
            message = "All service products successfully deactivated."
        else:
            message = "Some service products failed to deactivate."

        if z_access_token:
            response = {
                "status_code": 200,
                "data": response_data,
                "message": message,
                "total_selected_product": total_selected_product,
                "RevCustomerId": customer_ids,
            }
        else:
            response = {
                "flag": success_flag,
                "data": response_data,
                "message": message,
                "total_selected_product": total_selected_product,
                "RevCustomerId": customer_ids,
            }
        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        try:
            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "get_module_data",
                "request_received_at": request_received_at,
            }
            db.update_audit(audit_data_user_actions, "audit_user_actions")
        except Exception as e:
            logging.warning(f"Exception is {e}")

        return response
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = f"Unable to call the api and update the data"
        if z_access_token:
            response = {"status_code": 500, "message": message}
        else:
            response = {"flag": False, "message": message}
        return response


# #  function to check for deactivated SIMs and process them
def manage_deactivated_sims():
    """
    Processes and manages SIM cards that have been deactivated for over a year. The function identifies
    SIM cards with a 'deactive' status and activation dates older than one year, performs bulk updates to
    archive them, and logs relevant actions to audit and error logs.

    Parameters:
        None: This function does not take parameters directly; it operates based on the current database
              and environment configuration.

    Returns:
        None: The function does not return any data. It performs operations such as bulk changes and logging.
    """
    logging.info("Request Data Recieved")
    # database Connection
    database = DB("altaworx_central", **db_config)
    dbs = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        current_time = time()
        current_time_dt = datetime.fromtimestamp(current_time)
        one_year_ago = current_time_dt - timedelta(days=365)
        params = [one_year_ago.strftime("%Y-%m-%d")]
        query = """
                    SELECT *
                    FROM sim_management_inventory
                    WHERE sim_status = 'deactive'
                    AND date_activated < %s
                """
        deactivated_sims = database.execute_query(query, params=params)
        if not deactivated_sims.empty:
            return
        for sim in deactivated_sims.itertuples():
            # Process each SIM
            tenant_id = sim.tenant_id
            tenant_name = dbs.get_data("tenant", {"id": tenant_id}, ["tenant_name"])[
                "tenant_name"
            ].to_list()[0]
            service_provider_id = sim.service_provider_id
            service_provider = database.get_data(
                "serviceprovider",
                {"id": service_provider_id},
                ["service_provider_name"],
            )["service_provider_name"].to_list()[0]
            change_type = "Archive"
            change_type_id = database.get_data(
                "sim_management_bulk_change_type", {"display_name": change_type}, ["id"]
            )["id"].to_list()[0]
            iccids = sim.iccid
            # Prepare bulk change request
            bulk_change_data = {
                "service_provider": service_provider,
                "change_request_type_id": int(change_type_id),
                "change_request_type": change_type,
                "service_provider_id": int(service_provider_id),
                "modified_by": "system_scheduler",
                "status": "NEW",
                "iccid": iccids,
                "uploaded": len(sim.iccid) if sim.iccid else 0,
                "is_active": True,
                "is_deleted": False,
                "created_by": "system_scheduler",
                "processed_by": "system_scheduler",
                "tenant_id": int(tenant_id),
            }
            # Insert the bulk change request into the database
            change_id = database.insert_data(
                bulk_change_data, "sim_management_bulk_change"
            )
            # Log the audit trail
            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": current_time_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "created_by": "system_scheduler",
                "status": "True",
                "time_consumed_secs": round(time() - current_time, 2),
                "session_id": "scheduler_session",
                "tenant_name": tenant_name,
                "comments": json.dumps(bulk_change_data),
                "module_name": "update_bulk_change_data",
                "request_received_at": current_time_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[
                    :-3
                ],
            }
            dbs.insert_dict(audit_data_user_actions, "audit_logs")
    except Exception as e:
        logging.info(f"An error occurred: {e}")
        message = "Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {
                "service_name": "Sim Management",
                "created_date": current_time_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "error_message": message,
                "error_type": error_type,
                "users": "system_scheduler",
                "tenant_name": tenant_name,
                "comments": "",
                "module_name": "update_bulk_change_data",
                "request_received_at": current_time_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[
                    :-3
                ],
            }
            database.insert_dict(error_data, "error_log_table")
        except Exception as log_error:
            logging.info(f"Failed to log error: {log_error}")


# Function to format dates
def format_date(date):
    if isinstance(date, pd.Timestamp):  # Check if it's a Pandas Timestamp
        return date.strftime("%m-%d-%Y %H:%M:%S")
    return date  # Return as-is if not a Timestamp


def bulk_upload_download_template(data):
    """
    Generates a template for bulk upload/download based on the specified table's columns. This function
    retrieves the columns of a given table, filters out system-specific columns (such as audit fields),
    and creates an empty DataFrame with the remaining columns. The DataFrame is then converted to a blob
    format suitable for upload/download.

    Parameters:
        data (dict): A dictionary containing the following keys:
            - 'module_name' (str): The name of the module (optional).
            - 'table_name' (str): The name of the table to generate the template for.
            - 'db_name' (str): The name of the tenant database to connect to.

    Returns:
        dict: A dictionary containing:
            - 'flag' (bool): A flag indicating whether the operation was successful (True).
            - 'blob' (str): The DataFrame data in a blob format (UTF-8 encoded string).
    """
    try:
        # Get columns for the specific table
        table_name = data.get("table_name", "")

        tenant_database = data.get("db_name", "")
        # database Connection
        database = DB(tenant_database, **db_config)
        columns_df = database.get_table_columns(table_name)
        columns_to_remove = [
            "created_by",
            "created_date",
            "modified_by",
            "modified_date",
            "last_email_triggered_at" "email_status",
            "attachments",
        ]
        columns_df = columns_df[
            ~columns_df["column_name"]
            .str.lower()
            .isin([col.lower() for col in columns_to_remove])
        ]
        # Remove the 'id' column if it exists
        columns_df = columns_df[columns_df["column_name"] != "id"]
        # Filter out the columns to remove
        columns_df["column_name"] = (
            columns_df["column_name"].str.replace("_", " ").str.capitalize()
        )

        # Create an empty DataFrame with the column names as columns
        result_df = pd.DataFrame(columns=columns_df["column_name"].values)
        # return result_df
        blob_data = dataframe_to_blob(result_df)
        response = {"flag": True, "blob": blob_data.decode("utf-8")}
    except Exception as e:
        logging.error("error here", e)

    return response


def determine_nsdev(value):
    if value in ["yes", "Yes", "YES"]:
        return True
    elif value in ["no", "No", "NO"]:
        return False
    else:
        return True  # Default value


def import_bulk_data(data):
    """
    Imports bulk data from a Base64-encoded Excel file into a database.

    Parameters:
    data (dict): A dictionary containing the following keys:
        - 'username': The username of the person initiating the import.
        - 'table_name': The name of the table into which data will be inserted.
        - 'created_date': The creation date of the record.
        - 'module_name': The module type for which the data is being imported (e.g., 'Customer Rate Pool', 'IMEI Master Table').
        - 'blob_data': The Base64-encoded string of the Excel file containing the data to be imported.

    Returns:
    dict: A dictionary containing the result of the import operation:
        - 'status': The status of the operation ('success' or 'failure').
        - 'message': A descriptive message about the result.
        - 'error_details': (optional) If the operation fails, contains details about the error.
    """
    username = data.get("username", None)
    table_name = data.get("table_name", "")
    created_date = data.get("request_received_at", "")
    module_name = data.get("module_name", "")
    # Initialize the database connection

    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    # Check if blob data is provided
    blob_data = data.get("blob")
    if not blob_data:
        message = "Blob data not provided"
        response = {"flag": False, "message": message}
        return response
    try:
        # Extract and decode the blob data
        blob_data = blob_data.split(",", 1)[1]
        blob_data += "=" * (-len(blob_data) % 4)  # Padding for base64 decoding
        file_stream = BytesIO(base64.b64decode(blob_data))

        # Read the data into a DataFrame
        uploaded_dataframe = pd.read_excel(file_stream, engine="openpyxl")
        if uploaded_dataframe.empty:
            response = {
                "flag": False,
                "message": "Uploaded Excel has no data please add the data",
            }
            return response
        uploaded_dataframe.columns = uploaded_dataframe.columns.str.replace(
            " ", "_"
        ).str.lower()
        logging.info("Uploaded DataFrame:\n", uploaded_dataframe)
        if module_name == "Customer Rate Pool":
            # Add necessary columns
            uploaded_dataframe["created_by"] = username
            uploaded_dataframe["created_date"] = created_date
            uploaded_dataframe["modified_by"] = username  # Using username
            uploaded_dataframe["modified_date"] = created_date  # Today's date
            uploaded_dataframe["is_deleted"] = False  # Default value
            uploaded_dataframe["is_active"] = True  # Default value
            uploaded_dataframe["deleted_by"] = None  # Default value
            uploaded_dataframe["deleted_date"] = None  # Default value
            uploaded_dataframe["tenant_id"] = None
            uploaded_dataframe["service_provider_ids"] = None
            uploaded_dataframe["projected_usage"] = ""
            uploaded_dataframe["tenant_id"] = None
            # uploaded_dataframe['service_provider_name'] = None

            # Fetch `service_provider_id` and `service_provider_name` mapping from `customer_rate_pool`
            query = """
            SELECT id AS service_provider_id, service_provider_name
            FROM serviceprovider WHERE is_active = 'true'
            """
            service_provider_data = database.execute_query(query, True)

            # Check if the query result is iterable (list or DataFrame)
            # Check if the query result is iterable (list or DataFrame)
            if isinstance(service_provider_data, list):
                # Create the mapping for a list of tuples
                service_provider_map = {row[0]: row[1] for row in service_provider_data}
            elif isinstance(service_provider_data, pd.DataFrame):
                # Create the mapping for a DataFrame result
                service_provider_map = dict(
                    zip(
                        service_provider_data["service_provider_id"],
                        service_provider_data["service_provider_name"],
                    )
                )
            else:
                print(
                    "Error: Expected query result to be iterable (list or DataFrame), but got:",
                    type(service_provider_data),
                )
                message = "Failed to fetch service provider data"
                response = {"flag": False, "message": message}
                return response

            # Map `service_provider_id` to `service_provider_name` using the map created
            if "service_provider_id" in uploaded_dataframe.columns:
                uploaded_dataframe["service_provider_name"] = uploaded_dataframe[
                    "service_provider_id"
                ].map(service_provider_map)
            else:
                # If no `service_provider_id` in uploaded data, set `service_provider_name` to None
                uploaded_dataframe["service_provider_name"] = None

        elif module_name == "IMEI Master Table":
            uploaded_dataframe["created_date"] = created_date
            uploaded_dataframe["modified_by"] = username  # Using username
            uploaded_dataframe["modified_date"] = created_date  # Today's date
            uploaded_dataframe["is_active"] = True  # Default value
            uploaded_dataframe["deleted_by"] = ""  # Default value
            uploaded_dataframe["deleted_date"] = created_date  # Default value
            uploaded_dataframe["volte_capable"] = uploaded_dataframe[
                "volte_capable"
            ].apply(determine_nsdev)
            uploaded_dataframe["att_certified"] = uploaded_dataframe[
                "att_certified"
            ].apply(determine_nsdev)
            uploaded_dataframe["nsdev"] = uploaded_dataframe["nsdev"].apply(
                determine_nsdev
            )
            uploaded_dataframe["tenant_id"] = None

            uploaded_dataframe["service_provider"] = 1  # Default value
        elif module_name == "Customer Groups":
            uploaded_dataframe["created_by"] = username
            uploaded_dataframe["modified_by"] = username  # Using username
            uploaded_dataframe["modified_date"] = created_date  # Today's date
            uploaded_dataframe["is_active"] = True  # Default value
            uploaded_dataframe["is_deleted"] = False  # Default value
            uploaded_dataframe["deleted_by"] = ""  # Default value
            uploaded_dataframe["deleted_date"] = created_date  # Default value
            uploaded_dataframe["created_date"] = created_date  # Default value
        elif module_name == "Users":
            uploaded_dataframe["created_date"] = created_date
            uploaded_dataframe["created_by"] = username
            uploaded_dataframe["modified_date"] = created_date
            uploaded_dataframe["modified_by"] = username
            uploaded_dataframe["deleted_date"] = created_date
            uploaded_dataframe["deleted_by"] = username
            uploaded_dataframe["is_active"] = True
            uploaded_dataframe["is_deleted"] = False
            uploaded_dataframe["migrated"] = False

        elif module_name == "customer rate plan":
            rename_mapping = {
                "uses_bill_in_advance": "is_billing_advance_eligible",
                "max_plan_mb": "max_plan_data_mb",
                "min_plan_mb": "min_plan_data_mb",
                "mb_included": "plan_mb",
                "total_rate": "display_rate",
                "#_of_tns": "no_tns",
            }
            # Rename the columns in the DataFrame
            uploaded_dataframe.rename(columns=rename_mapping, inplace=True)
            uploaded_dataframe["created_by"] = username
            uploaded_dataframe["created_date"] = created_date
            uploaded_dataframe["modified_by"] = username  # Using username
            uploaded_dataframe["modified_date"] = created_date  # Today's date
            uploaded_dataframe["is_deleted"] = False  # Default value
            uploaded_dataframe["is_active"] = True  # Default value
            uploaded_dataframe["deleted_by"] = ""  # Default value
            uploaded_dataframe["deleted_date"] = created_date  # Default value
            # Add necessary columns
            # Default value
            uploaded_dataframe["service_provider_name"] = None
            uploaded_dataframe["serviceproviderids"] = None
            uploaded_dataframe["surcharge_3g"] = None
            uploaded_dataframe["base_rate_per_mb"] = None
            uploaded_dataframe["tenant_id"] = None
            uploaded_dataframe["auto_change_rate_plan"] = None
        if module_name in ("Users"):
            logging.info(
                "Inserting data into common_utils_database table: %s", table_name
            )
            common_utils_database.insert(
                uploaded_dataframe, table_name, if_exists="append", method="multi"
            )
        if module_name == "Customer Rate Pool":
            data_json = uploaded_dataframe.to_json(orient='records')

            data_list = json.loads(data_json)
            for record in data_list:
                if 'projected_usage' in record:
                    del record['projected_usage']
            database.insert_data(data_list, table_name)
        else:
            database.insert(uploaded_dataframe, table_name, if_exists='append', method='multi')
        message = "Upload operation is done"
        if module_name == "Users":
            uploaded_dataframe["last_modified_by"] = username
            uploaded_dataframe["module_name"] = username
            uploaded_dataframe["module_id"] = username
            uploaded_dataframe["sub_module_name"] = username
            uploaded_dataframe["sub_module_id"] = 1
            uploaded_dataframe["module_features"] = ""
            uploaded_dataframe["temp_password"] = ""
            uploaded_dataframe["mode"] = ""
            uploaded_dataframe["theme"] = ""
            uploaded_dataframe["customer_group"] = ""
            uploaded_dataframe["customers"] = ""
            uploaded_dataframe["service_provider"] = ""
            uploaded_dataframe["city"] = ""
            uploaded_dataframe["access_token"] = ""
            uploaded_dataframe["user_id"] = ""
        # Get and normalize DataFrame columns
        # columns_ = [col.strip().lower() for col in uploaded_dataframe.columns]
        columns_ = [
            col.strip().lower().replace(" ", "_") for col in uploaded_dataframe.columns
        ]

        logging.info("Normalized Columns from DataFrame:", columns_)

        # Get column names from the database table
        columns_df = database.get_table_columns(table_name)
        logging.info("Fetched Columns from Database:\n", columns_df)

        # Remove the 'id' column if it exists
        columns_df = columns_df[columns_df["column_name"] != "id"]

        # Normalize database columns for comparison
        column_names = [col.strip().lower() for col in columns_df["column_name"]]
        logging.info("Normalized Columns from Database Query:", column_names)

        # Compare the column names (ignoring order)
        if sorted(columns_) != sorted(column_names):
            logging.info("Column mismatch detected.")
            logging.info(
                "Columns in DataFrame but not in Database:",
                set(columns_) - set(column_names),
            )
            logging.info(
                "Columns in Database but not in DataFrame:",
                set(column_names) - set(columns_),
            )
            message = "Columns didn't match"
            response = {"flag": False, "message": message}
            return response

        # Return success response
        response = {"flag": True, "message": message}
        return response
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        message = f"An error occurred during the import: {str(e)}"
        response = {"flag": False, "message": message}
        return response



def get_inventory_data(data):
    """
    Retrieves the optimization data.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_params' for querying the status history.

    Returns:
    - dict: A dictionary containing the List view data, header mapping, and a success message or an error message.
    """
    logging.info("Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get("tenant_name", "")
    role_name = data.get("role_name", "")
    session_id = data.get("session_id", "")
    request_received_at = data.get("request_received_at", "")
    username = data.get("username", " ")
    table = data.get("table", "sim_management_inventory")
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": Partner}, ["id"]
        )["id"].to_list()[0]
        # Initialize the database connection
        tenant_database = data.get("db_name", "")
        # database Connection
        database = DB(tenant_database, **db_config)
        tenant_database = data.get("db_name", "altaworx_central")
        inventory_data = []
        pages = {}
        if "mod_pages" in data:
            start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
            end = data["mod_pages"].get("end") or 100  # Default to 100 if no value
            logging.debug(f"starting page is {start} and ending page is {end}")
            limit = data.get("limit", 100)
            # Calculate pages
            pages["start"] = start
            pages["end"] = end
            count_params = [table]
            count_query = (
                f"SELECT COUNT(*) FROM %s where is_active=True and tenant_id=1"
                % table
            )
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            pages["total"] = int(count_result)

        params = [start, limit]
        query = f"""
            SELECT id,
                service_provider_display_name,service_provider_id,
                TO_CHAR(date_added, 'MM-DD-YYYY HH24:MI:SS') AS date_added,
                iccid,
                msisdn,
                eid,
                customer_name,
                username,
                carrier_cycle_usage_mb,
                customer_cycle_usage_mb,
                customer_rate_pool_name,
                customer_rate_plan_name,soc,cost_center,
                carrier_rate_plan_name,
                sim_status,
                TO_CHAR(date_activated, 'MM-DD-YYYY HH24:MI:SS') AS date_activated,
                ip_address,
                billing_account_number,
                foundation_account_number,
                modified_by,
                TO_CHAR(modified_date, 'MM-DD-YYYY HH24:MI:SS') AS modified_date,
                TO_CHAR(last_usage_date, 'MM-DD-YYYY HH24:MI:SS') AS last_usage_date
                FROM
                public.sim_management_inventory where is_active=True and tenant_id=1
            ORDER BY
                modified_date DESC nulls last
           OFFSET %s LIMIT %s ;
        """
        inventory_data = database.execute_query(query, params=params).to_dict(
            orient="records"
        )
        # Generate the headers mapping
        headers_map = get_headers_mappings(
            tenant_database,
            ["SimManagement Inventory"],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
            common_utils_database,
        )
        # Prepare the response
        response = {
            "flag": True,
            "inventory_data": inventory_data,
            "header_map": headers_map,
            "pages": pages,
        }
        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {
                "service_name": "Sim Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "tenant_name": Partner,
                "session_id":session_id,
                "comments": "fetching the optimization data",
                "module_name": "get_inventory_data",
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        try:
            # Error Management
            error_data = {
                "service_name": "Sim management",
                "created_date": start_time,
                "error_messag": message,
                "error_type": e,
                "user": username,
                "session_id":session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "get_inventory_data",
                "request_received_at": start_time,
            }
            common_utils_database.log_error_to_db(error_data, "error_table")
        except Exception as e:
            logging.warning(f"Exception at updating the error table")
        # Generate the headers mapping
        headers_map = get_headers_mappings(
            tenant_database,
            ["SimManagement Inventory"],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
            common_utils_database,
        )
        # Prepare the response
        response = {
            "flag": True,
            "inventory_data": [],
            "header_map": headers_map,
            "pages": {},
        }
        return response


def update_features_pop_up_data(data):
    # Initialize the database connection
    tenant_database = data.get("db_name", "")
    service_provider_id = data.get("service_provider_id", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        soc_codes = database.get_data(
            "mobility_feature",
            {"service_provider_id": service_provider_id, "is_active": True},
            ["soc_code", "friendly_name"],
        ).to_dict(orient="records")
        response = {
            "flag": True,
            "soc_codes": soc_codes,
            "message": "soc codes retrieved successfully",
        }
        return response
    except Exception as e:
        logging.exception(f"Error occured while getting soc codes")
        response = {
            "flag": True,
            "soc_codes": {},
            "message": "soc codes retrieved failed",
        }
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


def statuses_inventory(data):
    logging.info("Request Data recieved")
    service_provider_id = data.get("service_provider_id", "")
    tenant_database = data.get("db_name", "altaworx_central")
    database = DB(tenant_database, **db_config)
    try:
        integration_id = database.get_data(
            "serviceprovider", {"id": service_provider_id}, ["integration_id"]
        )["integration_id"].to_list()[0]
        statuses = database.get_data(
            "device_status",
            {"integration_id": integration_id, "is_active": True, "is_deleted": False},
            ["display_name"],
        )["display_name"].to_list()
        response = {"flag": True, "update_status_values": statuses}
        return response
    except Exception as e:
        logging.exception(f"Exception while fetching statuses")
        response = {"flag": True, "update_status_values": []}
        return response


def carrier_rate_plan_list_view_(data):
    """
    Retrieves and returns a list of carrier rate plans from the database, with pagination,
    additional data such as service provider counts, and optimization group details.
    """
    logging.info("Starting carrier_rate_plan_list_view function")
    database = DB("common_utils", **db_config)

    try:
        username = data.get("client_id", "")
        service_account = database.get_data(
            "service_accounts", {"client_id": username}
        ).to_dict(orient="records")[0]
        logging.info(f"service_account  is {service_account}")
        tenant = service_account["tenant"]
        tenant_database = database.get_data(
            "tenant", {"tenant_name": tenant}, ["db_name"]
        )["db_name"].to_list()[0]
        db = DB(tenant_database, **db_config)
    except:
        return {"message": "Invalid details", "status_code": 400}

    # Set up pagination defaults
    start_page = data.get("mod_pages", {}).get("start", 0)
    limit = 100  # Set the limit to 100

    try:
        # Main data query
        query = """
            SELECT
                service_provider,
                rate_plan_code,
                rate_plan_short_name,
				friendly_name,
                device_type,
                base_rate,
                overage_rate_cost,
                plan_mb,
                data_per_overage_charge,
                allows_sim_pooling,
                is_retired
            FROM carrier_rate_plan
            WHERE
              friendly_name IS NOT NULL and is_active = 'true'
            ORDER BY modified_date DESC
            LIMIT %s OFFSET %s;
        """
        result = db.execute_query(query, params=[limit, start_page])
        df_dict=result.to_dict(orient='records')

        return {
            "message": "Successfully retrieved the carrier rate plan list.",
            "status_code": 200,
            "carrier_rate_plan_data": df_dict
        }

    except Exception as e:
        logging.exception("An error occurred in carrier_rate_plan_list_view:", e)
        return {
            "message": "Failed to fetch the carrier rate plan list.",
            "status_code": 500
        }



def carrier_rate_plan_list_view(data):
    """
    Retrieves and returns a list of carrier rate plans from the database, with pagination,
    additional data such as service provider counts, and optimization group details.

    This function validates the provided access token, fetches data for carrier rate plans,
    and includes additional metadata like service provider occurrences, headers mapping,
    and optimization group names. It handles pagination and error logging.

    Parameters:
    data (dict): A dictionary containing the following keys:
        - 'role_name' (str): The role of the user making the request.
        - 'module_name' (str): The module name for which data is being fetched.
        - 'db_name' (str): The tenant database name (defaults to 'altaworx_central').
        - 'mod_pages' (dict): Pagination information including 'start' and 'end' values.
        - 'tenant_name' (str): The name of the tenant for fetching tenant-specific information.

    Returns:
    dict: A dictionary with the following keys:
        - 'flag' (bool): Indicates the success or failure of the data fetch operation.
        - 'message' (str): A message describing the outcome of the operation.
        - 'data' (dict): Contains the fetched carrier rate plan data in the 'carrier_rate_plan' key.
        - 'headers_map' (dict): A dictionary of header mappings based on the tenant's configuration.
        - 'pages' (dict): Pagination information, including 'start', 'end', and 'total' count.
        - 'default_optimization_group_id' (list): A list of active optimization rate plan types.
        - 'optimization_rate_plan_type_id' (list): A list of active optimization group names.

    """
    logging.info("Starting carrier_rate_plan_list_view function")

    role_name = data.get("role_name", "")
    module_name = data.get("module_name", "")
    tenant_database = data.get("db_name", "altaworx_central")
    database = DB("common_utils", **db_config)

    # Set up pagination defaults
    start_page = data.get("mod_pages", {}).get("start", 0)
    end_page = data.get("mod_pages", {}).get("end", 100)
    limit = 100  # Set the limit to 100
    return_json_data = {}

    try:
        # Connect to the tenant database
        dbs = DB(tenant_database, **db_config)

        # # Get tenant's timezone
        tenant_name = data.get("tenant_name", "")
        tenant_timezone_query = (
            """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
        )
        tenant_timezone = database.execute_query(
            tenant_timezone_query, params=[tenant_name]
        )

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

        # Main data query
        query = """
            SELECT
                id,
                service_provider,rate_plan_code,
                rate_plan_short_name,
				friendly_name,
                device_type,
                base_rate,
                overage_rate_cost,
                plan_mb,
                data_per_overage_charge,
                allows_sim_pooling,
                is_retired,
                modified_by,
                TO_CHAR(modified_date::date, 'YYYY-MM-DD') AS modified_date,
                optimization_rate_plan_type_id,
                default_optimization_group_id
            FROM carrier_rate_plan
            WHERE
              friendly_name IS NOT NULL and is_active = 'true'
            ORDER BY modified_date DESC
            LIMIT %s OFFSET %s;
        """
        result = dbs.execute_query(query, params=[limit, start_page])

        # Count query for pagination
        count_query = "SELECT COUNT(*) AS total_count FROM carrier_rate_plan WHERE friendly_name IS NOT NULL and is_active = 'true'"
        total_count_result = dbs.execute_query(count_query, True)
        total_count = (
            total_count_result["total_count"].iloc[0]
            if not total_count_result.empty
            else 0
        )

        # Additional query to count occurrences of each service_provider
        provider_count_query = """
            SELECT service_provider, COUNT(*) as provider_count
            FROM carrier_rate_plan
            GROUP BY service_provider
        """
        provider_count_result = dbs.execute_query(provider_count_query, True)

        # Create a dictionary to check service_provider occurrences
        provider_counts = provider_count_result.set_index("service_provider")[
            "provider_count"
        ].to_dict()

        rate_plan_type_list = dbs.get_data(
            "optimization_rate_plan_type", {"is_active": True}, ["rate_plan_type_name"]
        )["rate_plan_type_name"].to_list()
        optimization_group_name_list = dbs.get_data(
            "optimization_group", {"is_active": True}, ["optimization_group_name"]
        )["optimization_group_name"].to_list()

        # Get headers mapping
        headers_map = get_headers_mappings(
            tenant_database, ["Rate Plan Socs"], role_name, "", "", "", "", data,database
        )

        # Check if result is empty
        if result.empty:
            logging.info("No data found for carrier_rate_plan")
            return {
                "flag": True,
                "data": [],
                "message": "Failed to get the rate plan socs data ",
                "headers_map": headers_map,
                "pages": {"start": start_page, "end": end_page, "total": int(total_count)},
            }

        # Convert and format data, add new column for provider occurrence
        df_dict = result.to_dict(orient="records")
        df_dict = convert_timestamp_data(df_dict, tenant_time_zone)
        # df_dict = convert_timestamp(df_dict)



        pages = {"start": start_page, "end": end_page, "total": int(total_count)}

        # Successful response
        logging.info("Returning data response")
        return {
            "flag": True,
            "message": "Data fetched successfully",
            "data": {"carrier_rate_plan": serialize_data(df_dict),
                    "optimization_rate_plan_type_id": optimization_group_name_list,
                    "default_optimization_group_id": rate_plan_type_list,},
            "default_optimization_group_id": rate_plan_type_list,
            "optimization_rate_plan_type_id": optimization_group_name_list,
            "headers_map": headers_map,
            "pages": pages,
        }

    except Exception as e:
        logging.exception("An error occurred in carrier_rate_plan_list_view:", e)
        headers_map = get_headers_mappings(
            tenant_database, [module_name], role_name, "", "", "", "", data,database
        )
        return {
            "flag": True,
            "message": "Data fetch failed",
            "headers_map": headers_map,
            "pages": {},
        }


def rate_plans_by_customer_count(data):
    common_utils_database = DB('common_utils', **db_config)
    database = DB('altaworx_test', **db_config)
    logging.info(f"Request Data Recieved")
    ServiceProviderId = data.get("ServiceProviderId", "")
    BillingPeriodId = data.get("BillingPeriodId", "")
    tenant_name = data.get("tenant_name", "")
    TenantId = common_utils_database.get_data(
        "tenant", {"tenant_name": "Altaworx"}, ["id"]
    )["id"].to_list()[0]
    customer_name = data.get("customer_name", "")
    # Fetch integration_id and portal_id using database queries
    integration_id = database.get_data(
        "serviceprovider", {"id": ServiceProviderId}, ["integration_id"]
    )["integration_id"].to_list()[0]
    portal_id = database.get_data(
        "integration", {"id": integration_id}, ["portal_type_id"]
    )["portal_type_id"].to_list()[0]
    print(portal_id,'portal_id')
    try:
        rev_status=common_utils_database.get_data('module',{"module_name":"Rev.IO Customers"},["is_active"])["is_active"].to_list()[0]
        if rev_status==True:
            SiteType=1
        else:
            SiteType=0
    except Exception as e:
        logging.error(f"Exception while fetching the sitetype")
        SiteType=1
    # If portal_id is 0, proceed with M2M connection and stored procedure execution
    if portal_id == 0:
        print(portal_id,'entered here in portal')
        # Define database connection parameters
        server = "altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com"
        database_name = "AltaworxCentral_Test"
        username = "ALGONOX-Vyshnavi"
        password = "cs!Vtqe49gM32FDi"
        if customer_name=="All Customers":
            AMOPCustomerIds=None
            RevCustomerIds = None
            #SiteType = 1
        else:
            rev_customer_data = database.get_data(
                "customers", {"customer_name": customer_name}, ["rev_customer_id"]
            )["rev_customer_id"].to_list()
            # Check if rev_customer_id data is available
            if rev_customer_data:
                RevCustomerIds = ",".join([str(id) for id in rev_customer_data])
                AMOPCustomerIds = ""
                #SiteType = 1
            else:
                # If rev_customer_id is None or empty, get customer_id data
                AMOPCustomerIds = ",".join(
                    [
                        str(id)
                        for id in database.get_data(
                            "customers", {"customer_name": customer_name}, ["customer_id"]
                        )["customer_id"].to_list()
                    ]
                )
                RevCustomerIds = ""
                #SiteType = 0
        # Try connecting to the database and executing the stored procedure
        try:
            with pytds.connect(
                server=server, database=database_name, user=username, password=password
            ) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    stored_procedure_name = "AltaworxCentral_Test.dbo.usp_OptimizationRatePlansByCustomerCount"

                    # Execute the stored procedure
                    cursor.callproc(
                        stored_procedure_name,
                        (
                            RevCustomerIds,
                            ServiceProviderId,
                            TenantId,
                            SiteType,
                            AMOPCustomerIds,
                            BillingPeriodId,
                        ),
                    )
                    print("successfully hit happened")
                    # Fetch and return the results
                    results = cursor.fetchall()
                    return  results

        except Exception as e:
            logging.exception("Error in connection or execution:", e)
            return None
    elif portal_id == 2:
        print("entered here in portal type 2")
        # Define database connection parameters
        server = "altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com"
        database_name = "AltaworxCentral_Test"
        username = "ALGONOX-Vyshnavi"
        password = "cs!Vtqe49gM32FDi"
        if customer_name !="All Customers":
            rev_customer_data = database.get_data(
                "customers", {"customer_name": customer_name}, ["rev_customer_id"]
            )["rev_customer_id"].to_list()
        # working for mobility rate plan count
        # Define your connection parameters
        with pytds.connect(
            server=server, database=database, user=username, password=password
        ) as conn:
            with conn.cursor() as cursor:
                stored_procedure_name = (
                    "dbo.usp_OptimizationMobilityRatePlansByCustomerCount"
                )
                if customer_name=="All Customers":
                    AMOPCustomerIds=None
                    RevCustomerIds = None
                    #SiteType = 1
                else:
                    # Check if rev_customer_id data is available
                    if rev_customer_data:
                        RevCustomerIds = ",".join([str(id) for id in rev_customer_data])
                        AMOPCustomerIds = ""
                        #SiteType = 1
                    else:
                        # If rev_customer_id is None or empty, get customer_id data
                        AMOPCustomerIds = ",".join(
                            [
                                str(id)
                                for id in database.get_data(
                                    "customers",
                                    {"customer_name": customer_name},
                                    ["customer_id"],
                                )["customer_id"].to_list()
                            ]
                        )
                        RevCustomerIds = ""
                        #SiteType = 0
                output_param = pytds.output(value=None, param_type=int)
                return_value = cursor.callproc(
                    stored_procedure_name,
                    (
                        RevCustomerIds,
                        ServiceProviderId,
                        TenantId,
                        SiteType,
                        AMOPCustomerIds,
                        BillingPeriodId,
                        output_param,
                    ),
                )  # Placeholder for output parameter
                output_value = return_value[-1]
                return output_value




def sim_cards_to_optimize_count(data):
    common_utils_database = DB('common_utils', **db_config)
    database = DB('altaworx_test', **db_config)
    """
    The sim_cards_to_optimize_count function calculates the number of
    SIM cards eligible for optimization based on the provided filters
    like ServiceProviderId, BillingPeriodId, tenant_name, and optimization_type.
    It retrieves the relevant data by executing a stored procedure on a remote database,
    either summing up SIM card counts across customers or targeting a specific customer.
    Errors are logged if database connection or execution fails.
    """
    logging.info(f"Request Data Recieved")
    ServiceProviderIds = data.get("ServiceProviderId", "")
    query = f"select id from serviceprovider where id={ServiceProviderIds}"
    ServiceProviderId = database.execute_query(query, True)["id"].to_list()[0]
    logging.debug(f"ServiceProviderId is {ServiceProviderId}")
    BillingPeriodId = data.get("BillingPeriodId", "")
    customer_name = data.get("customer_name", "")
    tenant_name = data.get("tenant_name", "")
    TenantId = common_utils_database.get_data(
        "tenant", {"tenant_name": "Altaworx"}, ["id"]
    )["id"].to_list()[0]
    optimization_type = data.get("optimization_type", "")
    # Define your connection parameters
    server = "altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com"
    database = "AltaworxCentral_Test"
    username = "ALGONOX-Vyshnavi"
    password = "cs!Vtqe49gM32FDi"
    sim_cards_to_optimize = 0  # Ensure default initialization
    if optimization_type == "Customer" and customer_name=="All Customers":
        # customer_name='Easy Shop Supermarket (300007343)'
        # Create a connection to the database
        try:
            with pytds.connect(
                server=server, database=database, user=username, password=password
            ) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    #rev_status=False
                    rev_status=common_utils_database.get_data('module',{"module_name":"Rev.IO Customers"},["is_active"])["is_active"].to_list()[0]
                    if rev_status==True:
                        if ServiceProviderId==20:
                            stored_procedure_name = (
                                "AltaworxCentral_Test.dbo.[usp_Optimization_Mobility_CustomersGet]"
                            )
                        else:
                            stored_procedure_name = (
                                "AltaworxCentral_Test.dbo.[usp_OptimizationCustomersGet]"
                            )

                    else:
                        if ServiceProviderId==20:
                            stored_procedure_name = (
                                "AltaworxCentral_Test.dbo.[usp_Optimization_Mobility_AMOPCustomersGet]"
                            )

                        else:
                            stored_procedure_name = (
                                "AltaworxCentral_Test.dbo.[usp_Optimization_AMOPCustomersGet]"
                            )


                    # Execute the stored procedure
                    cursor.callproc(
                        stored_procedure_name,
                        (TenantId,ServiceProviderId, BillingPeriodId),
                    )

                    # Fetch results if the stored procedure returns any
                    results = cursor.fetchall()
                    # If a specific customer_name is provided, count only that customer's records
                    if customer_name=="All Customers":
                        # If no customer_name is provided, sum all the records
                        for row in results:
                            sim_cards_to_optimize += row[-1]

                    else:
                        for row in results:
                            if row[1] == customer_name:
                                sim_cards_to_optimize = row[-1]
                                break

                    return sim_cards_to_optimize

        except Exception as e:
            logging.exception("Error in connection or execution:", e)
    elif optimization_type == "Carrier":
        # Create a connection to the database for optimization type other than 'Customer'
        try:
            with pytds.connect(
                server=server, database=database, user=username, password=password
            ) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    if ServiceProviderId==20:
                        stored_procedure_name = (
                            "AltaworxCentral_Test.dbo.[usp_Optimization_Mobility_SimCardsCount]"
                        )
                        SiteType=1
                        AMOPCustomerId=None
                        RevAccountNumber=None
                        isCarrierOptimization=True
                        # Execute the stored procedure
                        cursor.callproc(
                            stored_procedure_name,
                            (ServiceProviderId,RevAccountNumber,BillingPeriodId,SiteType,AMOPCustomerId,isCarrierOptimization),
                        )
                        # Fetch results if the stored procedure returns any
                        results = cursor.fetchall()
                    else:
                        stored_procedure_name = (
                            "AltaworxCentral_Test.dbo.[usp_OptimizationSimCardsCount]"
                        )
                        SiteType=1
                        AMOPCustomerId=None
                        RevAccountNumber=None
                        # Execute the stored procedure
                        cursor.callproc(
                            stored_procedure_name,
                            (ServiceProviderId,RevAccountNumber,BillingPeriodId,SiteType,AMOPCustomerId),
                        )
                        # Fetch results if the stored procedure returns any
                        results = cursor.fetchall()

                    for row in results:
                        sim_cards_to_optimize += row[-1]

                    return sim_cards_to_optimize

        except Exception as e:
            logging.exception("Error in connection or execution:", e)
    else:
        rev_customer_id = str(data.get("customer_id", ""))
        rev_customer_id = str(rev_customer_id)
        # Create a connection to the database for optimization type other than 'Customer'
        try:
            with pytds.connect(
                server=server, database=database, user=username, password=password
            ) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    if ServiceProviderId==20:
                        stored_procedure_name = (
                            "AltaworxCentral_Test.dbo.[usp_Optimization_Mobility_SimCardsCount]"
                        )
                    else:
                        stored_procedure_name = (
                            "AltaworxCentral_Test.dbo.[usp_OptimizationSimCardsCount]"
                        )
                    SiteType=1
                    AMOPCustomerId=None
                    RevAccountNumber=rev_customer_id
                    print(ServiceProviderId,RevAccountNumber,BillingPeriodId,SiteType,AMOPCustomerId)
                    # Execute the stored procedure
                    cursor.callproc(
                        stored_procedure_name,
                        (ServiceProviderId,RevAccountNumber,BillingPeriodId,SiteType,AMOPCustomerId),
                    )
                    # Fetch results if the stored procedure returns any
                    results = cursor.fetchall()
                    for row in results:
                        sim_cards_to_optimize += row[-1]

                    return sim_cards_to_optimize
        except Exception as e:
            logging.exception("Error in connection or execution:", e)




def get_customer_total_sim_cards_count(rev_customer_id, database, ServiceProviderId):
    logging.info(f"Request Data Received")
    common_utils_database = DB('common_utils', **db_config)
    try:
        # Ensure rev_customer_id is a string
        rev_customer_id = str(rev_customer_id)

        # Check if rev_customer_id is 0 or empty string and go to else block
        if rev_customer_id == "0" or not rev_customer_id:
            # If rev_customer_id is not provided or is '0', count SIM cards for all customers
            rev_customer_ids_query = f'''
            SELECT
                count(*)
                FROM
                    Device_Tenant AS dt
                INNER JOIN
                    Device AS d ON dt.Device_id = d.id
                LEFT OUTER JOIN
                    customers AS s ON dt.customer_Id = s.id
                INNER JOIN
                    ServiceProvider sp ON d.Service_Provider_Id = sp.Id
                WHERE
                    (sp.Tenant_Id = 1 OR dt.Tenant_Id = 1)
                    AND dt.Is_Active = true
                    AND dt.Is_Deleted = false AND d.Service_Provider_Id ={ServiceProviderId}
            '''
            rev_customer_ids = database.execute_query(rev_customer_ids_query, True)
            count_value = rev_customer_ids["count"][0]  # Assuming the result is returned as a DataFrame
            return count_value
        else:
            rev_status=common_utils_database.get_data('module',{"module_name":"Rev.IO Customers"},["is_active"])["is_active"].to_list()[0]
            if rev_status==True:
                site_id_query = f'''
                    SELECT c.id FROM public.revcustomer rc
                    JOIN customers c ON rc.id=c.rev_customer_id
                    WHERE rc.rev_customer_id='{rev_customer_id}'
                    AND rc.is_active=True AND rc.tenant_id=1
                    AND rc.integration_authentication_id=4
                '''
                siteId = database.execute_query(site_id_query, True)['id'].to_list()[0]
                if ServiceProviderId==20:
                    rev_customer_ids_query=f'''
                            SELECT
                            COUNT(*) AS count
                            FROM
                                Mobility_Device_Tenant AS dt
                            INNER JOIN
                                Mobility_Device AS d ON dt.Mobility_Device_Id = d.id
                            LEFT OUTER JOIN
                                customers AS s ON dt.customer_id = s.id
                            INNER JOIN
                                ServiceProvider sp ON d.service_provider_id = sp.Id
                            WHERE
                                dt.Tenant_Id = 1
                                AND dt.Is_Active = TRUE
                                AND d.service_provider_id = 20
                                AND dt.Account_Number = '{rev_customer_id}'
                                AND dt.Account_Number_Integration_Authentication_Id = 4
                                AND (dt.customer_id = {siteId} OR s.parent_customer_id = {siteId});
                    '''
                else:
                    # If rev_customer_id is provided and valid, count SIM cards for this specific customer
                    rev_customer_ids_query = f'''
                        SELECT
                            count(*)
                            FROM
                                Device_Tenant AS dt
                            INNER JOIN
                            Device AS d ON dt.Device_Id = d.id
                            LEFT OUTER JOIN
                                customers AS s ON dt.customer_id = s.id
                            INNER JOIN
                                ServiceProvider sp ON d.Service_Provider_Id = sp.Id
                            WHERE
                                (sp.Tenant_Id = 1 OR dt.Tenant_Id = 1)
                                AND dt.Is_Active = true
                                and d.is_active =true
                                AND d.Service_Provider_Id = {ServiceProviderId}
                                and Account_Number = '{rev_customer_id}'
                                AND Account_Number_Integration_Authentication_Id =4 and s.Id={siteId}
                    '''
                rev_customer_ids = database.execute_query(rev_customer_ids_query, True)
                count_value = rev_customer_ids["count"][0]  # Assuming the result is returned as a DataFrame

                return count_value
            else:
                integration_authentication_id=database.get_data('integration_authentication',{"service_provider_id":ServiceProviderId,"is_active":True},['id'])['id'].to_list()[0]
                # If rev_customer_id is provided and valid, count SIM cards for this specific customer
                if ServiceProviderId ==20:
                    rev_customer_ids_query = f'''
                        SELECT
                            count(*)
                            FROM
                                Device_Tenant AS dt
                            INNER JOIN
                            Device AS d ON dt.Device_Id = d.id
                            LEFT OUTER JOIN
                                customers AS s ON dt.customer_id = s.id
                            INNER JOIN
                                ServiceProvider sp ON d.Service_Provider_Id = sp.Id
                            WHERE
                                (sp.Tenant_Id = 1 OR dt.Tenant_Id = 1)
                                AND dt.Is_Active = true
                                and d.is_active =true
                                AND d.Service_Provider_Id = {ServiceProviderId}
                                and Account_Number = '{rev_customer_id}'
                                AND Account_Number_Integration_Authentication_Id ={integration_authentication_id}
                    '''
                else:
                    rev_customer_ids_query = f'''
                        SELECT
                        count(*)
                        FROM
                            Mobility_Device_Tenant AS dt
                        INNER JOIN
                        Mobility_Device AS d ON dt.Mobility_Device_Id = d.id
                        LEFT OUTER JOIN
                            customers AS s ON dt.customer_id = s.id
                        INNER JOIN
                            ServiceProvider sp ON d.service_provider_id = sp.Id
                        WHERE
                            (sp.Tenant_Id = 1 OR dt.Tenant_Id = 1)
                            AND dt.Is_Active = true
                            and d.is_active =true
                            AND d.service_provider_id = {ServiceProviderId}
                            and Account_Number = '{rev_customer_id}'
                            AND Account_Number_Integration_Authentication_Id ={integration_authentication_id}
                    '''

                rev_customer_ids = database.execute_query(rev_customer_ids_query, True)
                count_value = rev_customer_ids["count"][0]  # Assuming the result is returned as a DataFrame

                return count_value

    except Exception as e:
        logging.exception(f"Exception is {e}")
        count_value = 0
        return count_value



def get_optimization_pop_up_data(data):
    logging.info(f"Request Data in recieved in get_optimization_pop_up_data")
    ##database connection
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    ServiceProviderId = data.get("ServiceProviderId", "")
    try:
        optimization_type = data.get("optimization_type", "")

        if optimization_type == "Customer":
            """Rate Plan Count"""
            try:
                # Call function to get rate plans count by customer
                results = rate_plans_by_customer_count(
                    data
                )
                rate_plan_count = int(
                    results[0][0]
                )  # Ensure conversion to standard int
                print(rate_plan_count,'rate_plan_countrate_plan_countrate_plan_countrate_plan_count')
            except Exception as e:
                logging.exception("Failed to fetch the rate_plans", e)
                rate_plan_count = 0
            logging.debug(f"rate_plan_count is {rate_plan_count}")

            """Sim cards to Optimize"""
            try:
                # Call function to get SIM cards to optimize
                sim_cards_to_optimize = sim_cards_to_optimize_count(
                    data
                )
                if sim_cards_to_optimize is None:
                    sim_cards_to_optimize = 0
                sim_cards_to_optimize = int(
                    sim_cards_to_optimize
                )  # Ensure conversion to standard int
            except Exception as e:
                logging.warning("sim_cards_to_optimize", e)
                sim_cards_to_optimize = 0

            """Total Sim cards"""
            rev_customer_id = str(data.get("customer_id", ""))
            total_sim_cards_count = int(
                get_customer_total_sim_cards_count(rev_customer_id, database,ServiceProviderId)
            )  # Ensure conversion to standard int

            response = {
                "flag": True,
                "rate_plan_count": rate_plan_count,
                "sim_cards_to_optimize": sim_cards_to_optimize,
                "total_sim_cards_count": total_sim_cards_count,
            }
            return response

        else:
            """Rate Plan Count"""
            params = [ServiceProviderId]
            logging.info(ServiceProviderId, "ServiceProviderId")
            rate_plan_count_query = "SELECT count(*) FROM public.carrier_rate_plan where service_provider_id=%s and is_active=True"
            rate_plan_count = int(
                database.execute_query(rate_plan_count_query, params=params).iloc[0, 0]
            )  # Ensure conversion to standard int
            logging.info(rate_plan_count, "rate_plan_count")

            """Sim cards to Optimize"""
            try:
                sim_cards_to_optimize = sim_cards_to_optimize_count(
                    data
                )
                if sim_cards_to_optimize is None:
                    sim_cards_to_optimize = 0
                sim_cards_to_optimize = int(
                    sim_cards_to_optimize
                )  # Ensure conversion to standard int
            except Exception as e:
                logging.exception("sim_cards_to_optimize", e)
                sim_cards_to_optimize = 0

            """Total Sim cards"""
            params = [ServiceProviderId]
            total_sim_cards_count_query='''
            SELECT
                count(*)
                FROM
                    Device_Tenant AS dt
                INNER JOIN
                    Device AS d ON dt.Device_id = d.id
                LEFT OUTER JOIN
                    customers AS s ON dt.customer_Id = s.id
                INNER JOIN
                    ServiceProvider sp ON d.Service_Provider_Id = sp.Id
                WHERE
                    (sp.Tenant_Id = 1 OR dt.Tenant_Id = 1)
                    AND dt.Is_Active = true
                    AND dt.Is_Deleted = false AND d.Service_Provider_Id =%s
            '''
            #total_sim_cards_count_query = "SELECT count(*) FROM public.sim_management_inventory where service_provider_id=%s  and is_active=True"
            total_sim_cards_count = int(
                database.execute_query(total_sim_cards_count_query, params=params).iloc[
                    0, 0
                ]
            )  # Ensure conversion to standard int

            response = {
                "flag": True,
                "rate_plan_count": rate_plan_count,
                "sim_cards_to_optimize": sim_cards_to_optimize,
                "total_sim_cards_count": total_sim_cards_count,
            }
            return response

    except Exception as e:
        logging.exception("Error in connection or execution:", e)




##start button in optimize button in list view
def start_optimization(data):
    """
    The start_optimization function initiates an optimization process
    by sending a POST request with tenant and user-specific details to
    1.0 Controller. It retrieves the tenant ID from the database, sets
    necessary headers, and sends the request body as JSON, returning the
    API's response or error details in case of failure.
    """
    logging.info(f"Request Data Recieved")
    try:
        body = data.get("body", {})
        username = data.get("username", "")
        tenant_name = data.get("tenant_name", "")
        # tenant_database = data.get('db_name', '')
        # database Connection
        # database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": 'Altaworx'}, ["id"]
        )["id"].to_list()[0]
        tenant_id = str(tenant_id)
        url = os.getenv("OPTIMIZATIONAPI", " ")
        username = "amop-core"
        password = "uDLD4AK3vOnqAjpn2DVRhwlrcbTLB6EY"
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
        authorization_header = f"Basic {encoded_credentials}"

        # Define the headers
        headers = {
            "Authorization": authorization_header,
            "user-name": "lohitha.v@algonox.com",
            "x-tenant-id":tenant_id ,
            "Content-Type": "application/json",  # Specify content type for JSON
        }
        # Send the POST request
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response_data = {
            "flag": True,
            "status code": response.status_code,
            "message": response.json(),
        }
        # Return the status code and response JSON
        return response_data
    except Exception as e:
        logging.exception(f"Error fetching data: {e}")
        message = f"exception is {e}"
        response_data = {"flag": False, "message": message}
        # Return the status code and response JSON
        return response_data



def convert_timestampdata(data, tenant_time_zone):
    """
    Convert specified timestamp columns in the provided data structure to the tenant's timezone.
    Handles nested dictionaries or lists.
    """
    target_timezone = timezone(tenant_time_zone)
    # Specify the timestamp columns
    timestamp_columns = ["created_date", "modified_date", "deleted_date", "processed_date"]
    def process_record(record):
        for col in timestamp_columns:
            if col in record and record[col]:
                try:
                    # Attempt to parse and convert timestamp
                    timestamp = pd.to_datetime(record[col], errors="coerce")
                    if timestamp is not pd.NaT:
                        if timestamp.tz is None:
                            timestamp = timestamp.tz_localize("UTC")
                        record[col] = timestamp.tz_convert(target_timezone).strftime(
                            "%m-%d-%Y %H:%M:%S"
                        )
                except Exception as e:
                    print(f"Error processing column '{col}': {record[col]} - {e}")
        return record

    # Traverse through lists or dictionaries
    if isinstance(data, list):
        return [process_record(item) for item in data]
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):  # Process list of records
                data[key] = [process_record(item) for item in value]
        return data
    else:
        raise ValueError("Input data must be a list or dictionary.")


def serialize_data(data):
    """Recursively convert pandas objects in the data structure to serializable
    types."""
    if isinstance(data, list):
        return [serialize_data(item) for item in data]
    elif isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    elif isinstance(data, pd.Timestamp):
        # Handle pd.Timestamp and NaT explicitly
        return data.strftime("%m-%d-%Y %H:%M:%S") if pd.notna(data) else None
    elif data is pd.NaT:  # Directly check for pd.NaT
        return None  # Or return '' if you'd prefer an empty string
    elif isinstance(data, np.datetime64):  # Handle numpy datetime64
        return str(data) if pd.notna(data) else None
    elif isinstance(data, (float, int)) and pd.isna(data):  # Handle NaN
        return None
    else:
        return data  # Return as is if not a pandas object or NaN/NaT



def qualification_data(data):
    db = DB('common_utils', **db_config)
    Partner = data.get("Partner", "")
    request_received_at = data.get("request_received_at", None)
    module_name = data.get("module_name", "")
    session_id = data.get("session_id", "")
    tenant_database = data.get('db_name', '')
        # Database Connection
    database = DB(tenant_database, **db_config)
    username = data.get("username", '')
    first_last_name = data.get("firstLastName", '')
    qualification_id = data.get("qualification_id", '')

    try:
        qualification_id = data.get("qualification_id", '')
        # Fetch integration data
        activation_data = database.execute_query(f"SELECT * FROM qualification_address WHERE qualification_id = {qualification_id}", True)
        activation_data = activation_data.to_dict(orient="records")
        if not activation_data:  # This is the correct check for an empty list
            print("activation_data record not found.")
            return {"error": "activation_data not configured"}

        response_data = {"flag": True, "activation_data": serialize_data(activation_data), "service_provider": "AT&T - Telegence", "change_type":"Activate New Service"}

        try:

            # Log the user action in the audit table
            audit_data_user_actions = {
                "service_name": "Module Management",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response_data["flag"]),
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "exports data",
                "module_name": "exports",
                "request_received_at": request_received_at,
            }
            db.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
            print("Audit log recorded successfully")
        except Exception as e:
            print(f"Exception during audit log: {e}")
        return response_data

    except Exception as e:
        print(f"An error occurredd uring the export process: {e}")
        db.update_dict(
            "export_status", {"status_flag": "Failure"}, {"module_name": module_name}
        )
        db.log_error_to_db(
            {
                "service_name": "Module Management",
                "created_date": request_received_at,
                "error_message": str(e),
                "error_type": type(e).__name__,
                "users": username,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": str(e),
                "module_name": "export",
                "request_received_at": request_received_at,
            },
            "error_log_table",
        )
        return {"flag": False,"message": f"Session ID: {session_id} - The export {module_name} process encountered an error. Contact support if the issue persists."}
