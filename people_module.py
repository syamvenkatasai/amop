"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""

# Importing the necessary Libraries
import ast
import boto3
import re
import requests
import os
import pandas as pd
from common_utils.email_trigger import send_email
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
from common_utils.permission_manager import PermissionManager
import datetime
from datetime import time
from datetime import datetime, timedelta
import time
from io import BytesIO
import asyncio
from common_utils.data_transfer_main import DataTransfer
import json
from io import BytesIO
import base64
import zipfile
import io
import copy
import openpyxl
import pytds
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail,
    Content,
    Attachment,
    FileContent,
    FileName,
    FileType,
    Disposition,
)
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import base64
import re
from pytz import timezone
import boto3
import logging
import os
import time
from io import BytesIO
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import threading  # For asynchronous execution

# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}
logging = Logging(name="module_api")


def path_fun(path,data):

    if path == "/export":
        result = export(data)
    elif path == "/people_revio_customers_list_view":
        result = people_revio_customers_list_view(data)
    elif path == "/download_people_bulk_upload_template":
        result = download_people_bulk_upload_template(data)
    elif path == "/add_people_revcustomer_dropdown_data":
        result = add_people_revcustomer_dropdown_data(data)
    elif path == "/people_bulk_import_data":
        result = people_bulk_import_data(data)
    elif path == "/submit_update_info_people_revcustomer":
        result = submit_update_info_people_revcustomer(data)
    elif path == "/people_list_view":
        result = people_list_view(data)
    elif path == "/update_people_data":
        result = update_people_data(data)
    else:
        result = {"flag": False, "error": "Invalid path or method"}
        logging.warning("Invalid path or method requested: %s", path)

    return result

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
    ##Database connection

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


def form_modules_dict(data, sub_modules, tenant_modules, role_name):
    """
    Description:The form_modules_dict function constructs a nested dictionary that maps parent modules
    to their respective submodules and child modules. It filters and organizes modules based on the
    user's role, tenant permissions, and specified submodules.
    """
    logging.info("Starting to form modules dictionary.")
    # Initialize an empty dictionary to store the output
    out = {}
    # Iterate through the list of modules in the data
    for item in data:
        parent_module = item["parent_module_name"]
        logging.debug("Processing parent module: %s", parent_module)
        # Skip modules not assigned to the tenant unless the role is 'super admin'
        if (
            parent_module not in tenant_modules and parent_module
        ) and role_name.lower() != "super admin":
            continue
        # If there's no parent module, initialize an empty dictionary for the module
        if not parent_module:
            out[item["module_name"]] = {}
            continue
        else:
            out[item["parent_module_name"]] = {}
        # Iterate through the data again to find related modules and submodules
        for module in data:
            temp = {}
            # Skip modules not in the specified submodules unless the role is 'super admin'
            if (
                module["module_name"] not in sub_modules
                and module["submodule_name"] not in sub_modules
            ) and role_name.lower() != "super admin":
                logging.debug(
                    "Skipping parent module: %s (not in tenant modules)", parent_module
                )
                continue
            # Handle modules without submodules and create a list for them
            if (
                module["parent_module_name"] == parent_module
                and module["module_name"]
                and not module["submodule_name"]
            ):
                temp = {module["module_name"]: []}
            # Handle modules with submodules and map them accordingly
            elif (
                module["parent_module_name"] == parent_module
                and module["module_name"]
                and module["submodule_name"]
            ):
                temp = {module["submodule_name"]: [module["module_name"]]}
            # Update the output dictionary with the constructed module mapping
            if temp:
                for key, value in temp.items():
                    if key in out[item["parent_module_name"]]:
                        out[item["parent_module_name"]][key].append(value[0])
                    elif temp:
                        out[item["parent_module_name"]].update(temp)

    # Return the final dictionary containing the module mappings
    logging.info("Finished forming modules dictionary: %s", out)
    return out


def transform_structure(input_data):
    """
    Description:The transform_structure function transforms a nested dictionary
    of modules into a list of structured dictionaries,each with queue_order to
    maintain the order of parent modules, child modules, and sub-children
    """

    logging.info("Starting transformation of input data.")

    # Initialize an empty list to store the transformed data
    transformed_data = []
    # Initialize the queue order for parent modules
    queue_order = 1
    # Iterate over each parent module and its children in the input data
    for parent_module, children in input_data.items():
        transformed_children = []
        child_queue_order = 1
        # Iterate over each child module and its sub-children
        for child_module, sub_children in children.items():
            transformed_sub_children = []
            sub_child_queue_order = 1
            # Iterate over each sub-child module
            for sub_child in sub_children:
                transformed_sub_children.append(
                    {
                        "sub_child_module_name": sub_child,
                        "queue_order": sub_child_queue_order,
                        "sub_children": [],
                    }
                )
                sub_child_queue_order += 1
            # Append the transformed child module with its sub-children
            transformed_children.append(
                {
                    "child_module_name": child_module,
                    "queue_order": child_queue_order,
                    "sub_children": transformed_sub_children,
                }
            )
            child_queue_order += 1
        # Append the transformed parent module with its children
        transformed_data.append(
            {
                "parent_module_name": parent_module,
                "queue_order": queue_order,
                "children": transformed_children,
            }
        )
        queue_order += 1
    # Return the list of transformed data
    return transformed_data


def get_people_data(data_list, module_name, tenant_id, database):
    # database = DB('altaworx_central', **db_config)
    logging.info("Fetching data for module: %s", module_name)
    # logging.info(tenant_id,"tenant_id")

    if module_name == "Bandwidth Customers":
        band_id = []

        for data_item in data_list["customers"]:
            if data_item["bandwidth_customer_id"]:
                band_id.append(data_item["bandwidth_customer_id"])
        logging.info("Bandwidth Customer IDs collected: %s", band_id)

        if band_id:
            band_id_int = tuple(int(float(i)) for i in band_id)
            logging.info(
                "Executing query to fetch bandwidth customers with IDs: %s", band_id_int
            )

            query = f"""
            SELECT bws.id AS bandwidth_unique_col, 
                   bws.bandwidth_account_id, 
                   bws.bandwidth_customer_name, 
                   c.bandwidth_customer_id
            FROM customers AS c
            JOIN bandwidth_customers AS bws 
              ON bws.id = CAST(CAST(c.bandwidth_customer_id AS FLOAT) AS INTEGER)
            WHERE bws.id IN {band_id_int}
            ORDER BY c.modified_date DESC;
            """
            df = database.execute_query(query, True)
            total = len(df)
            logging.info("Number of bandwidth customers fetched: %d", total)
            bandwidth_account_id = df["bandwidth_account_id"].to_list()[0]
            logging.info("Fetched bandwidth account ID: %s", bandwidth_account_id)
            data_out = df.to_dict(orient="records")

            global_account_number_df = database.get_data(
                "bandwidthaccount",
                {"id": bandwidth_account_id},
                ["id", "global_account_number"],
            )
            global_account_number = global_account_number_df[
                "global_account_number"
            ].to_list()[0]
            bandwidthaccount_unique_col = global_account_number_df["id"].to_list()[0]
            logging.info(
                "Global account number: %s, Bandwidth account unique col: %s",
                global_account_number,
                bandwidthaccount_unique_col,
            )
            merged_list = []
            dict2 = {d["bandwidth_customer_id"]: d for d in data_list["customers"]}
            for dic_data_out in data_out:
                # Find the corresponding dictionary in dict2
                dic_data_list = dict2.get(dic_data_out["bandwidth_customer_id"], {})
                # Merge dictionaries
                merged_dict = {**dic_data_list, **dic_data_out}
                merged_dict.update({"global_account_number": global_account_number})
                merged_dict.update(
                    {"bandwidthaccount_unique_col": bandwidthaccount_unique_col}
                )
                merged_list.append(merged_dict)

            data_list["customers"] = merged_list
            logging.info("Successfully merged bandwidth customer data.")

        return data_list, total
    elif module_name == "E911 Customers":
        e911_customer_ids = tuple(int(float(i)) for i in data_list["e911_customer_id"])
        # e911_customer_ids=tuple(data_list['e911_customer_id'])
        if e911_customer_ids:
            df = database.get_data(
                "e911customers",
                {"id": e911_customer_ids},
                order={"modified_date": "desc"},
            )
            total = len(df)
            data_list["e911_customers"] = df.to_dict(orient="records")
            data_list.pop("e911_customer_id")
        return data_list, total

    elif module_name == "NetSapiens Customers":
        reseller_ids = database.get_data(
            "customers", {"netsapiens_customer_id": "not Null"}, ["id"]
        )["id"].to_list()
        total = len(reseller_ids)
        # logging.info(reseller_ids)
        for data_item in data_list["customers"]:
            if data_item["id"] in reseller_ids:
                data_item["netsapiens_type"] = "Reseller"
            else:
                data_item["netsapiens_type"] = "Domain"

        return data_list, total


def update_people_data(data):
    """
    updates module data for a specified module by checking user and tenant
    to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view,
      handles errors, and logs relevant information.
    """
    # Start time  and date calculation
    start_time = time.time()
    # print(f"Request Data: {data}")
    Partner = data.get("Partner", "")
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
        logging.info("Checking user permissions.")

        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)

        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            logging.warning("User does not have permission: %s", result)
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        logging.warning(f"got exception in the restriction")

    request_received_at = data.get("request_received_at", "")
    session_id = data.get("session_id", "")
    user_name = data.get("user_name", "")
    action = data.get("action", "")
    ui_changed_data = data.get("changed_data", {})
    module_name = data.get("module", {})
    ##Database connection
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    dbs = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        logging.info("Fetching column mappings for module: %s", module_name)
        df = dbs.get_data(
            "module_column_mappings",
            {"module_name": module_name},
            ["main_update_table", "sub_table_mapping", "unique_columns"],
        )
        main_update_table = df["main_update_table"].to_list()[0]
        try:
            sun_table_map = json.loads(df["sub_table_mapping"].to_list()[0])
        except:
            sun_table_map = {}
        try:
            unique_column = json.loads(df["unique_columns"].to_list()[0])
        except:
            unique_column = df["unique_columns"].to_list()

        changed_data = {}
        for key, value in ui_changed_data.items():
            if value and value != None and value != "None":
                changed_data[key] = value
            else:
                changed_data[key] = None

        update_dics = {}
        for table, cols in sun_table_map.items():
            if table not in update_dics:
                update_dics[table] = {}
            for col in cols:
                if col in changed_data and changed_data[col]:
                    update_dics[table][col] = changed_data[col]
                if col in changed_data:
                    changed_data.pop(col)
        if action == "delete":
            filtered_changed_data = {
                key: value for key, value in changed_data.items() if value is not None
            }
            logging.debug("Updating record in main table: %s", main_update_table)
            unique_column_val = filtered_changed_data.pop(
                unique_column[main_update_table]
            )
            logging.debug("Unique column value for update: %s", unique_column_val)
            logging.debug(
                main_update_table, "33333333333333333333333333333333333333333"
            )

            database.update_dict(
                main_update_table, filtered_changed_data, {"id": changed_data["id"]}
            )
            logging.debug("Record updated successfully in table: %s", main_update_table)
            # database.execute(delete_query_sub_table)
        elif action == "update":
            filtered_changed_data = {
                key: value for key, value in changed_data.items() if value is not None
            }
            logging.debug("Updating record in main table: %s", main_update_table)
            unique_column_val = filtered_changed_data.pop(
                unique_column[main_update_table]
            )
            logging.debug("Unique column value for update: %s", unique_column_val)

            database.update_dict(
                main_update_table, filtered_changed_data, {"id": changed_data["id"]}
            )
            logging.debug("Record updated successfully in table: %s", main_update_table)

        elif action == "create":
            logging.debug("Creating new record in main table: %s", main_update_table)

            # Check if module is "Bandwidth Customers" and fetch bandwidth_account_id
            if module_name == "Bandwidth Customers":
                # Fetch the id from bandwidth_customers where bandwidth_customer_name matches Partner
                result = database.get_data(
                    "bandwidth_customers", {"bandwidth_customer_name": Partner}, ["id"]
                )

                # If a matching record is found, use it as the bandwidth_account_id
                if not result.empty:
                    bandwidth_id = result["id"].iloc[0]
                    changed_data["bandwidth_customer_id"] = int(bandwidth_id)
                    logging.debug(
                        f"Assigned bandwidth_account_id: {bandwidth_id} for Partner: {Partner}"
                    )

            # Check if module is "NetSapiens Customers" and fetch netsapiens_customer_id
            elif module_name == "NetSapiens Customers":
                # Fetch the territory_id from netsapiens_reseller where partner matches description
                territory_value = changed_data.get("tenant_name", None)
                query_filter = (
                    {"territory": territory_value}
                    if territory_value
                    else {"description": data.get("description", "")}
                )

                # Fetch the territory_id based on the selected filter
                result = database.get_data(
                    "netsapiens_reseller", query_filter, ["territory_id"]
                )
                logging.debug(
                    result, "3333333333333333332222222222222222222222222222222222222222"
                )

                if result.empty:
                    logging.debug(
                        "No matching record found for 'territory'. Attempting to query by 'description' instead."
                    )
                    description_value = changed_data.get("tenant_name", "")
                    query_filter = {"description": description_value}

                    # Run the query again using description as the filter
                    result = database.get_data(
                        "netsapiens_reseller", query_filter, ["territory_id"]
                    )

                # If a matching record is found, use it as the netsapiens_customer_id
                if not result.empty:
                    territory_id = result["territory_id"].iloc[0]
                    changed_data["netsapiens_customer_id"] = int(territory_id)
                    print(
                        f"Assigned netsapiens_customer_id: {territory_id} for description: {data.get('description', '')}"
                    )

            # Clean up changed_data to ensure it only contains relevant fields for the insert
            if "id" in changed_data:
                changed_data.pop(
                    "id"
                )  # Remove 'id' if present, as it may be auto-generated
            if "created_by" not in changed_data:
                changed_data["created_by"] = (
                    user_name  # Ensure created_by is set to the current user
                )
            if "created_date" not in changed_data:
                changed_data["created_date"] = (
                    request_received_at  # Set created_date to the request time
                )

            if "is_deleted" not in changed_data or changed_data["is_deleted"] in (
                None,
                "None",
                "",
            ):
                changed_data["is_deleted"] = False

            logging.debug("Inserting data: %s", changed_data)
            # Insert the new record into the main update table
            inster_id = database.insert_data(changed_data, main_update_table)

            message = f"Record created successfully"
            logging.debug("Create action completed successfully.")
            response = {"flag": True, "message": message}

        message = f"Updated sucessfully"
        response = {"flag": True, "message": message}
        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        try:
            audit_data_user_actions = {
                "service_name": "Module Management",
                "created_date": request_received_at,
                "created_by": user_name,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(changed_data),
                "module_name": "update_people_data",
                "request_received_at": request_received_at,
            }
            dbs.update_audit(audit_data_user_actions, "audit_user_actions")
        except Exception as e:
            print(f"Exception is {e}")
        return response
    except Exception as e:
        print(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {
                "service_name": "update_people_data",
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": user_name,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "People Module",
                "request_received_at": request_received_at,
            }
            dbs.log_error_to_db(error_data, "error_log_table")
        except Exception as e:
            print(f"Exception is {e}")
        return response


# Function to convert Timestamps to strings
def convert_timestamps(obj):
    logging.info("Converting timestamps in the object: %s", obj)
    if isinstance(obj, dict):
        logging.info("Processing a dictionary.")
        return {k: convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        logging.info("Processing a list with %d elements.", len(obj))
        return [convert_timestamps(elem) for elem in obj]
    elif isinstance(obj, pd.Timestamp):
        logging.info("Converting pandas Timestamp: %s", obj)
        return obj.strftime("%m-%d-%Y %H:%M:%S")
    logging.info("No conversion applied for object: %s", obj)
    return obj


def dataframe_to_blob(data_frame):
    """
    Description:The Function is used to convert the dataframe to blob
    """
    logging.info("Converting DataFrame to blob.")
    # Create a BytesIO buffer
    bio = BytesIO()

    # Use ExcelWriter within a context manager to ensure proper saving
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        logging.info("Writing DataFrame to Excel.")
        data_frame.to_excel(writer, index=False)
        logging.info("DataFrame written to Excel successfully.")

    # Get the value from the buffer
    bio.seek(0)
    blob_data = base64.b64encode(bio.read())
    logging.info("DataFrame converted to blob successfully.")

    return blob_data


def export(data, max_rows=500):
    """
    Description:Exports data into an Excel file. It retrieves data based on the module name from the database,
    processes it, and returns a blob representation of the data if within the allowed row limit.
    """
    # logging.info the request data for debugging
    # logging.info(f"Request Data: {data}")
    ### Extract parameters from the Request Data
    Partner = data.get("Partner", "")
    request_received_at = data.get("request_received_at", None)
    module_name = data.get("module_name", "")
    start_date = data.get("start_date", "")
    end_date = data.get("end_date", "")
    user_name = data.get("user_name", "")
    session_id = data.get("session_id", "")
    tenant_database = data.get("db_name", "")
    ids = data.get("ids", "")
    ##database connection for common utilss
    db = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    logging.info(f"Fetching export query for module: {module_name}")
    # Start time  and date calculation
    start_time = time.time()
    try:
        ##databse connenction
        database = DB(tenant_database, **db_config)
        # Fetch the query from the database based on the module name
        module_query_df = db.get_data("export_queries", {"module_name": module_name})
        # logging.info(module_query_df,'module_query_df')
        ##checking the dataframe is empty or not
        if module_query_df.empty:
            return {
                "flag": False,
                "message": f"No query found for module name: {module_name}",
            }
        # Extract the query string from the DataFrame
        query = module_query_df.iloc[0]["module_query"]
        if not query:
            logging.warning(f"Unknown module name: {module_name}")
            return {"flag": False, "message": f"Unknown module name: {module_name}"}
        ##params for the specific module
        if module_name in ("inventory status history", "bulkchange status history"):
            params = [ids]
        else:
            params = [start_date, end_date]
        if module_name == "Users":
            data_frame = db.execute_query(query, params=params)
        else:
            data_frame = database.execute_query(query, params=params)
        # Check the number of rows in the DataFrame
        row_count = data_frame.shape[0]
        # logging.info(row_count,'row_count')
        ##checking the max_rows count
        if row_count > max_rows:
            return {
                "flag": False,
                "message": f"Cannot export more than {max_rows} rows.",
            }
        if module_name == "NetSapiens Customers":
            data_frame["NetSapiensType"] = "Reseller"
        # Capitalize each word and add spaces
        data_frame.columns = [
            col.replace("_", " ").capitalize() for col in data_frame.columns
        ]
        data_frame["S.No"] = range(1, len(data_frame) + 1)
        # Reorder columns dynamically to put S.NO at the first position
        columns = ["S.No"] + [col for col in data_frame.columns if col != "S.No"]

        # Format specific columns with dollar symbol for "Rate Plan Socs" module

        if module_name == "Rate Plan Socs":
            if "Overage dollar mb" in data_frame.columns:
                data_frame["Overage dollar mb"] = data_frame["Overage dollar mb"].apply(
                    lambda x: f"${x}"
                )
            if "Base rate" in data_frame.columns:
                data_frame["Base rate"] = data_frame["Base rate"].apply(
                    lambda x: f"${x}"
                )

        if module_name == "Customer Rate Plan":

            # Check if "Id" column exists in data_frame
            if "Id" in data_frame.columns:
                try:
                    # Convert all "Id" values into a list for a single query
                    ids_list = data_frame["Id"].tolist()

                    # Use a single SQL query to get the counts for each ID at once
                    count_query = """
                        SELECT customer_rate_plan_id, COUNT(*) AS count
                        FROM sim_management_inventory
                        WHERE customer_rate_plan_id IN %s
                        GROUP BY customer_rate_plan_id
                    """

                    # Execute the single query with the list of IDs
                    count_results = database.execute_query(
                        count_query, params=(tuple(ids_list),)
                    )

                    # Convert the count results to a dictionary for quick lookup
                    tn_counts_dict = {
                        int(row["customer_rate_plan_id"]): row["count"]
                        for _, row in count_results.iterrows()
                    }

                    # Map counts back to the DataFrame
                    data_frame["# of TNs"] = (
                        data_frame["Id"]
                        .map(tn_counts_dict)
                        .fillna(0)
                        .astype(int)
                        .astype(str)
                    )
                    columns.append("# of TNs")
                    columns.remove("Id")

                except Exception as e:
                    logging.exception(
                        f"Failed to retrieve count for Customer Rate Plan: {e}"
                    )
                    return {
                        "flag": False,
                        "message": f"An error occurred while fetching count for Customer Rate Plan: {e}",
                    }

        data_frame = data_frame[columns]
        # Proceed with the export if row count is within the allowed limit
        data_frame = data_frame.astype(str)
        data_frame.replace(to_replace="None", value="", inplace=True)
        logging.info("Converting DataFrame to blob.")

        blob_data = dataframe_to_blob(data_frame)
        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))

        # Return JSON response
        response = {"flag": True, "blob": blob_data.decode("utf-8")}
        audit_data_user_actions = {
            "service_name": "Module Management",
            "created_date": request_received_at,
            "created_by": user_name,
            "status": str(response["flag"]),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": "",
            "module_name": "export",
            "request_received_at": request_received_at,
        }
        db.update_audit(audit_data_user_actions, "audit_user_actions")
        logging.info("Auditing user actions.")
        return response
    except Exception as e:
        error_type = str(type(e).__name__)
        logging.exception(f"An error occurred: {e}")
        message = f"Error is {e}"
        response = {"flag": False, "message": message}
        try:
            # Log error to database
            error_data = {
                "service_name": "Module Management",
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": user_name,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "export",
                "request_received_at": request_received_at,
            }
            db.log_error_to_db(error_data, "error_log_table")
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response


def people_revio_customers_list_view(data):
    # logging.info(f"Request Data: {data}")

    tenant_name = data.get("tenant_name", None)
    role_name = data.get("role_name", None)
    tenant_database = data.get("db_name", "")
    # Database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

    try:
        return_json_data = {}

        # Fetch tenant_id based on tenant_name
        # tenant_id = common_utils_database.get_data("tenant", {'tenant_name': tenant_name}, ['id'])['id'].to_list()[0]

        # Pagination logic
        start_page = data.get("mod_pages", {}).get("start", 0)
        end_page = data.get("mod_pages", {}).get("end", 100)

        limit = end_page - start_page
        offset = start_page

        # # Get tenant's timezone
        tenant_timezone_query = (
            """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
        )
        tenant_timezone = common_utils_database.execute_query(
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
        headers_map = get_headers_mapping(
            tenant_database,
            ["people_rev_io_customers"],
            role_name,
            "",
            "",
            "",
            "",
            data,
            common_utils_database,
        )

        # Prepare dropdown data
        dropdown_options = [{"label": "All", "value": "all"}]  # Add the "All" option

        query = """
                        SELECT
                            COUNT(*) OVER() AS total_count,
                            id,
                            partner, 
                            agent_name,  
                            name, 
                            account,
                            customer_bill_period_end_day, 
                            customer_bill_period_end_hour
                        FROM 
                            public.vw_people_revio_customers
                        ORDER BY  
                            modified_date DESC
                        LIMIT %s OFFSET %s;"""

        params = [limit, offset]
        logging.info(f"Executing query to fetch all customers with params: {params}")

        # Execute the main query and get results
        start_time = time.time()
        result = database.execute_query(query, params=params)
        query_duration = time.time() - start_time
        logging.info(f"Query executed in {query_duration:.2f} seconds")

        # Execute the count query
        logging.info("Executing count query.")
        # count_result = database.execute_query(count_query, True)
        total_count = int(result.iloc[0]["total_count"]) if not result.empty else 0

        # Pagination pages info
        pages = {"start": start_page, "end": end_page, "total": total_count}

        logging.info("Preparing the response data.")
        # Execute each query and store the result
        # result = database.execute_query(query, params=params)
        # df_dict = result.to_dict(orient='records')
        df_dict = convert_timestamp_data(
            result.to_dict(orient="records"), tenant_time_zone
        )

        # Set the response data
        if not result.empty:
            return_json_data["flag"] = True
            return_json_data["message"] = "Data fetched successfully"
            return_json_data["data"] = serialize_data(df_dict)
            return_json_data["headers_map"] = headers_map
            return_json_data["pages"] = pages
            return_json_data["dropdown_options"] = dropdown_options
            logging.info("Data fetched successfully.")
        else:
            return_json_data["flag"] = False
            return_json_data["message"] = "No data found or an error occurred"
            return_json_data["data"] = []
            return_json_data["dropdown_options"] = dropdown_options
            logging.warning("No data found.")

        return return_json_data

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        # Error handling
        return_json_data["flag"] = False
        return_json_data["message"] = f"Failed!! Error: {str(e)}"
        return_json_data["data"] = []
        return return_json_data


def add_people_revcustomer_dropdown_data(data):
    """
    Description: Retrieves add_service_product dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    """
    # checking the access token valididty
    username = data.get("username", None)

    tenant_name = data.get("tenant_name", None)
    session_id = data.get("session_id", None)
    module_name = data.get("module_name", None)
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)

    try:
        response_data = {}
        rev_customer_name = data.get("customer_name", None)
        response_message = ""
        # Query 1: Get customer ID
        query1 = """SELECT IA.username FROM integration_authentication IA JOIN revcustomer RC ON IA.integration_id = RC.integration_authentication_id;"""
        logging.info("Executing Query 1 to retrieve customer ID.")

        rev_io_account_df = database.execute_query(query1, flag=True)
        # Extract only the 'username' column values as a list
        if isinstance(rev_io_account_df, pd.DataFrame) and not rev_io_account_df.empty:
            rev_io_account = rev_io_account_df["username"].dropna().tolist()
            # Remove duplicates by converting to a set and back to a list
            rev_io_account = list(dict.fromkeys(rev_io_account))

            response_data["rev_io_account"] = rev_io_account
            logging.debug(f"Retrieved rev_io_account: {rev_io_account}")
        else:
            logging.info("No data found or query failed")
        # Retrieve all customer_name values from the 'customers' table
        customer_names_df = database.get_data("customers", {}, ["customer_name"])
        if isinstance(customer_names_df, pd.DataFrame) and not customer_names_df.empty:
            customer_names = customer_names_df["customer_name"].tolist()
            customer_names = list(set(customer_names))
            response_data["customer_names"] = customer_names
            logging.debug(f"Retrieved customer names: {customer_names}")
        else:
            logging.info("No customer names found or query failed")
            response_data["customer_names"] = []

        # Query 2: Get Bill Profile
        query2 = """SELECT  description FROM public.revbillprofile where integration_authentication_id = 1 and is_active = 'true' and is_deleted = 'false' order by description ASC"""

        logging.info("Executing Query 2 to retrieve bill profiles.")
        rev_io_bill_profile_df = database.execute_query(query2, flag=True)
        # Extract only the 'username' column values as a list
        if (
            isinstance(rev_io_bill_profile_df, pd.DataFrame)
            and not rev_io_bill_profile_df.empty
        ):
            rev_bill_profile_list = (
                rev_io_bill_profile_df["description"].dropna().unique().tolist()
            )
            response_data["rev_bill_profile"] = rev_bill_profile_list
            logging.debug(f"Retrieved bill profiles: {rev_bill_profile_list}")
        else:
            logging.info("No bill profiles found or query failed")
            response_data["rev_bill_profile"] = []

        # Query 3: Get Customer Rate Plan
        query3 = """
                SELECT *
                FROM revcustomer
                WHERE is_active = 'true' 
                AND is_deleted = 'false'
                AND rev_customer_id IS NOT NULL 
                AND rev_customer_id <> ''
                AND integration_authentication_id = 1
                AND status IS NOT NULL 
                AND status <> 'CLOSED'
                ORDER BY customer_name, rev_customer_id;
            """
        logging.info("Executing Query 3 to retrieve customer rate plans.")
        customer_rate_plan = database.execute_query(query3, flag=True)
        # Extract only the 'username' column values as a list
        if (
            isinstance(customer_rate_plan, pd.DataFrame)
            and not customer_rate_plan.empty
        ):
            # Construct the customer rate plan list
            response_data["customer_rate_plan"] = [
                f"{row['customer_name']} - {row['rev_customer_id']}"  # Adjust column names as needed
                for index, row in customer_rate_plan.iterrows()
            ]
            logging.debug(
                f"Retrieved customer rate plans: {response_data['customer_rate_plan']}"
            )
        else:
            logging.warning("No customers found or query failed")
            response_data["customer_rate_plan"] = []

        message = " data sent sucessfully"
        response = {"flag": True, "message": message, "response_data": response_data}
        return response
    except Exception as e:
        logging.warning(f"Something went wrong and error is {e}")
        message = "Something went wrong while getting add service product"
        # Error Management
        error_data = {
            "service_name": "SIM management",
            "error_messag": message,
            "error_type": e,
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
        }
        database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "message": message}


def download_people_bulk_upload_template(data):
    # Construct the SQL query based on the provided logic
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE 
            (table_name = 'revagent' AND column_name = 'agent_name') OR
            (table_name = 'revcustomer' AND column_name IN ('customer_name', 'rev_customer_id')) OR
            (table_name = 'customers' AND column_name IN ('tenant_name','customer_bill_period_end_day', 'customer_bill_period_end_hour'));
    """

    # Execute the query to fetch the columns without parameters (set flag=True)
    tenant_database = data.get("db_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    logging.info(f"Executing query to fetch column names from the database: {query}")
    columns_df = database.execute_query(
        query, flag=True
    )  # Use flag=True since no parameters are required

    if isinstance(
        columns_df, bool
    ):  # Handle case if execution returns True (error scenario)

        logging.info("Query execution returned a boolean value instead of a DataFrame.")
        return {"flag": False, "error": "Failed to retrieve columns from the database."}
    logging.debug(
        f"Columns retrieved from the database: {columns_df['column_name'].tolist()}"
    )
    # Remove the 'id' column if it exists
    columns_df = columns_df[columns_df["column_name"] != "id"]
    columns_df["column_name"] = (
        columns_df["column_name"].str.replace("_", " ").str.capitalize()
    )
    # logging.info(columns_df, "00000000000000000")
    # logging.info(columns_df['column_name'].values, "o99999999999999999999999")

    # Create an empty DataFrame with the column names as columns
    result_df = pd.DataFrame(columns=columns_df["column_name"].values)
    # logging.info(result_df, "88888876543")

    logging.debug(f"Resulting DataFrame structure: {result_df.columns.tolist()}")
    # Convert the DataFrame to blob (binary data) and return it as part of the response
    blob_data = dataframe_to_blob(result_df)
    response = {"flag": True, "blob": blob_data.decode("utf-8")}
    logging.info("Successfully generated the bulk upload template.")
    return response


def convert_booleans(data):
    logging.debug(f"Initial data for conversion: {data}")
    for key, value in data.items():
        if isinstance(value, str) and value.lower() == "true":
            data[key] = True
            logging.debug(f"Converted '{key}' from string 'true' to boolean True")
        elif isinstance(value, str) and value.lower() == "false":
            data[key] = False
            logging.debug(f"Converted '{key}' from string 'false' to boolean False")
        elif isinstance(value, dict):  # Recursively process nested dictionaries
            logging.debug(
                f"Found nested dictionary at key '{key}', processing recursively."
            )
            convert_booleans(value)
    return data  # Return the modified dictionary


def submit_update_info_people_revcustomer(data):
    """
    Updates email template data for a specified module by checking user and tenant permissions.
    Constructs and executes SQL queries to fetch and manipulate data, handles errors, and logs relevant information.
    """
    data = convert_booleans(data)
    changed_data = data.get("changed_data", {})
    user_name = data.get("username", "")
    tenant_name = data.get("tenant_name", "")
    request_received_at = data.get("request_received_at", "")
    new_data = {k: v for k, v in data.get("new_data", {}).items() if v}
    new_payload = {k: v for k, v in data.get("new_payload", {}).items() if v}
    unique_id = changed_data.get("id")
    action = data.get("action", "")

    # Database connection setup
    tenant_database = data.get("db_name", "")
    # database Connection
    dbs = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

    tenant_id = common_utils_database.get_data(
        "tenant", {"tenant_name": tenant_name}, ["id"]
    )["id"].to_list()[0]

    try:
        if action == "create":
            new_data = {k: v for k, v in new_data.items() if v not in [None, "None"]}
            for k, v in new_data.items():
                if isinstance(v, list):
                    if all(isinstance(item, dict) for item in v):
                        new_data[k] = json.dumps(
                            v
                        )  # Convert list of dicts to JSON string
                    else:
                        new_data[k] = ", ".join(
                            str(item) for item in v if item is not None
                        )  # Convert other types to strings

            new_payload = {
                k: v for k, v in new_payload.items() if v not in [None, "None"]
            }
            for k, v in new_payload.items():
                if isinstance(v, list):
                    if all(isinstance(item, dict) for item in v):
                        new_payload[k] = json.dumps(
                            v
                        )  # Convert list of dicts to JSON string
                    else:
                        new_payload[k] = ", ".join(
                            str(item) for item in v if item is not None
                        )  # Convert other types to strings

            # url = 'https://api.revioapi.com/v1/Customers'
            url = os.getenv("PEOPLE_REVIO_CUSTOMER", " ")
            headers = {
                "Ocp-Apim-Subscription-Key": "04e3d452d3ba44fcabc0b7085cdde431",
                "Authorization": "Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r",
            }

            # Pass the new_data as params in the GET request
            params = new_data

            # Call the API and check response
            api_response = requests.get(url, headers=headers, params=params)
            if api_response.status_code == 200:
                # Success, construct and return response with both API and database insert responses
                message = "Add Rev.io product data submitted successfully."

                # Call the insert_data function and capture its response
                insert_response = dbs.insert_data(new_payload, "customers")

                # Ensure insert_response is a dictionary for consistency
                if isinstance(
                    insert_response, int
                ):  # if it's an ID or similar, wrap in dict
                    insert_response = {"success": True, "inserted_id": insert_response}
                elif not isinstance(insert_response, dict):
                    insert_response = {
                        "success": False,
                        "message": "Unknown insert response format",
                    }

                # Create the response data including both the API response and insert response
                response_data = {
                    "flag": True,
                    "message": message,
                    "api_response": api_response.json(),
                    "insert_response": insert_response,
                }

                return response_data
            else:
                # API call failed, return error message
                raise Exception(
                    f"Failed to retrieve data from client API: {api_response.status_code} - {api_response.text}"
                )

        elif action == "update":
            # Remove fields not in 'customers' table from 'changed_data'
            allowed_fields = [
                "id",
                "customer_bill_period_end_day",
                "customer_bill_period_end_hour",
                "modified_by",
                "modified_date",
            ]
            filtered_changed_data = {
                k: v
                for k, v in changed_data.items()
                if k in allowed_fields and v is not None
            }

            # Ensure 'id' is available for WHERE clause
            if "id" in filtered_changed_data:
                dbs.update_dict(
                    "customers",
                    filtered_changed_data,
                    {"id": filtered_changed_data["id"]},
                )
                message = "Update successful."
                response_data = {"flag": True, "message": message}
            else:
                message = "Update failed. Missing required ID for update."
                response_data = {"flag": False, "message": message}

            return response_data

        elif action == "info":
            line_item_count = 0  # Initialize with a default value

            query = """
                SELECT COUNT(dt.customer_id) AS TotalSimCardCount  
                    FROM public.device jd  
                    INNER JOIN public.device_tenant dt ON jd.id = dt.device_id  
                    INNER JOIN public.customers s ON dt.customer_id = s.id  
                    WHERE jd.is_active = 'true' 
                        AND jd.is_deleted = 'false' 
                        AND (dt.customer_id IS NOT NULL  
                            AND s.id IS NOT NULL  
                            AND s.id = %s)  
                        AND dt.tenant_id = %s;
            """
            params = [data["info_data"]["id"], tenant_id]

            # Execute the query
            result = dbs.execute_query(query, params=params)

            if isinstance(result, list) and result:
                line_item_count = result[0].get("line_item_count", 0)
            elif isinstance(result, pd.DataFrame) and not result.empty:
                line_item_count = result.iloc[0].get("line_item_count", 0)

            response_data = {
                "flag": True,
                "message": f"{action} Successfully",
                "line_item_count": str(line_item_count),
            }
            return response_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = "Unable to save the data"
        response = {"flag": False, "message": message}

        return response


def people_bulk_import_data(data):
    username = data.get("username")
    insert_flag = data.get("insert_flag", "append")
    tenant_database = data.get("db_name", "")

    # Initialize the database connection
    database = DB(tenant_database, **db_config)

    # Check if blob data is provided
    blob_data = data.get("blob")
    if not blob_data:
        logging.warning("Blob data not provided")
        return {"flag": False, "message": "Blob data not provided"}

    try:
        # Extract and decode the blob data
        blob_data = blob_data.split(",", 1)[1]
        blob_data += "=" * (-len(blob_data) % 4)  # Padding for base64 decoding
        file_stream = BytesIO(base64.b64decode(blob_data))

        # Read the data into a DataFrame
        uploaded_dataframe = pd.read_excel(file_stream, engine="openpyxl")

        # SQL query for fetching columns dynamically
        query = """
            SELECT 
                C.tenant_name AS tenant_name, 
                RA.agent_name AS agent_name, 
                RC.customer_name AS customer_name, 
                RC.rev_customer_id AS rev_customer_id, 
                C.customer_bill_period_end_day, 
                C.customer_bill_period_end_hour
            FROM 
                revcustomer RC
            LEFT JOIN 
                revagent RA ON RA.rev_agent_id = RC.agent_id
            LEFT JOIN 
                customers C ON C.rev_customer_id = RC.id
            LIMIT 100 OFFSET 0;
        """
        # Fetch the columns and data from the query
        result_df = database.execute_query(query, True)

        uploaded_dataframe.columns = uploaded_dataframe.columns.str.replace(
            " ", "_"
        ).str.lower()
        print(uploaded_dataframe.columns, "333333333333333333")
        # Normalize DataFrame columns
        uploaded_columns = [col.strip().lower() for col in uploaded_dataframe.columns]
        query_columns = [col.strip().lower() for col in result_df.columns]

        # Compare the column names
        if sorted(uploaded_columns) != sorted(query_columns):
            logging.warning("Column mismatch detected")
            return {
                "flag": False,
                "message": "Columns didn't match",
                "uploaded_columns": list(set(uploaded_columns) - set(query_columns)),
                "query_columns": list(set(query_columns) - set(uploaded_columns)),
            }

        # Split uploaded dataframe into separate dataframes for each table
        revcustomer_data = uploaded_dataframe[
            ["customer_name", "rev_customer_id", "agent_name"]
        ]
        revagent_data = uploaded_dataframe[["agent_name"]].drop_duplicates()
        customers_data = uploaded_dataframe[
            [
                "tenant_name",
                "customer_name",
                "customer_bill_period_end_day",
                "customer_bill_period_end_hour",
            ]
        ]

        # Perform the insertion
        if insert_flag == "append":
            database.insert(
                revcustomer_data, "revcustomer", if_exists="append", method="multi"
            )
            database.insert(
                revagent_data, "revagent", if_exists="append", method="multi"
            )
            database.insert(
                customers_data, "customers", if_exists="append", method="multi"
            )
            logging.info("Append operation completed successfully")
            return {"flag": True, "message": "Append operation is done"}

        logging.error("Invalid insert_flag value")
        return {"flag": False, "message": "Invalid insert_flag value"}

    except Exception as e:
        return {
            "flag": False,
            "message": f"An error occurred during the import: {str(e)}",
        }


def convert_timestamp(data):
    logging.debug(f"Converting data: {data}")
    if isinstance(data, pd.Timestamp):
        logging.debug(f"Converting pd.Timestamp: {data}")
        return data.strftime("%m-%d-%Y %H:%M:%S")
    elif isinstance(data, dict):
        logging.debug("Processing dictionary in convert_timestamp")
        return {k: convert_timestamp(v) for k, v in data.items()}
    elif isinstance(data, list):
        logging.debug("Processing list in convert_timestamp")
        return [convert_timestamp(v) for v in data]
    else:
        logging.debug(f"No conversion needed for type: {type(data)}")
        return data


def people_list_view(data):
    """
    Description: Retrieves emails in list view data from the database.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    """
    logging.info("Starting people_list_view function")
    # print(f"Request Data: {data}")
    # Database Connection
    database = DB("common_utils", **db_config)
    role_name = data.get("role_name", "")
    tenant_name = data.get("tenant_name", "")
    module_name = data.get("module_name", "")
    tenant_database = data.get("db_name", "altaworx_central")
    # Initialize data_list to hold customer data
    total_resellers = 0
    return_json_data = {}
    try:
        dbs = DB(tenant_database, **db_config)
        start_page = data.get("mod_pages", {}).get("start", 0)
        end_page = data.get("mod_pages", {}).get("end", 100)
        limit = 100  # Set the limit to 100

        logging.debug(f"Received request data")
        logging.info(
            f"Module name: {module_name}, Role name: {role_name}, Start page: {start_page}"
        )

        # Query to get the list of tenants
        tenant_query = "SELECT DISTINCT tenant_name FROM tenant WHERE is_active=True"
        tenant_list = database.execute_query(tenant_query, True)[
            "tenant_name"
        ].to_list()
        logging.info("Fetched tenant list")

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

        # Handle 'NetSapiens Customers'
        if module_name == "NetSapiens Customers":
            query = """
                SELECT 
                    *
                FROM vw_people_netsapiens_customers_list_view
				ORDER BY modified_date DESC, customer_name DESC
                LIMIT %s OFFSET %s;
            """
            params = [limit, start_page]

            # Execute the query using the existing execute_query function
            result = dbs.execute_query(query, params=params)
            count_query = """
                    SELECT COUNT(*) AS total_count
                        FROM vw_people_netsapiens_customers_list_view

                """
            total_count_result = dbs.execute_query(count_query, True)
            total_count = (
                total_count_result["total_count"].iloc[0]
                if not total_count_result.empty
                else 0
            )
            pages = {"start": start_page, "end": end_page, "total": int(total_count)}
            logging.info("Query executed for NetSapiens Customers")

            # Get headers mapping
            headers_map = get_headers_mapping(
                tenant_database,
                ["NetSapiens Customers"],
                role_name,
                "",
                "",
                "",
                "",
                data,
                database,
            )

            if not result.empty:
                df_dict = result.to_dict(orient="records")
                df_dict = convert_timestamp_data(df_dict, tenant_time_zone)
                df_dict = serialize_data(df_dict)

                logging.debug(f"Data fetched for NetSapiens Customers")

                # Prepare final response
                return_json_data.update(
                    {
                        "flag": True,
                        "message": "Data fetched successfully",
                        "data": df_dict,
                        "tenant_name": tenant_list,
                        "headers_map": headers_map,
                        "pages": pages,
                    }
                )
                return return_json_data
            else:
                return_json_data.update(
                    {"flag": False, "data": [], "message": "No data found!"}
                )
                logging.warning("No data found for NetSapiens Customers")
                return return_json_data

        # Handle 'Bandwidth customers'
        elif module_name == "Bandwidth Customers":
            query = """
                SELECT 
                    *
                FROM vw_people_bandwidth_customers
                ORDER BY modified_date DESC, customer_name ASC
                LIMIT %s OFFSET %s;
            """
            params = [limit, start_page]

            # Execute the query using the existing execute_query function
            result = dbs.execute_query(query, params=params)

            count_query = """
                    SELECT COUNT(*) AS total_count
                        FROM vw_people_bandwidth_customers

                """
            total_count_result = dbs.execute_query(count_query, True)
            total_count = (
                total_count_result["total_count"].iloc[0]
                if not total_count_result.empty
                else 0
            )
            logging.info("Query executed for Bandwidth Customers")
            headers_map = get_headers_mapping(
                tenant_database,
                ["Bandwidth Customers"],
                role_name,
                "",
                "",
                "",
                "",
                data,
                database,
            )

            # Check if result is empty
            if result.empty:
                return_json_data.update(
                    {
                        "flag": False,
                        "data": [],
                        "message": "No data found for Bandwidth customers!",
                    }
                )
                logging.warning("No data found for Bandwidth customers")
                return return_json_data

            # Process and format the data
            df_dict = result.to_dict(orient="records")
            # total = len(df_dict)
            df_dict = convert_timestamp_data(df_dict, tenant_time_zone)
            df_dict = serialize_data(df_dict)
            pages = {"start": start_page, "end": end_page, "total": int(total_count)}

            logging.info("Returning data response")
            return_json_data.update(
                {
                    "flag": True,
                    "message": "Data fetched successfully",
                    "data": df_dict,
                    "tenant_name": tenant_list,
                    "headers_map": headers_map,
                    "pages": pages,
                    "total_resellers": total_resellers,
                }
            )
            return return_json_data
        elif module_name == "E911 Customers":
            query = """
                SELECT
                id,
                account_name,
                account_id,
                created_by,
                created_date,
                modified_by,
                modified_date,
                deleted_by,
                deleted_date,
                is_active,
                is_deleted 
                FROM e911customers 
                WHERE is_active = 'true' AND is_deleted = 'false'
                ORDER BY modified_date DESC
                LIMIT %s OFFSET %s;
            """
            params = [limit, start_page]

            # Execute the query using the existing execute_query function
            result = dbs.execute_query(query, params=params)

            count_query = """
                    SELECT COUNT(*) AS total_count
                        FROM e911customers 
                WHERE is_active = 'true' AND is_deleted = 'false'

                """
            total_count_result = dbs.execute_query(count_query, True)
            total_count = (
                total_count_result["total_count"].iloc[0]
                if not total_count_result.empty
                else 0
            )
            logging.info("Query executed for e911customers")
            headers_map = get_headers_mapping(
                tenant_database,
                ["E911 Customers"],
                role_name,
                "",
                "",
                "",
                "",
                data,
                database,
            )

            # Check if result is empty
            if result.empty:
                return_json_data.update(
                    {
                        "flag": False,
                        "data": [],
                        "message": "No data found for E911 Customers!",
                    }
                )
                logging.info("No data found for E911 Customers")
                return return_json_data

            # Process and format the data
            df_dict = result.to_dict(orient="records")
            # total = len(df_dict)
            df_dict = convert_timestamp_data(df_dict, tenant_time_zone)
            df_dict = serialize_data(df_dict)
            pages = {"start": start_page, "end": end_page, "total": int(total_count)}

            logging.info("Returning data response")
            return_json_data.update(
                {
                    "flag": True,
                    "message": "Data fetched successfully",
                    "data": df_dict,
                    "tenant_name": tenant_list,
                    "headers_map": headers_map,
                    "pages": pages,
                    "total_resellers": total_resellers,
                }
            )
            return return_json_data

    except Exception as e:
        logging.exception("An error occurred in people_list_view")
        # Get headers mapping
        headers_map = get_headers_mapping(
            tenant_database, [module_name], role_name, "", "", "", "", data, database
        )
        response = {
            "flag": False,
            "message": "Data fetched failure",
            "headers_map": headers_map,
            "pages": {},
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
