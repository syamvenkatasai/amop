"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""

# Importing the necessary Libraries
from common_utils.db_utils import DB
from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging
from usage_device_notifications import UsageNotifications
# from sim_management.sim_management import get_headers_mappings
from common_utils.permission_manager import PermissionManager
from psycopg2 import DatabaseError
import os
import requests

# import datetime
# from datetime import datetime
import time
import time as time_module
import json
import random
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import numpy as np
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
import schedule
import pytz
from io import BytesIO
import base64
import re
from pytz import timezone


# Dictionary to store database configuration settings retrieved from environment variables.
# db_config = {
#     'host': "amoppostgres.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
#     'port': "5432",
#     'user':"root",
#     'password':"AmopTeam123"
# }
db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}

logging = Logging(name="notification_services")

def path_func(path,data):
    
    if path == "/total_emails_count":
        result = total_emails_count(data)
    elif path == "/failed_emails_count":
        result = failed_emails_count(data)
    elif path == "/successful_emails_count":
        result = successful_emails_count(data)
    elif path == "/email_templates_count":
        result = email_templates_count(data)
    elif path == "/email_status_pie_chart":
        result = email_status_pie_chart(data)
    elif path == "/email_triggers_by_day":
        result = email_triggers_by_day(data)
    elif path == "/emails_per_trigger_type_weekly":
        result = emails_per_trigger_type_weekly(data)
    elif path == "/no_of_error_emails_weekly":
        result = no_of_error_emails_weekly(data)
    elif path == "/get_notification_rules":
        result = fetch_usage_notification_rules(data)
    elif path == "/Notification_sync":
        result = Notification_sync(data)
    elif path == "/email_list":
        result = email_list(data)
    elif path == "/process_notification_rules":
        result =process_notification_rules(data)
    elif path == "/get_email_details":
        result = get_email_details(data)
    elif path == "/email_template_list_view":
        result = email_template_list_view(data)
    elif path == "/submit_update_copy_status_email_template":
        result = submit_update_copy_status_email_template(data)
    elif path == "/send_report_emails":
        result = send_report_emails()
    elif path == "/killbill_mail_trigger":
        result = killbill_mail_trigger()
    else:
        result = {"flag": False, "error": "Invalid path or method"}

    return result


def convert_timestamp_data(df, tenant_time_zone):
    """Convert timestamp columns in the provided DataFrame to the
    tenant's timezone."""

    # Create a timezone object for the tenant
    target_timezone = pytz.timezone(tenant_time_zone)

    # List of timestamp columns to convert (adjust based on your data)
    timestamp_columns = [
        "created_date",
        "modified_date",
        "deleted_date",
    ]  # Adjust as needed

    # Convert specified timestamp columns to the tenant's timezone
    for col in timestamp_columns:
        if col in df.columns:
            # Convert to datetime if it's not already
            df[col] = pd.to_datetime(df[col], errors="coerce")

            # If the timestamp is naive, localize it to UTC first
            df[col] = df[col].apply(
                lambda x: x.tz_localize("UTC") if x.tzinfo is None else x
            )

            # Now convert to the target timezone
            df[col] = df[col].apply(
                lambda x: x.tz_convert(target_timezone).strftime("%m-%d-%Y %H:%M:%S") if pd.notnull(x) else None
            )

    # Replace NaT with None for JSON serialization
    df = df.where(pd.notnull(df), None)

    return df

def fetch_usage_notification_rules(data):
    """
    Fetches usage notification rules and associated metadata for a tenant.

    Args:
        data (dict): A dictionary containing the following keys:
            - db_name (str): The tenant's database name (default: "altaworx_central").
            - role_name (str): The role name for header mapping (optional).
            - mod_pages (dict): Pagination details with "start" (default: 0) and "end" (default: 100).
            - username, user_name, user (str): The username of the requester (optional, takes any of the keys).
            - tenant_name, tenant (str): The name of the tenant (optional, takes any of the keys).

    Returns:
        dict: A dictionary containing:
            - flag (bool): True if the data was fetched successfully, False otherwise.
            - rule_data (list): List of dictionaries with rule definitions.
            - operation_types (list): List of operator types as dictionaries with name and ID.
            - field_types (dict): Mapping of field types to their associated fields.
            - expression_type (list): List of expression types (names).
            - service_providers (list): List of service provider names.
            - headers_map (dict): Header mappings for usage notifications.
            - customer_drop_down (dict): Dropdown data for customers and groups.
            - pages (dict): Pagination information including total number of rules.
    """
    logging.info("Data received:", data)
    start_time = time.time()

    # Initialize database connections
    tenant_database_name = data.get("db_name", "altaworx_central")
    tenant_database = DB(tenant_database_name, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

    # Extract parameters from input data
    role_name = data.get("role_name", "")
    ui_mod_pages = data.get("mod_pages", {})
    end = ui_mod_pages.get("end", 100)
    start = ui_mod_pages.get("start", 0)
    user_name = data.get("username") or data.get("user_name") or data.get("user")
    tenant_name = data.get("tenant_name") or data.get("tenant")

    try:
        # Fetch rule definitions with pagination
        rule_data_df = tenant_database.get_data(
            "rule_rule_definition",
            {"is_active": True},
            order={"modified_date": "desc"},
            mod_pages={"start": start, "end": end}
        )
        rule_data_df["id"] = rule_data_df["id"].astype(str)

        # Fetch tenant timezone
        tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
        tenant_timezone = common_utils_database.execute_query(tenant_timezone_query, params=[tenant_name])

        if tenant_timezone.empty or tenant_timezone.iloc[0]["time_zone"] is None:
            raise ValueError("No valid timezone found for tenant.")
        
        tenant_time_zone = tenant_timezone.iloc[0]["time_zone"]

        # Convert timestamps to tenant timezone
        rule_data_df = convert_timestamp_data(rule_data_df, tenant_time_zone)
        rule_data_df["modified_date"] = pd.to_datetime(rule_data_df['modified_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        rule_data_df["created_date"] = pd.to_datetime(rule_data_df['created_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        rule_data_df["object_type_id"] = rule_data_df["object_type_id"].astype(str)
        rule_data_df["created_date"] = rule_data_df["created_date"].astype(str)
        rule_data_df["modified_date"] = rule_data_df["modified_date"].astype(str)
        rule_data_df["deleted_date"] = rule_data_df["deleted_date"].astype(str)
        rule_data_df["rule_id_1_0"] = rule_data_df["rule_id_1_0"].astype(str)
        rule_data = rule_data_df.to_dict(orient="records")

        # Fetch total rule count for pagination
        query_count = "SELECT COUNT(*) AS total_rows FROM rule_rule_definition WHERE is_active=true;"
        count = tenant_database.execute_query(query_count, True)['total_rows'].to_list()[0]
        ui_mod_pages['total'] = count

        # Fetch additional metadata
        expression_type = tenant_database.get_data("rule_expression_type", {"is_active": True}, ["name"])["name"].to_list()
        operation_types = tenant_database.get_data("rule_operator_type", {"is_active": True}, ["name", "id"]).to_dict(orient="records")
        operation_types = [{item["name"]: str(item["id"])} for item in operation_types]

        # Fetch field types and mappings
        query = """
            SELECT CONCAT(rf.display_name, '(', ro.name, ')') as field_name, dt.data_type_name,
                   CONCAT(rf.id, '_', ro.id) as id
            FROM rule_field rf
            JOIN data_type as dt ON dt.id = rf.data_type_id
            JOIN rule_object_type as ro ON ro.id = rf.object_type_id
            WHERE rf.is_active=true
        """
        field_name_df = tenant_database.execute_query(query, True).to_dict(orient="records")
        field_types = {}
        for field in field_name_df:
            if field["data_type_name"] not in field_types:
                if field["data_type_name"] == "String":
                    if "text" not in field_types:
                        field_types["text"] = []
                elif (field["data_type_name"] == "BigInteger"or field["data_type_name"] == "Integer"):
                    if "Whole Numbers" not in field_types:
                        field_types["Whole Numbers"] = []
                else:
                    field_types[field["data_type_name"]] = []
            if field["data_type_name"] == "String":
                field_types["text"].append({field["field_name"]: field["id"]})
            elif (
                field["data_type_name"] == "BigInteger"
                or field["data_type_name"] == "Integer"
            ):
                field_types["Whole Numbers"].append({field["field_name"]: field["id"]})
            else:
                field_types[field["data_type_name"]].append(
                    {field["field_name"]: field["id"]}
                )

        operation_types = tenant_database.get_data(
            "rule_operator_type", {"is_active": True}, ["name", "id"]
        ).to_dict(orient="records")
        operation_types = [{item["name"]: str(item["id"])} for item in operation_types]

        # Fetch customer dropdown data
        customer_names = tenant_database.get_data("customers", {"is_active": True}, ["customer_name", "id"]).to_dict(orient="records")
        customer_groups = tenant_database.get_data("customergroups", {"is_active": True}, ["name", "id"]).to_dict(orient="records")
        customer_drop_down = {
            "customer_names": [{item["customer_name"]: str(item["id"])} for item in customer_names],
            "customer_groups": [{item["name"]: str(item["id"])} for item in customer_groups],
        }

        # Fetch service providers and header mappings
        service_providers = list(set(tenant_database.get_data(
            "serviceprovider", {"is_active": True}, ["service_provider_name"]
        )["service_provider_name"].to_list()))
        headers_map = get_headers_mapping(
            tenant_database_name, ["Usage Notification"], role_name, "", "", "", "", data
        )

        # Log successful audit
        time_consumed = int(float(f"{time.time() - start_time:.4f}"))
        audit_data_user_actions = {
            "service_name": "Notifications",
            "created_date": start_time,
            "created_by": user_name,
            "status": str(True),
            "time_consumed_secs": time_consumed,
            "tenant_name": tenant_name,
            "comments": "Fetching the usage notification data",
            "module_name": "fetch_usage_notification_rules",
            "request_received_at": start_time,
        }
        common_utils_database.update_audit(audit_data_user_actions, "audit_user_actions")

        # Return fetched data
        return {
            "flag": True,
            "rule_data": rule_data,
            "operation_types": operation_types,
            "field_types": field_types,
            "expression_type": expression_type,
            "service_providers": service_providers,
            "headers_map": headers_map,
            "customer_drop_down": customer_drop_down,
            "pages": ui_mod_pages
        }

    except Exception as e:
        logging.error("Error encountered:", e)
        try:
            # Log error in audit table
            error_data = {
                "service_name": "Notifications",
                "created_date": start_time,
                "error_messag": str(e),
                "error_type": str(type(e)),
                "user": user_name,
                "tenant_name": tenant_name,
                "comments": str(e),
                "module_name": "fetch_usage_notification_rules",
                "request_received_at": start_time,
            }
            common_utils_database.log_error_to_db(error_data, "error_table")
        except Exception as nested_e:
            logging.warning("Exception at updating the error table:", nested_e)
        return {"flag": False, "message": "Error while fetching data"}


def get_Expression(expression):
    object_type=''
    expresion_id=[]
    for exp in expression:
        temp=exp
        if 'field_one' in exp and type(exp["field_one"])==dict and exp["field_one"]:
            key=list(exp["field_one"].keys())[0]
            value=exp["field_one"][key]
            values=value.split('_')
            value=values[0]
            object_type=values[1]
            temp["field_one"]={key:value}
        elif 'field_one' in exp:
            temp["field_one"]=exp["field_one"]
        if 'field_two' in exp and type(exp["field_two"])==dict and exp["field_two"]:
            key=list(exp["field_two"].keys())[0]
            value=exp["field_two"][key]
            values=value.split('_')
            value=values[0]
            object_type=values[1]
            temp["field_two"]={key:value}
        elif 'field_two' in exp:
            temp["field_two"]=exp["field_two"]
        expresion_id.append(temp)
    return expresion_id,object_type


def process_notification_rules(data):
    """
    Processes notification rules based on the provided action: create, edit, or delete.

    Args:
        data (dict): Input data containing details of the rule and the action to perform.

    Returns:
        dict: Result of the operation, including success flag and message.
    """
    # Log the incoming data
    logging.info("here data received is", data)

    # Initialize variables and database connections
    start_time = time.time()
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    tenant_database_name = data.get("db_name", "altaworx_central")
    tenant_database = DB(tenant_database_name, **db_config)
    session_id = data.get("session_id", "")
    # Extract input details
    role_name = data.get("role_name", "")
    user_name = data.get("username") or data.get("user_name") or data.get("user")
    tenant_name = data.get("tenant_name") or data.get("tenant")
    rule_data = data.get("changed_data")
    action = data.get("action")

    try:
        # Prepare the rule definition dictionary with required fields
        rule_rule_definition = {
            'rule_id': rule_data['rule_id'],
            'name': rule_data['name'],
            'subject_line': rule_data['subject_line'],
            'display_message': rule_data['display_message'],
            'notes': rule_data['notes'],
            'order_of_execution': str(rule_data['order_of_execution']),
            'modified_by': user_name,
            'should_show_projected_usage_cost': str(rule_data.get('show_projected_usage_cost', False)),
            'expression_names': json.dumps(rule_data['expression_names']),
            'service_provider': rule_data['service_provider']
        }

        # Extract service provider name if it's a dictionary
        if 'label' in rule_rule_definition['service_provider']:
            rule_rule_definition['service_provider'] = rule_rule_definition['service_provider']['label']

        # Handle customer list
        if "customer_list" in rule_data and rule_data['customer_list'] == 'All':
            # Fetch all active customers and groups
            customer_names = tenant_database.get_data(
                "customers", {"is_active": True}, ["customer_name", "id"]
            ).to_dict(orient="records")
            customer_names = [{item["customer_name"]: str(item["id"])} for item in customer_names]

            customer_groups = tenant_database.get_data(
                "customergroups", {"is_active": True}, ["name", "id"]
            ).to_dict(orient="records")
            customer_groups = [{item["name"]: str(item["id"])} for item in customer_groups]

            # Prepare the customer dropdown structure
            customer_drop_down = {
                "customer_names": customer_names,
                "customer_groups": customer_groups,
            }
            rule_rule_definition['send_to_customer'] = "True"
            rule_rule_definition['customers_list'] = json.dumps(customer_drop_down)
        else:
            # Use the provided customer list
            rule_rule_definition['send_to_customer'] = "False"
            rule_rule_definition['customers_list'] = json.dumps(rule_data.get('customer_list', []))

        # Extract expressions and object type IDs
        expression_ids, object_type_ids = get_Expression(rule_data['expression_names'])
        rule_rule_definition['expression_ids'] = json.dumps(expression_ids)
        rule_rule_definition['object_type_id'] = object_type_ids

        # Handle system admin notification flag
        rule_rule_definition['send_to_carrier'] = str(rule_data.get('send_to_system_admin', False))

        # Perform actions based on the 'action' parameter
        if action == 'create':
            # Set additional fields for creation
            rule_rule_definition['is_active'] = "True"
            rule_rule_definition['is_deleted'] = "False"
            rule_rule_definition['created_by'] = user_name
            rule_rule_definition['version_no'] = '1'

            # Insert the new rule and update its ID
            rule_def_id = tenant_database.insert_data(rule_rule_definition, 'rule_rule_definition')
            tenant_database.update_dict('rule_rule_definition', {"rule_def_id": rule_def_id}, {'id': rule_def_id})

        elif action == 'edit':
            # Mark the existing rule as inactive and deleted
            where_column = rule_data['rule_def_id']
            update = {'is_active': "False", 'is_deleted': "True"}
            tenant_database.update_dict('rule_rule_definition', update, {'rule_def_id': where_column})

            # Insert the updated rule as a new version
            rule_rule_definition.update({
                'is_active': "True",
                'is_deleted': "False",
                'created_by': user_name,
                'version_no': str(int(rule_data['version_no']) + 1),
                'rule_def_id': rule_data['rule_def_id']
            })
            tenant_database.insert_data(rule_rule_definition, 'rule_rule_definition')
            
            rule_def_id=rule_data['rule_def_id']

        elif action == 'delete':
            # Mark the rule as deleted
            where_column = rule_data['rule_def_id']
            rule_rule_definition = {
                'is_active': "False",
                'is_deleted': "True",
                'deleted_by': user_name
            }
            tenant_database.update_dict('rule_rule_definition', rule_rule_definition, {'rule_def_id': where_column})

            rule_def_id=rule_data['rule_def_id']

        # Record audit information
        try:
            end_time = time.time()
            time_consumed = int(float(f"{end_time - start_time:.4f}"))
            audit_data_user_actions = {
                "service_name": "Notifications",
                "created_date": start_time,
                "created_by": user_name,
                "status": str(True),
                "time_consumed_secs": time_consumed,
                "tenant_name": tenant_name,
                'session_id':session_id,
                "comments": "Process the Usage notification data",
                "module_name": "process_notification_rules",
                "request_received_at": start_time,
            }
            common_utils_database.update_audit(audit_data_user_actions, "audit_user_actions")
        except Exception as e:
            logging.exception(f"Exception while recording audit: {e}")

        # Return success response
        return {"flag": True, "Message": "Data Processed Successfully","rule_def_id":str(rule_def_id)}

    except Exception as e:
        # Log the error and update the error table
        logging.error("Error encountered: ", e)
        try:
            error_data = {
                "service_name": "Notifications",
                "created_date": start_time,
                "error_message": str(e),
                'session_id':session_id,
                "error_type": e,
                "user": user_name,
                "tenant_name": tenant_name,
                "comments": str(e),
                "module_name": "process_notification_rules",
                "request_received_at": start_time,
            }
            common_utils_database.log_error_to_db(error_data, "error_table")
        except Exception as e:
            logging.warning("Exception while updating the error table: ", e)

        # Return failure response
        return {"flag": False, "message": "Error while Processing data"}
    

def Notification_sync(data):

    logging.info("here data received is", data)
    # Initialize variables and database connections
    try:
        sync_notification=UsageNotifications()
        sync_notification.usage_notification(data)
    except Exception as e:
        logging.exception(f"Exception while recording audit: {e}")

    # Return success response
    return {"flag": True, "Message": "Data Synced Successfully"}


def get_headers_mapping(
    tenant_database,
    module_list,
    role,
    user,
    tenant_id,
    sub_parent_module,
    parent_module,
    data,
):
    """
    Description: The  function retrieves and organizes field mappings,headers,and module features
    based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields,and
    compiles features into a structured dictionary for each module.
    """
    ##Database connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    feature_module_name = data.get("feature_module_name", "")
    user_name = data.get("username") or data.get("user_name") or data.get("user")
    tenant_name = data.get("tenant_name") or data.get("tenant")
    try:
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": tenant_name}["id"]
        )["id"].to_list()[0]
    except Exception as e:
        logging.warning(f"Getting exception at fetching tenant id {e}")
    ret_out = {}
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

        except Exception as e:
            logging.exception(f"there is some error {e}")
            pass
        # Add the final features to the headers dictionary
        headers["module_features"] = final_features
        ret_out[module_name] = headers

    return ret_out


def get_features_by_feature_name(
    user_name, tenant_id, feature_name, common_utils_database
):
    # db = DB('common_utils', **db_config)
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


def total_emails_count(data):
    """
    Fetches the total number of emails that got triggered from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - partner (str): The partner name for filtering emails.
            - start_date (str): The start date for filtering emails (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering emails (format: YYYY-MM-DD).
            - email_type (str): The type of email to filter by('Application','AWS','Infra'or'all').
    Returns:
        dict: containing the status of total_emails_count and the data card information.
    """
    partner_name = data.get("partner")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    email_type = data.get("email_type")

    if email_type == "All":
        query = """
            SELECT COUNT(*)
            FROM email_audit
            WHERE partner_name = %s
              AND email_status != 'N/A'
              AND created_date BETWEEN %s AND %s
              AND email_type IN ('Application', 'AWS')
        """
        params = [
            partner_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]
    else:
        query = """
            SELECT COUNT(*)
            FROM email_audit
            WHERE partner_name = %s
              AND email_status != 'N/A'
              AND created_date BETWEEN %s AND %s
              AND email_type = %s
        """
        params = [
            partner_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
            email_type,
        ]
    # Database connection
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Fetch the count and ensure it's a standard Python int
        if isinstance(res, pd.DataFrame) and not res.empty:
            total_emails = int(res.iloc[0, 0])  # Convert to standard Python int
        else:
            total_emails = 0
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "Total Emails Sent",
                "chart_type": "data",
                "data": total_emails,
                "icon": "useroutlined",
                "height": 100,
                "width": 300,
            },
        }
    except (DatabaseError, ValueError) as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching total emails",
        }
    return response


def failed_emails_count(data):
    """
    Fetches the count of emails that failed to deliver from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - partner (str): The partner name for filtering emails.
            - start_date (str): The start date for filtering emails (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering emails (format: YYYY-MM-DD).
            - email_type (str): The type of email to filter by('Application','AWS','Infra',or'all').
    Returns:
        dict: containing the status of failed_emails_count and the data card information.
    """
    partner_name = data.get("partner")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    email_type = data.get("email_type")
    if email_type == "All":
        query = """
            SELECT COUNT(*)
            FROM email_audit
            WHERE partner_name = %s AND created_date BETWEEN %s AND %s
            AND email_status != 'N/A'
            AND email_type IN ('Application', 'AWS') AND email_status = 'failure'
            """
        params = [
            partner_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]
    else:
        query = """
            SELECT COUNT(*)
            FROM email_audit
            WHERE partner_name = %s AND created_date BETWEEN %s AND %s
            AND email_status != 'N/A'
            AND email_type = %s AND email_status = 'failure'
            """
        params = [
            partner_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
            email_type,
        ]
    # Database connection
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Fetch the count and ensure it's a standard Python int
        if isinstance(res, pd.DataFrame) and not res.empty:
            failed_emails = int(res.iloc[0, 0])  # Convert to standard Python int
        else:
            failed_emails = 0
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "No: of Emails Failed",
                "chart_type": "data",
                "data": failed_emails,
                "icon": "useroutlined",
                "height": 100,
                "width": 300,
            },
        }
    except (DatabaseError, ValueError) as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching the count of failed emails",
        }
    return response


def successful_emails_count(data):
    """
    Fetches the total number of successfully delivered emails from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - partner (str): The partner name for filtering emails.
            - start_date (str): The start date for filtering emails (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering emails (format: YYYY-MM-DD).
            - email_type (str): The type of email to filter by('Application','AWS','Infra'or'all').
    Returns:
        dict:containing the status of  successful_emails_count and card information.
    """
    partner_name = data.get("partner")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    email_type = data.get("email_type")
    if email_type.lower() == "all":
        query = """
            SELECT COUNT(*)
            FROM email_audit
            WHERE partner_name = %s AND created_date BETWEEN %s AND %s
            AND email_type IN ('Application', 'AWS') AND email_status = 'success'
        """
        params = [
            partner_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]
    else:
        query = """
            SELECT COUNT(*)
            FROM email_audit
            WHERE partner_name = %s AND created_date BETWEEN %s AND %s
            AND email_type = %s AND email_status = 'success'
        """
        params = [
            partner_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
            email_type,
        ]
    # Database connection
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Fetch the count and ensure it's a standard Python int
        if isinstance(res, pd.DataFrame) and not res.empty:
            successful_emails = int(res.iloc[0, 0])  # Convert to standard Python int
        else:
            successful_emails = 0
        response = {
            "flag": True,
            "data": {
                "title": "No: of Emails Successful",
                "chart_type": "data",
                "data": successful_emails,
                "icon": "useroutlined",
                "height": 100,
                "width": 300,
            },
        }
    except (DatabaseError, ValueError) as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching the count of successful emails",
        }
    return response


def email_templates_count(data):
    """
    Fetches the count of email templates from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - partner (str): The partner name for filtering email templates.
            - start_date (str): The start date for filtering email templates (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering email templates (format: YYYY-MM-DD).
    Returns:
        dict:containing the status of the email_templates_count and card information.
    """
    partner_name = data.get("partner")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    email_type = data.get("email_type")
    if email_type.lower() == "all":
        query = """
        SELECT COUNT(*)
        FROM email_templates
        WHERE partner_name = %s
        AND email_type IN ('Application', 'AWS') AND email_status = true
        """
        params = [partner_name]
    else:
        query = """
        SELECT COUNT(*)
        FROM email_templates
        WHERE partner_name = %s
        AND email_type = %s AND email_status = true
        """
        params = [partner_name, email_type]
    # Database connection
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Fetch the count and ensure it's a standard Python int
        if isinstance(res, pd.DataFrame) and not res.empty:
            email_templates_count = int(
                res.iloc[0, 0]
            )  # Convert to standard Python int
        else:
            email_templates_count = 0
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "No: of Email Templates",
                "chart_type": "data",
                "data": email_templates_count,
                "icon": "useroutlined",
                "height": 100,
                "width": 300,
            },
        }
    except (DatabaseError, ValueError) as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching the count of email templates",
        }
    return response


def email_status_pie_chart(data):
    """
    Fetches the count of total emails triggered vs successful and unsuccessful emails.
    Args:
        data (dict): A dictionary containing the following keys:
            - partner (str): The partner name for filtering emails.
            - start_date (str): The start date for filtering emails (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering emails (format: YYYY-MM-DD).
    Returns:
        dict:containing the status of email_status_pie_chart and pie chart data.
    """
    partner_name = data.get("partner")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    email_type = data.get("email_type")
    if email_type.lower() == "all":
        query = """
            SELECT email_status, COUNT(*)
            FROM email_audit
            WHERE partner_name = %s
            AND created_date >= %s
            AND created_date < %s
            AND email_status != 'N/A'
            GROUP BY email_status;
        """
        # Adjust end_date to the day after
        params = [
            partner_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]
    else:
        query = """
            SELECT email_status, COUNT(*)
            FROM email_audit
            WHERE partner_name = %s
            AND created_date >= %s
            AND created_date < %s
            AND email_status != 'N/A'
            AND email_type = %s
            GROUP BY email_status
        """
        # Adjust end_date to the day after
        params = [
            partner_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
            email_type,
        ]
    # Database connection
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        if hasattr(res, "fetchall"):  # Check if 'res' has 'fetchall' method
            email_status_counts = res.fetchall()
        else:
            email_status_counts = res
        if isinstance(email_status_counts, list):
            df = pd.DataFrame(email_status_counts, columns=["email_status", "count"])
        else:
            df = email_status_counts
        # Process results into pie chart format
        status_labels = {"sent": "Sent", "success": "Successful", "failure": "Error"}
        data_card = [
            {
                "type": status_labels.get(row["email_status"], "Unknown"),
                "value": row["count"],
            }
            for index, row in df.iterrows()
        ]
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "Successful Vs Error",
                "chart_type": "pie",
                "data": data_card,
                "angleField": "value",
                "colorField": "type",
                "radius": 0.8,
                "innerRadius": 0.6,
                "height": 300,
                "width": 500,
            },
        }
    except (DatabaseError, ValueError) as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching email status counts",
        }
    return response


def email_triggers_by_day(data):
    """
    Fetches the count of total emails triggered per day in the specified week.

    Args:
        data (dict): A dictionary containing the following keys:
            - partner (str): The partner name for filtering emails.
            - start_date (str): The start date for filtering emails (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering emails (format: YYYY-MM-DD).
            - email_type (str): The type of email to filter by.

    Returns:
        dict: A dictionary containing the status of the email_triggers_by_day and the bar chart data.
    """
    # Get today's date and calculate the start and end of the current week
    today = datetime.now().date()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)  # Corrected line

    # Update the date range in data to the current week
    data["start_date"] = start_of_week.strftime("%Y-%m-%d")
    data["end_date"] = end_of_week.strftime("%Y-%m-%d")

    # Extract parameters from data
    email_type = data.get("email_type", "all")
    partner_name = data.get("partner")
    if email_type.lower() == "all":
        query = """
        SELECT DATE(created_date) AS date, COUNT(*) AS count
        FROM email_audit
        WHERE  partner_name = %s AND created_date BETWEEN %s AND %s
        AND email_type IN ('Application', 'AWS')
        AND email_status != 'N/A'
        GROUP BY DATE(created_date)
        ORDER BY DATE(created_date)
        """
        params = [partner_name, data["start_date"], data["end_date"]]
    else:
        query = """
        SELECT DATE(created_date) AS date, COUNT(*) AS count
        FROM email_audit
        WHERE  partner_name = %s AND created_date BETWEEN %s AND %s
        AND email_type = %s
        AND email_status != 'N/A'
        GROUP BY DATE(created_date)
        ORDER BY DATE(created_date)
        """
        params = [partner_name, data["start_date"], data["end_date"], email_type]
    try:

        logging.info(
            "Executing query:", query % tuple(params)
        )  # Debug: logging.info the query with params

        # Database connection
        database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

        # Execute query with parameters
        res = database.execute_query(query, params=params)

        # Check if the result is a DataFrame and process it
        if isinstance(res, pd.DataFrame):
            daily_counts = res.set_index("date")["count"].to_dict()
        else:
            daily_counts = dict(res.fetchall()) if res else {}

        # Convert dates in daily_counts keys to string format if not already
        daily_counts = {
            date.strftime("%Y-%m-%d"): count for date, count in daily_counts.items()
        }

        # Prepare the data for the entire week with zeros for days with no email triggers
        data_card = []
        days_of_week = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]  # Day names

        for i in range(7):
            day = start_of_week + timedelta(days=i)
            date_str = day.strftime("%Y-%m-%d")
            count = daily_counts.get(date_str, 0)  # Ensure date format matches
            data_card.append(
                {
                    "day": days_of_week[i],  # Use day name from days_of_week list
                    "value": count,
                }
            )

        # Log the final data_card for debugging
        logging.debug("#####data_card", data_card)

        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "Total No: of Email Sent",
                "chart_type": "bar",
                "data": data_card,
                "xField": "day",  # Now using 'day' instead of 'type'
                "yField": "value",
                "smooth": True,
                "height": 300,
                "width": 500,
            },
        }

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": False,
            "message": "Something went wrong fetching email triggers by day",
        }

    return response


def emails_per_trigger_type_weekly(data):
    """
    the count of emails triggered per day for week starting from nearest Monday and email type.
    Args:
        data (dict): A dictionary containing the following keys:
            - start_date (str): The start date for filtering emails (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering emails (format: YYYY-MM-DD).
            - email_type (str): The type of email to filter by.
    Returns:
        dict: containing the status of the emails_per_trigger_type_weekly and bar chart data.
    """
    # Extract parameters from data
    email_type = data.get("email_type", "all")
    partner_name = data.get("partner")
    # Determine the start of the week (Monday) and end of the week (Sunday)
    today = datetime.now().date()
    start_date = today - timedelta(days=today.weekday())
    end_date = start_date + timedelta(days=6)
    # Adjust start_date to Monday
    start_date = start_date - timedelta(days=start_date.weekday())
    end_date = start_date + timedelta(days=6)
    if email_type.lower() == "all":
        query = """
        SELECT DATE(created_date) AS date, email_type, COUNT(*) AS count
        FROM email_audit
        WHERE partner_name = %s
        AND created_date BETWEEN %s AND %s
        AND email_type IN ('Application', 'AWS')
        AND email_status != 'N/A'
        GROUP BY DATE(created_date), email_type
        ORDER BY DATE(created_date)
        """
        params = [
            partner_name,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        ]
    else:
        query = """
        SELECT DATE(created_date) AS date, email_type, COUNT(*) AS count
        FROM email_audit
        WHERE partner_name = %s
        AND created_date BETWEEN %s AND %s
        AND email_type = %s
        AND email_status != 'N/A'
        GROUP BY DATE(created_date), email_type
        ORDER BY DATE(created_date)
        """
        params = [
            partner_name,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            email_type,
        ]
    try:
        # Database connection
        database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Check if the result is a DataFrame and process it
        if isinstance(res, pd.DataFrame):
            daily_counts = (
                res.pivot(index="date", columns="email_type", values="count")
                .fillna(0)
                .to_dict(orient="index")
            )
        else:
            daily_counts = dict(res.fetchall()) if res else {}
        # Log the daily_counts for debugging
        # Convert dates in daily_counts keys to string format if not already
        daily_counts = {
            date.strftime("%Y-%m-%d"): {email_type: count}
            for date, count in daily_counts.items()
        }
        # Prepare the data for the entire week with zeros for days with no email triggers
        data_card = []
        days_of_week = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        for i in range(7):  # Fixed to 7 days for a week
            day = start_date + timedelta(days=i)
            date_str = day.strftime("%Y-%m-%d")
            count = daily_counts.get(date_str, {}).get(
                email_type, 0
            )  # Ensure date format matches and email_type filter
            data_card.append({"day": days_of_week[i], "value": count})
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "Email Types",
                "chart_type": "stacked-bar",
                "data": data_card,
                "xField": "type",
                "yField": "value",
                "isStack": True,
                "smooth": True,
                "height": 300,
                "width": 500,
            },
        }
    except (DatabaseError, ValueError) as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching weekly email triggers by type",
        }
    return response


def no_of_error_emails_weekly(data):
    """
    count of error emails triggered per day in week starting from nearest Monday and email type.
    Args:
        data (dict): A dictionary containing the following keys:
            - start_date (str): The start date for filtering emails (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering emails (format: YYYY-MM-DD).
            - email_type (str): The type of email to filter by.

    Returns:
        dict:containing the status of the no_of_error_emails_weekly and the bar chart data.
    """
    # Extract parameters from data
    email_type = data.get("email_type", "All")
    partner_name = data.get("partner")
    # Determine the start of the week (Monday) and end of the week (Sunday)
    today = datetime.now().date()
    start_date = today - timedelta(days=today.weekday())
    end_date = start_date + timedelta(days=6)
    # Adjust start_date to Monday
    start_date = start_date - timedelta(days=start_date.weekday())
    end_date = start_date + timedelta(days=6)
    if email_type.lower() == "all":
        query = """
        SELECT DATE(created_date) AS date, COUNT(*) AS count
        FROM email_audit
        WHERE email_status = 'failure' AND partner_name = %s
        AND created_date BETWEEN %s AND %s
        AND email_type IN ('Application', 'AWS')
        GROUP BY DATE(created_date)
        ORDER BY DATE(created_date)
        """
        params = [
            partner_name,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        ]
    else:
        query = """
        SELECT DATE(created_date) AS date, COUNT(*) AS count
        FROM email_audit
        WHERE email_status = 'failure' AND partner_name = %s
        AND created_date BETWEEN %s AND %s
        AND email_type = %s
        GROUP BY DATE(created_date)
        ORDER BY DATE(created_date)
         """
        params = [
            partner_name,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            email_type,
        ]
    try:
        # Database connection
        database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Check if the result is a DataFrame and process it
        if isinstance(res, pd.DataFrame):
            daily_counts = res.set_index("date")["count"].to_dict()
        else:
            daily_counts = {row[0]: row[1] for row in res.fetchall()} if res else {}
        # Convert datetime.date keys to string format consistency
        daily_counts_str = {
            date.strftime("%Y-%m-%d"): count for date, count in daily_counts.items()
        }
        # Prepare the data for the entire week with zeros for days with no error emails
        data_card = []
        days_of_week = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        for i in range(7):  # Fixed to 7 days for a week
            day = start_date + timedelta(days=i)
            date_str = day.strftime("%Y-%m-%d")
            logging.debug("####date_str", date_str)
            count = daily_counts_str.get(date_str, 0)  # Ensure date format matches
            logging.debug("####count###", count)
            data_card.append({"day": days_of_week[i], "value": count})
        response = {
            "flag": True,
            "data": {
                "title": "No: of Email Errors",
                "chart_type": "bar_two",
                "data": data_card,
                "xField": "day",
                "yField": "value",
                "smooth": True,
                "height": 300,
                "width": 500,
            },
        }
    except (DatabaseError, ValueError) as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching weekly error email counts",
        }
    return response


def email_list(data):
    """
    Fetches a list of emails that got triggered based on filters and pagination.
    Args:
        data (dict): A dictionary containing the following keys:
            - partner (str): The partner name for filtering emails.
            - start_date (str): The start date for filtering emails (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering emails (format: YYYY-MM-DD).
            - email_type (str): The type of email to filter by.
            - limit (int): The number of records to fetch (pagination).
            - offset (int): The starting point for the records to fetch (pagination).
    Returns:
        dict: A dictionary containing the status of the request and the list of emails.
    """
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    tenant_database = data.get("db_name", "altaworx_central")
    role_name = data.get("role_name", "")
    tenant_name = data.get("tenant_name", "")
    # Set default pagination if not provided
    start = data.get("mod_pages", {}).get("start", 0)
    end = data.get("mod_pages", {}).get("end", 100)

    limit = end - start
    offset = start

    # Get tenant's timezone
    tenant_name = data.get("tenant_name", "")
    tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
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

    # Query to get the total number of rows for pagination
    total_count_query = """SELECT COUNT(*) AS total FROM email_audit"""
    total_count_result = database.execute_query(total_count_query, flag=True)
    total_count = int(total_count_result.iloc[0]["total"])

    # Pagination information
    pages_data = {"start": start, "end": end, "total": total_count}

    # Query to fetch the latest records based on created_date
    query = """
        SELECT
            *
        FROM email_audit
        ORDER BY created_date DESC  -- Sorting by latest records
        LIMIT %s OFFSET %s
        """

    # Execute query with limit and offset for pagination
    params = [limit, offset]
    df = database.execute_query(query, params=params)  # Get the DataFrame directly
    df_dict = df.to_dict(orient="records")  # Convert to dictionary for processing

    df_dict = convert_timestamp(df_dict, tenant_time_zone)

    try:
        # Prepare response data
        headers_map = get_headers_mapping(
            tenant_database, ["Notifications"], role_name, "", "", "", "", data
        )
        data_dict_all = {"Notifications": serialize_data(df_dict)}

        response = {
            "flag": True,
            "message": "Data fetched successfully",
            "data": data_dict_all,
            "headers_map": headers_map,
            "pages": pages_data,
        }
        return response

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": False,
            "message": "Something went wrong fetching email list",
            "data": {},  # Return empty dictionary on error
        }
        return response


def get_email_details(data):
    """
    Fetches detailed information about a specific email from the AMOP application.
    Args:
        serial_number (int): The serial number (ID) of the email to retrieve details for.
    Returns:
        dict: A dictionary containing the status of the request and the email details.
    """
    query = """
        SELECT
            partner_name,
            template_name,
            email_type,
            from_mail,
            to_mail,
            cc_mail,
            bcc_mail,
            subject,
            body
        FROM email_template
        WHERE id = %s
        """
    serial_number = data.get("id")
    params = [serial_number]
    # Database connection
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Process the result
        if isinstance(res, pd.DataFrame) and not res.empty:
            email_details = res.iloc[0].to_dict()  # Convert the row to a dictionary
        else:
            email_details = {}
        # Prepare the response
        response = {"flag": True, "data": email_details}
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": False,
            "message": "Something went wrong fetching email details",
        }
    return response


def convert_timestamp(df_dict, tenant_time_zone):
    """Convert timestamp columns in the provided dictionary list to the tenant's timezone."""
    # Create a timezone object
    target_timezone = timezone(tenant_time_zone)

    # List of timestamp columns to convert
    timestamp_columns = [
        "created_date",
        "modified_date",
        "last_email_triggered_at",
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


def email_template_list_view(data):
    """
    Description: Retrieves emails in list view data from the database.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    """
    logging.info(f"Request Data Recieved")
    # database Connection
    database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    # database = DB('AmopAlgouatDB', **db_config)
    tenant_database = data.get("db_name", "")
    # database Connection
    dbs = DB(tenant_database, **db_config)
    role_name = data.get("role_name", "")
    tenant_database = data.get("db_name", "altaworx_central")
    try:
        return_json_data = {}
        start_page = data.get("mod_pages", {}).get("start", 0)
        end_page = data.get("mod_pages", {}).get("end", 100)

        # Query to get total number of rows for pagination
        total_count_query = """SELECT COUNT(*) AS total FROM email_templates"""
        total_count_result = database.execute_query(total_count_query, flag=True)
        total_count = int(total_count_result.iloc[0]["total"])

        # Pagination pages info
        pages = {"start": start_page, "end": end_page, "total": total_count}

        # SQL query to get all columns from the templates table with pagination
        query = """SELECT * FROM email_templates  ORDER BY modified_date DESC, created_date DESC LIMIT 100 OFFSET %s"""
        params = [start_page]
        roles_query = "SELECT DISTINCT(role_name) FROM roles"
        df = database.execute_query(roles_query, True)

        # Convert the 'role_name' column to a list
        role_list = df["role_name"].to_list()

        # Get tenant's timezone
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

        email_credientials = database.get_data(
            "tenant",
            {"tenant_name": "Altaworx", "is_active": True},
            ["from_email", "sendgrid_api"],
        )

        if not email_credientials.empty:
            # Check if both keys exist and have valid values
            if all(email_credientials.get(key).notna().any() for key in ["from_email", "sendgrid_api"]):
                email_condition = True
            else:
                email_condition = False
        else:
            email_condition = False


        ##get create template data
        email_template_table_mapping_dataframe = dbs.get_data(
            "email_template_table_mapping",
            {},
            [
                "id",
                "parent_module_name",
                "sub_module_name",
                "module_name",
                "table_column_mapping",
                "template_names",
            ],
        )
        # Call the function and build the hierarchy
        email_template_table_mapping_dict = build_hierarchy(
            email_template_table_mapping_dataframe
        )
        # Execute the query using the existing execute_query function
        result = database.execute_query(query, params=params)
        headers_map = get_headers_mapping(
            tenant_database, ["Email Template List"], role_name, "", "", "", "", data
        )
        tenant_query = f"SELECT distinct(tenant_name) FROM tenant where is_active=True"
        tenant_list = database.execute_query(tenant_query, True)[
            "tenant_name"
        ].to_list()
        if not result.empty:
            df_dict = result.to_dict(orient="records")
            df_dict = convert_timestamp(df_dict, tenant_time_zone)
            return_json_data["flag"] = True
            return_json_data["message"] = "Data fetched successfully"
            return_json_data["data"] = serialize_data(df_dict)
            return_json_data["partners"] = tenant_list
            return_json_data["role_list"] = role_list
            return_json_data["email_template_table_mapping_dict"] = (
                email_template_table_mapping_dict
            )
            return_json_data["headers_map"] = headers_map
            return_json_data["pages"] = pages
            return_json_data["email_condition"] = email_condition
            return return_json_data
        else:
            logging.error(f"Error occurred: {str(e)}")
            return_json_data["flag"] = False
            return_json_data["data"] = []
            return_json_data["message"] = "Failed!!"
            return return_json_data
    except Exception as e:
        logging.exception(f"Error occurred: {str(e)}")
        return_json_data["flag"] = False
        return_json_data["message"] = "Failed!!"
        return_json_data["data"] = []
        return return_json_data


# Function to build the hierarchy structure with table_column_mapping and template_names
def build_hierarchy(df):
    parent_dict = {}
    # Replace None or NaN values in 'parent_module_name' with ""
    df["parent_module_name"] = df["parent_module_name"].fillna("")

    # Create the hierarchy structure
    for _, row in df.iterrows():
        parent_name = row["parent_module_name"]
        submodule_name = row["sub_module_name"]
        module_name = row["module_name"]
        table_column_mapping = row["table_column_mapping"]
        template_names = row["template_names"]

        if parent_name not in parent_dict:
            parent_dict[parent_name] = {
                "parent_module_name": parent_name,
                "children": [],
            }

        if submodule_name == "None":
            parent_dict[parent_name]["children"].append(
                {
                    "child_module_name": module_name,
                    "table_column_mapping": table_column_mapping,
                    "template_names": template_names,
                    "sub_children": [],
                }
            )
        else:
            child_found = False
            for child in parent_dict[parent_name]["children"]:
                if child["child_module_name"] == submodule_name:
                    child["sub_children"].append(
                        {
                            "sub_child_module_name": module_name,
                            "table_column_mapping": table_column_mapping,
                            "template_names": template_names,
                            "sub_children": [],
                        }
                    )
                    child_found = True
                    break
            if not child_found:
                parent_dict[parent_name]["children"].append(
                    {
                        "child_module_name": submodule_name,
                        "table_column_mapping": None,  # Submodule doesn't have its own table_mapping
                        "template_names": None,  # Submodule doesn't have its own template_names
                        "sub_children": [
                            {
                                "sub_child_module_name": module_name,
                                "table_column_mapping": table_column_mapping,
                                "template_names": template_names,
                                "sub_children": [],
                            }
                        ],
                    }
                )

    # Ensure 'None' parent module is included
    if "None" not in parent_dict:
        parent_dict["None"] = {"parent_module_name": "None", "children": []}

    # Convert dictionary to list and sort, handling 'Unknown' values
    parent_list = sorted(
        parent_dict.values(),
        key=lambda x: (x["parent_module_name"] == "Unknown", x["parent_module_name"]),
    )

    return {"Modules_mapping": parent_list}


def convert_booleans(data):
    for key, value in data.items():
        if isinstance(value, str) and value.lower() == "true":
            data[key] = True
        elif isinstance(value, str) and value.lower() == "false":
            data[key] = False
        elif isinstance(value, dict):  # Recursively process nested dictionaries
            convert_booleans(value)
    return data  # Return the modified dictionary


def submit_update_copy_status_email_template(data):
    """
    Updates email template data for a specified module by checking user and tenant permissions.
    Constructs and executes SQL queries to fetch and manipulate data, handles errors, and logs relevant information.
    """
    data = convert_booleans(data)
    changed_data = data.get("changed_data", {})
    copy_data = data.get("copy_data", {})
    new_data = {k: v for k, v in data.get("new_data", {}).items() if v}
    status_data = data.get("status_data", {})
    unique_id = changed_data.get("id")
    table_name = data.get("table_name", "")
    action = data.get("action", "")
    # Database connection setup
    dbs = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

    # Prepare email audit data variables
    email_audit_data = {}

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
            new_data["email_status"] = False
            if new_data:
                dbs.insert_data(new_data, table_name)
                logging.info("Data insertion was successful.")
                email_audit_data = {
                    "template_name": new_data.get("template_name", ""),
                    "email_type": "Application",
                    "partner_name": new_data.get("partner_name", ""),
                    "to_email": new_data.get("to_mail", ""),
                    "cc_email": new_data.get("cc_mail", ""),
                    "comments": "",
                    "subject": new_data.get("subject", ""),
                    "body": new_data.get("body", ""),
                    "role": new_data.get("role", ""),
                    "parents_module_name": new_data.get("parents_module_name", ""),
                    "sub_module_name": new_data.get("sub_module_name", ""),
                    "child_module_name": new_data.get("child_module_name", ""),
                }
                email_audit_data["action"] = "Template created"
                email_audit_data["email_status"] = "success"
                common_utils_database.update_audit(email_audit_data, "email_audit")
        elif action == "update":
            if unique_id is not None:
                changed_data = {
                    k: v for k, v in changed_data.items() if v not in [None, "None"]
                }
                update_data = {
                    key: value for key, value in changed_data.items() if key != "id"
                }

                # Convert list of dictionaries in the attachments field to JSON string
                if "attachments" in update_data and isinstance(
                    update_data["attachments"], list
                ):
                    update_data["attachments"] = json.dumps(update_data["attachments"])

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

                if update_data:
                    dbs.update_dict(table_name, update_data, {"id": unique_id})
                    email_audit_data = {
                        "template_name": update_data.get("template_name", ""),
                        "email_type": "Application",
                        "partner_name": update_data.get("partner_name", ""),
                        "cc_email": update_data.get("cc_mail", ""),
                        "to_email": update_data.get(
                            "to_mail", ""
                        ),  # Ensure correct column name
                        "comments": "",
                        "subject": update_data.get("subject", ""),
                        "body": update_data.get("body", ""),
                        "role": update_data.get("role", ""),
                        "parents_module_name": update_data.get(
                            "parents_module_name", ""
                        ),
                        "sub_module_name": update_data.get("sub_module_name", ""),
                        "child_module_name": update_data.get("child_module_name", ""),
                    }
                    email_audit_data["action"] = "Template updated"
                    email_audit_data["email_status"] = "success"
                    common_utils_database.update_audit(email_audit_data, "email_audit")
        elif action == "copy":
            copy_data = {k: v for k, v in copy_data.items() if v not in [None, "None"]}
            for k, v in copy_data.items():
                if isinstance(v, list):
                    copy_data[k] = ", ".join(
                        str(item) for item in v if item is not None
                    )  # Convert other types to strings
            if copy_data:
                dbs.insert_data(copy_data, table_name)
                email_audit_data = {
                    "template_name": copy_data.get("template_name", ""),
                    "email_type": "Application",
                    "partner_name": copy_data.get("partner_name", ""),
                    "cc_email": copy_data.get("cc_mail", ""),
                    "to_email": copy_data.get(
                        "to_mail", ""
                    ),  # Ensure correct column name
                    "comments": "",
                    "subject": copy_data.get("subject", ""),
                    "body": copy_data.get("body", ""),
                    "role": copy_data.get("role", ""),
                    "parents_module_name": copy_data.get("parents_module_name", ""),
                    "sub_module_name": copy_data.get("sub_module_name", ""),
                    "child_module_name": copy_data.get("child_module_name", ""),
                }
                email_audit_data["action"] = "Template copied"
                email_audit_data["email_status"] = "success"
                common_utils_database.update_audit(email_audit_data, "email_audit")
        elif action == "status":
            status_data = {k: v for k, v in new_data.items() if v not in [None, "None"]}
            if status_data:
                dbs.update_dict(table_name, status_data, {"id": unique_id})
                email_audit_data = {
                    "template_name": status_data.get("template_name", ""),
                    "email_type": "Application",
                    "partner_name": status_data.get("partner_name", ""),
                    "cc_email": status_data.get("cc_mail", ""),
                    "to_email": status_data.get(
                        "to_mail", ""
                    ),  # Ensure correct column name
                    "comments": "",
                    "subject": status_data.get("subject", ""),
                    "body": status_data.get("body", ""),
                    "role": status_data.get("role", ""),
                    "parents_module_name": status_data.get("parents_module_name", ""),
                    "sub_module_name": status_data.get("sub_module_name", ""),
                    "child_module_name": status_data.get("child_module_name", ""),
                }
                email_audit_data["action"] = "status"
                email_audit_data["email_status"] = "success"
                common_utils_database.update_audit(email_audit_data, "email_audit")

        response_data = {"flag": True, "message": f"{action} Successfully"}
        return response_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = "Unable to save the data"
        response = {"flag": False, "message": message}

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


def send_report_emails():
    try:
        database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

        # Get the reports_name and email_frequency from email_template table
        email_templates = database.get_data(
            "email_templates",
            {},
            ["reports_name", "email_frequency", "created_by", "template_name"],
        )
        logging.debug(email_templates, "Email templates fetched.")

        for template in email_templates.itertuples():
            reports_name = template.reports_name
            email_frequency = template.email_frequency
            username = template.created_by
            template_name = template.template_name

            if reports_name is None or email_frequency is None:
                logging.debug(
                    f"Skipping template with None values: reports_name={reports_name}, email_frequency={email_frequency}"
                )
                continue  # Skip unsupported templates
            # Get module_query from export_queries table
            module_query_result = database.get_data(
                "export_queries", {"module_name": reports_name}, ["module_query"]
            )
            if module_query_result.empty:
                logging.info(
                    f"No query found for reports_name: {reports_name}. Skipping this template."
                )
                continue
            module_query = module_query_result["module_query"].iloc[0]
            # Step 3: Set start_date and end_date based on email_frequency
            today = datetime.now()
            if email_frequency == "Daily":
                start_date = today
                end_date = today
            elif email_frequency == "Weekly":
                start_date = today - timedelta(days=7)
                end_date = today
            elif email_frequency == "Monthly":
                start_date = today.replace(day=1) - timedelta(days=1)
                start_date = start_date.replace(day=1)
                end_date = today.replace(day=1) - timedelta(days=1)
            elif email_frequency.startswith("Custom date"):
                custom_days = int(email_frequency.split("(")[1].split(")")[0])
                start_date = today - timedelta(days=custom_days)
                end_date = today
            else:
                logging.info(
                    f"Unsupported frequency: {email_frequency}. Skipping this template."
                )
                continue  # Skip unsupported frequencies

            # Execute the query with parameters for start_date and end_date
            params = [
                start_date.strftime("%Y-%m-%d 00:00:00"),
                end_date.strftime("%Y-%m-%d 23:59:59"),
            ]
            result_df = database.execute_query(module_query, params=params)

            if result_df.empty:
                logging.info(
                    f"No results found for the query: {module_query}. Skipping this template."
                )
                continue

            # Generate an Excel file from the result DataFrame
            blob_data = dataframe_to_blob(result_df)
            # Step 6: Get recipient email
            to_email = database.get_data("users", {"username": username}, ["email"])[
                "email"
            ].to_list()[0]

            # Prepare the email content
            subject = f"Report: {reports_name} - {email_frequency}"

            # Send the email with Excel file attached
            result = send_email(
                template_name=template_name,
                username=username,
                user_mail=to_email,
                subject=subject,
                attachments=blob_data,  # Pass the blob_data as attachment
            )
            # Handle email sending result and update audit
            if isinstance(result, dict) and result.get("flag") is False:
                to_emails = result.get("to_emails")
                cc_emails = result.get("cc_emails")
                subject = result.get("subject")
                from_email = result.get("from_email")
                partner_name = result.get("partner_name")

                # Email failed - log failure in email audit
                email_audit_data = {
                    "template_name": template_name,
                    "email_type": "Application",
                    "partner_name": partner_name,
                    "email_status": "failure",
                    "from_email": from_email,
                    "to_email": to_emails,
                    "cc_email": cc_emails,
                    "action": "Email sending failed",
                    "comments": "Email sending failed",
                    "subject": subject,
                    "body": body,
                }
                common_utils_database.update_audit(email_audit_data, "email_audit")
                common_utils_database.update_dict(
                    "email_templates",
                    {"last_email_triggered_at": today},
                    {"template_name": "Reports"},
                )
                logging.exception(f"Failed to send email: {email_audit_data}")
            else:
                to_emails, cc_emails, subject, from_email, body, partner_name = result

                query = """
                            SELECT parents_module_name, sub_module_name, child_module_name, partner_name
                            FROM email_templates
                            WHERE template_name = %s
                        """

                params = [template_name]
                # Execute the query with template_name as the parameter
                email_template_data = common_utils_database.execute_query(
                    query, params=params
                )
                (
                    parents_module_name,
                    sub_module_name,
                    child_module_name,
                    partner_name,
                ) = email_template_data[0]

                # Email success - log success in email audit
                email_audit_data = {
                    "template_name": template_name,
                    "email_type": "Application",
                    "partner_name": partner_name,
                    "email_status": "success",
                    "from_email": from_email,
                    "to_email": to_emails,
                    "cc_email": cc_emails,
                    "comments": "Report Email sent successfully",
                    "subject": subject,
                    "body": body,
                    "action": "Email triggered",
                    "parents_module_name": parents_module_name,
                    "sub_module_name": sub_module_name,
                    "child_module_name": child_module_name,
                }
                common_utils_database.update_audit(email_audit_data, "email_audit")
                logging.info(f"Email sent successfully: {email_audit_data}")

            # Update last email triggered timestamp
            database.update_dict(
                "email_templates",
                {"last_email_triggered_at": today},
                {"reports_name": reports_name},
            )

    except Exception as e:
        logging.exception(f"Something went wrong and error is: {e}")
        message = "Something went wrong while sending report email"

        # Error Management
        error_data = {
            "service_name": "Report Email Scheduler",
            "error_message": str(e),
            "error_type": type(e).__name__,
            "user": "username",
            "tenant_name": partner_name,  # Replace with actual tenant_name
            "comments": message,
            "module_name": "reports_name",
        }

        database.log_error_to_db(error_data, "error_table")
        return {"flag": False, "message": message}


# # Schedule the send_report_emails function to run daily
# schedule.every().day.at("02:00").do(send_report_emails)  # Set the time to 08:00 AM or any other time you prefer

# # Start the scheduler
# while True:
#     schedule.run_pending()  # Run any scheduled tasks
#     time_module.sleep(43200)


def killbill_mail_trigger():
    database = DB("altaworx_central", **db_config)
    common_utils_database = DB("common_utils", **db_config)
    template_name = "Killbill Invoice"
    # url = "http://98.82.152.66:5002/fetch_records_in_time_range"
    url = os.getenv("KILL_BILL_MAIL_TRIGGER", " ")

    try:
        api_response = requests.get(url)
        api_response.raise_for_status()  # Check if the request was successful
        response_data = api_response.json()  # Parse JSON response

        if response_data.get("flag") == True:
            main_data = response_data.get("main_data", {})

            # Dictionary to store all transactions for each account
            account_transactions = {}

            for account_id, details in main_data.items():
                email_id = details.get("email_id")  # Extract email_id
                # email_id = 'burhanmohammad123@gmail.com'
                customer_name = details.get("name", "")
                data_entries = details.get("data", [])

                # If data_entries is a dictionary, convert it to a list with one item
                if isinstance(data_entries, dict):
                    data_entries = [data_entries]

                # Process each entry in data
                for entry in data_entries:
                    transactions = entry.get("transactions", [])

                    # If there are no transactions, skip this account
                    if not transactions:
                        logging.info(
                            f"No transactions found for account {account_id}. Skipping."
                        )
                        continue  # Skip to the next account if no transactions are found

                    # Process each transaction in transactions
                    for transaction in transactions:
                        # Extract fields
                        amount = transaction.get("amount", "")
                        transaction_id = transaction.get("transactionId", "")
                        transaction_external_key = transaction.get(
                            "transactionExternalKey", ""
                        )
                        payment_id = transaction.get("paymentId", "")
                        payment_external_key = transaction.get("paymentExternalKey", "")
                        transaction_type = transaction.get("transactionType", "")
                        currency = transaction.get("currency", "")
                        effective_date = transaction.get("effectiveDate", "")
                        processed_amount = transaction.get("processedAmount", "")
                        processed_currency = transaction.get("processedCurrency", "")
                        status = transaction.get("status", "")
                        gateway_error_code = transaction.get("gatewayErrorCode", "")
                        gateway_error_message = transaction.get("gatewayErrorMsg", "")
                        first_payment_reference_id = transaction.get(
                            "firstPaymentReferenceId", ""
                        )
                        second_payment_reference_id = transaction.get(
                            "secondPaymentReferenceId", ""
                        )
                        properties = transaction.get("properties", "")

                        # Insert data into the database
                        data_to_insert = {
                            "account_id": account_id,
                            "to_email": email_id,
                            "customer_name": customer_name,
                            "amount": amount,
                            "transaction_id": transaction_id,
                            "transaction_external_key": transaction_external_key,
                            "payment_id": payment_id,
                            "payment_external_key": payment_external_key,
                            "transaction_type": transaction_type,
                            "currency": currency,
                            "effective_date": effective_date,
                            "processed_amount": processed_amount,
                            "processed_currency": processed_currency,
                            "status": status,
                            "gateway_error_code": gateway_error_code,
                            "gateway_error_message": gateway_error_message,
                            "first_payment_reference_id": first_payment_reference_id,
                            "second_payment_reference_id": second_payment_reference_id,
                            "properties": properties,
                        }

                        insert_result = database.insert_data(
                            data_to_insert, "kill_bill_email_notifications"
                        )

                        # Check if the data insertion was successful
                        if insert_result:
                            logging.info(
                                f"Transaction data inserted successfully for account {account_id}, transaction {transaction_id}"
                            )

                            if account_id not in account_transactions:
                                account_transactions[account_id] = {
                                    "account_id": account_id,
                                    "email_id": email_id,
                                    "customer_name": customer_name,
                                    "transactions": [],
                                }
                            account_transactions[account_id]["transactions"].append(
                                data_to_insert
                            )

                            # Send the email with the invoice summary for each transaction
                            subject = f"Invoice Notification for Account: {customer_name} ({account_id}) - Transaction {transaction_id}"
                            body = f"Dear {customer_name},\n\nHere is your invoice summary for the transactions:\n\n"

                            body += (
                                f"- Transaction ID: {transaction_id}\n"
                                f"  Amount: {amount} {currency}\n"
                                f"  Type: {transaction_type}\n"
                                f"  Status: {status}\n"
                                f"  Date: {effective_date}\n\n"
                            )
                            body += "\nThank you for your business.\n\nBest regards,\nYour Company Name"

                            # Send email for the transaction
                            result = send_email(
                                template_name=template_name,
                                to_emails=email_id,
                                subject=subject,
                                body=body,
                            )

                            # Handle email result and update email audit
                            if isinstance(result, dict) and result.get("flag") is False:
                                logging.error(f"Failed to send email to {email_id}.")
                            else:
                                subject, from_email, body, email_id = result
                                email_audit_data = {
                                    "template_name": template_name,
                                    "email_type": "Application",
                                    "email_status": "success",
                                    "from_email": from_email,
                                    "to_email": email_id,
                                    "comments": "Invoice email sent successfully",
                                    "subject": subject,
                                    "body": body,
                                    "action": "Email triggered",
                                }
                                common_utils_database.update_audit(
                                    email_audit_data, "email_audit"
                                )
                                logging.debug(
                                    f"Email sent successfully to {email_id}: {email_audit_data}"
                                )
                        else:
                            logging.error(
                                f"Failed to insert data for transaction {transaction_id}."
                            )

            logging.info("Response processed and emails sent.")
        else:
            logging.info("Flag is false, no data to process.")

    except requests.exceptions.RequestException as e:
        logging.exception(f"An error occurred: {e}")
    except ValueError:
        logging.exception("Failed to decode JSON response.")
