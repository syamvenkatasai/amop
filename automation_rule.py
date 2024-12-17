"""
@Author1 : Phaneendra.Y
Created Date: 10-10-24
"""

# Standard Library Imports
import datetime
from datetime import datetime, timedelta
import time
import json
import re
import os

# Third-Party Imports
import pandas as pd
from pytz import timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Local Application Imports
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging


db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}
logging = Logging(name="sim_management")


def path_fun(path,data):
    if path == "/get_automation_rule_data":
        result = get_automation_rule_data(data)
    elif path == "/get_automation_rule_create_pop_up_data":
        result = get_automation_rule_create_pop_up_data(data)
    elif path == "/insert_automation_data":
        result = insert_automation_data(data)

    elif path == "/get_automation_rule_details_data":
        result = get_automation_rule_details_data(data)

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
):
    """
    Description: The  function retrieves and organizes field mappings,headers,and module features
    based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields,and
    compiles features into a structured dictionary for each module.
    """
    ##Database connection
    logging.info(f"Module name is :{module_list} and role is {role}")
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    feature_module_name = data.get("feature_module_name", "")
    user_name = data.get("username") or data.get("user_name") or data.get("user")
    tenant_name = data.get("tenant_name") or data.get("tenant")
    try:
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": tenant_name}, ["id"]
        )["id"].to_list()[0]
        logging.debug(f"tenant_id  is :{tenant_id}")
    except Exception as e:
        logging.exception(f"Getting exception at fetching tenant id {e}")
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
        logging.info(f"Got the header map here")
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
            logging.warning(f"there is some error {e}")
            pass
        # Add the final features to the headers dictionary
        headers["module_features"] = final_features
        ret_out[module_name] = headers

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

    # Parse the JSON string to a dictionary
    user_features = json.loads(
        user_features_raw[0]
    )  # Assuming it's a list with one JSON string
    logging.debug(f"user features are {user_features}")
    # Initialize a list to hold features for the specified feature name
    features_list = []

    # Loop through all modules to find the specified feature name
    for module, features in user_features.items():
        if feature_name in features:
            features_list.extend(features[feature_name])
    logging.debug(f" features_list are {features_list}")
    return features_list


# Function to format dates
def format_date(date):
    if isinstance(date, pd.Timestamp):  # Check if it's a Pandas Timestamp
        return date.strftime("%m-%d-%Y %H:%M:%S")
    return date  # Return as-is if not a Timestamp



def get_automation_rule_create_pop_up_data(data):
    logging.info(f"Request Recieved")
    module_name = data.get("module_name", "Automation rule")
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get("tenant_name", "")
    request_received_at = data.get("request_received_at", "")
    username = data.get("username", " ")
    session_id = data.get("session_id", " ")
    table = data.get("table", "automation_rule")
    # Initialize the database connection
    tenant_database = data.get("db_name", "altaworx_central")
    tenant_name = data.get("db_name", "altaworx_central")
    tenant_names_str = os.getenv("TENANT_NAMES", "")
    # Convert the comma-separated string into a list
    tenant_names = tenant_names_str.split(',')
    if tenant_name in tenant_names:
        service_provider_name =  "AT&T - Telegence"
        service_provider_id= 6
    else:
        service_provider_name = "AT&T - Telegence - UAT ONLY"
        service_provider_id= 20
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    action = data.get("action", "")
    logging.debug(f"action is {action}")
    try:
        if action == "create":
            automation_rule_condition_dict = database.get_data(
                "automation_rule_condition",
                {"is_active": True},
                ["id", "automation_rule_condition_name"],
            ).to_dict(orient="records")
            automation_rule_action_dict = database.get_data(
                "automation_rule_action",
                {"is_active": True},
                ["id", "automation_rule_action_name"],
            ).to_dict(orient="records")
            automation_rule_followup_effective_date_type_dict = database.get_data(
                "automation_rule_followup_effective_date_type",
                {"is_active": True},
                ["id", "description"],
            ).to_dict(orient="records")
            # Get integration_id
            integration_id = database.get_data(
                "serviceprovider",
                {"service_provider_name": service_provider_name},
                ["integration_id"],
            )["integration_id"].to_list()[0]
            # Fetch data from 'device_status' table
            device_status_data = database.get_data(
                "device_status",
                {"integration_id": integration_id, "allows_api_update": True},
                ["id", "display_name"],
            )
            # Convert the result to a list of dictionaries and remove duplicates based on 'id'
            Has_Current_status_values = list(
                {
                    d["id"]: d for d in device_status_data.to_dict(orient="records")
                }.values()
            )
            service_provider_name_dict = database.get_data(
                "serviceprovider",
                {"service_provider_name": service_provider_name},
                ["id", "service_provider_name"],
            ).to_dict(orient="records")
            features_data = database.get_data(
                "mobility_feature",
                {"service_provider_id": service_provider_id},
                ["soc_code", "friendly_name"],
            ).to_dict(orient="records")
            message = f"Data Fetched Successfully"
            # Prepare the response
            response = {
                "flag": True,
                "automation_rule_condition_dict": automation_rule_condition_dict,
                "automation_rule_action_dict": automation_rule_action_dict,
                "automation_rule_followup_effective_date_type_dict": automation_rule_followup_effective_date_type_dict,
                "Has_Current_status_values": Has_Current_status_values,
                "service_provider_name_dict": service_provider_name_dict,
                "features_data": features_data,
                "message": message,
            }
        else:
            automation_rule_condition_dict = database.get_data(
                "automation_rule_condition",
                {"is_active": True},
                ["id", "automation_rule_condition_name"],
            ).to_dict(orient="records")
            automation_rule_action_dict = database.get_data(
                "automation_rule_action",
                {"is_active": True},
                ["id", "automation_rule_action_name"],
            ).to_dict(orient="records")
            automation_rule_followup_effective_date_type_dict = database.get_data(
                "automation_rule_followup_effective_date_type",
                {"is_active": True},
                ["id", "description"],
            ).to_dict(orient="records")
            # Get integration_id
            integration_id = database.get_data(
                "serviceprovider",
                {"service_provider_name": service_provider_name},
                ["integration_id"],
            )["integration_id"].to_list()[0]
            # Fetch data from 'device_status' table
            device_status_data = database.get_data(
                "device_status",
                {"integration_id": integration_id, "allows_api_update": True},
                ["id", "display_name"],
            )
            # Convert the result to a list of dictionaries and remove duplicates based on 'id'
            Has_Current_status_values = list(
                {
                    d["id"]: d for d in device_status_data.to_dict(orient="records")
                }.values()
            )
            service_provider_name_dict = database.get_data(
                "serviceprovider",
                {"service_provider_name": service_provider_name},
                ["id", "service_provider_name"],
            ).to_dict(orient="records")
            features_data = database.get_data(
                "mobility_feature",
                {"service_provider_id": service_provider_id},
                ["soc_code", "friendly_name"],
            ).to_dict(orient="records")
            automation_rule_id = data.get("automation_rule_id", "")
            automation_rule_detail_data = database.get_data(
                "automation_rule_detail",
                {"automation_rule_id": automation_rule_id, "is_active": True},
            ).to_dict(orient="records")
            # Step 2: Format dates within automation_rule_detail_data
            for record in automation_rule_detail_data:
                record["created_date"] = format_date(record.get("created_date"))
                record["modified_date"] = format_date(record.get("modified_date"))
                record["deleted_date"] = format_date(record.get("deleted_date"))

            # Step 3: Extract rule_followup_id values from automation_rule_detail_data
            rule_followup_ids = [
                item["rule_followup_id"] for item in automation_rule_detail_data
            ]
            # Step 4: Query automation_rule_followup_detail for each rule_followup_id
            automation_rule_followup_data = []
            automation_rule_followup_details = []
            for rule_followup_id in rule_followup_ids:
                followup_detail = database.get_data(
                    "automation_rule_followup_detail",
                    {"rule_followup_id": rule_followup_id, "is_active": True},
                ).to_dict(orient="records")
                # Step 5: Format dates within each followup_detail
                for record in followup_detail:
                    record["created_date"] = format_date(record.get("created_date"))
                    record["modified_date"] = format_date(record.get("modified_date"))
                    record["deleted_date"] = format_date(record.get("deleted_date"))
                followup_data = database.get_data(
                    "automation_rule_followup_effective_date_type",
                    {"id": rule_followup_id, "is_active": True},
                ).to_dict(orient="records")
                automation_rule_followup_data.extend(followup_data)
                automation_rule_followup_details.extend(
                    followup_detail
                )  # Use extend to flatten the list
            message = f"Data Fetched Successfully"
            # Prepare the response
            response = {
                "flag": True,
                "automation_rule_detail_data": automation_rule_detail_data,
                "automation_rule_followup_details": automation_rule_followup_details,
                "automation_rule_condition_dict": automation_rule_condition_dict,
                "automation_rule_action_dict": automation_rule_action_dict,
                "automation_rule_followup_effective_date_type_dict": automation_rule_followup_effective_date_type_dict,
                "Has_Current_status_values": Has_Current_status_values,
                "service_provider_name_dict": service_provider_name_dict,
                "features_data": features_data,
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
                "session_id": session_id,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "tenant_name": Partner,
                "comments": "fetching the optimization data",
                "module_name": "get_optimization_data",
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
            "created_date": start_time,
            "error_messag": message,
            "session_id": session_id,
            "error_type": e,
            "user": username,
            "tenant_name": Partner,
            "comments": message,
            "module_name": "get_optimization_data",
            "request_received_at": start_time,
        }
        common_utils_database.log_error_to_db(error_data, "error_table")
        response = {"flag": False, "error": str(e)}
        return response


def get_automation_rule_data(data):
    logging.info("Request data Recieved")
    module_name = data.get("module_name", "Automation rule")
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get("tenant_name", "")
    request_received_at = data.get("request_received_at", "")
    username = data.get("username", " ")
    table = data.get("table", "automation_rule")
    session_id = data.get("session_id", " ")
    # Initialize the database connection
    tenant_database = data.get("db_name", "altaworx_central")
    role_name = data.get("role_name", "")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    # # Get tenant's timezone
    tenant_name = data.get("tenant_name", "")
    tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
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
    try:
        automation_rule_data = []
        pages = {}
        if "mod_pages" in data:
            start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
            end = data["mod_pages"].get("end") or 100  # Default to 100 if no value
            limit = data.get("limit", 100)
            # Calculate pages
            pages["start"] = start
            pages["end"] = end
            count_params = [table]
            if module_name == "Automation rule":
                count_query = "SELECT COUNT(*) FROM %s where is_active=True" % table
            else:
                count_query = "SELECT COUNT(*) FROM %s" % table
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            pages["total"] = int(count_result)
        # Fetch the query from the database based on the module name
        module_query_df = common_utils_database.get_data(
            "export_queries", {"module_name": module_name}
        )
        ##checking the dataframe is empty or not
        if module_query_df.empty:
            return {
                "flag": False,
                "message": f"No query found for module name: {module_name}",
            }
        # Extract the query string from the DataFrame
        query = module_query_df.iloc[0]["module_query"]
        if not query:
            return {"flag": False, "message": f"Unknown module name: {module_name}"}
        params = [start, end]
        automation_rule_data = database.execute_query(query, params=params).to_dict(
            orient="records"
        )
        automation_rule_data = convert_timestamp_data(
            automation_rule_data, tenant_time_zone
        )
        # Generate the headers mapping
        headers_map = get_headers_mappings(
            tenant_database,
            [module_name],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
        )
        # Prepare the response
        response = {
            "flag": True,
            "automation_rule_data": serialize_data(automation_rule_data),
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
                "session_id":session_id,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "tenant_name": Partner,
                "comments": "fetching the optimization data",
                "module_name": "get_optimization_data",
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
                "session_id":session_id,
                "error_type": e,
                "user": username,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "get_optimization_data",
                "request_received_at": start_time,
            }
            common_utils_database.log_error_to_db(error_data, "error_table")
        except Exception as e:
            logging.warning(f"Exception occured at updating the error table")
        # Generate the headers mapping
        headers_map = get_headers_mappings(
            tenant_database,
            [module_name],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
        )
        # Prepare the response
        response = {
            "flag": True,
            "automation_rule_data": {},
            "header_map": headers_map,
            "pages": {},
        }
        return response


def delete_automation_rule(data, automation_rule_id,database):
    # automation_rule_id = data.get('automation_rule_id', '')
    logging.debug(automation_rule_id, "automation_rule_id")
    #database = data.get("db_name", "altaworx_test")

    # Create the database URL for SQLAlchemy
    db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{database}"
    # Create a new engine instance
    engine = create_engine(db_url)

    # Create a configured "Session" class
    Session = sessionmaker(bind=engine)

    # Create a session
    session = Session()
    try:
        # Get the automation_rule_detail followup IDs
        automation_rule_details_data = session.execute(
            text(
                """SELECT rule_followup_id FROM
                  automation_rule_detail WHERE automation_rule_id = :automation_rule_id
                  """
            ),
            {"automation_rule_id": automation_rule_id},
        ).fetchall()

        # Extract the followup IDs
        rule_followup_ids = [
            row[0] for row in automation_rule_details_data if row[0] is not None
        ]
        logging.debug(rule_followup_ids, "rule_followup_ids")

        # Delete from automation_rule_detail
        session.execute(
            text(
                "DELETE FROM automation_rule_detail WHERE automation_rule_id = :automation_rule_id"
            ),
            {"automation_rule_id": automation_rule_id},
        )

        # Delete from automation_rule
        session.execute(
            text("DELETE FROM automation_rule WHERE id = :automation_rule_id"),
            {"automation_rule_id": automation_rule_id},
        )

        # If there are followup IDs, delete from the related tables
        if rule_followup_ids:
            formatted_followup_ids = ", ".join(map(str, rule_followup_ids))
            session.execute(
                text(
                    f"""DELETE FROM automation_rule_followup_detail
                    WHERE rule_followup_id IN ({formatted_followup_ids})
                    """
                )
            )
            session.execute(
                text(
                    f"DELETE FROM automation_rule_followup WHERE id IN ({formatted_followup_ids})"
                )
            )
        else:
            logging.info("No valid followup IDs to delete.")

        # Commit the transaction
        session.commit()
        logging.info("Deletion successful in the database")
        response = {"flag": True, "message": "Deleted Successfully"}
        return response

    except Exception as e:
        # Rollback in case of any errors
        session.rollback()
        logging.exception(f"Error: {e}")
        return {"flag": False, "message": str(e)}

    finally:
        # Close the session
        session.close()

def insert_automation_data(data):
    logging.info("Request data Recieved")
    automation_rule_id = data.get("automation_rule_id", "")
    action = data.get("action", "")
    module_name = data.get("module_name", "Automation rule")
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get("tenant_name", "")
    request_received_at = data.get("request_received_at", "")
    username = data.get("username", " ")
    session_id = data.get("session_id", " ")
    # Initialize the database connection
    tenant_database = data.get("db_name", "altaworx_test")
    data = data.get("data")
    # database Connection
    db = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

    logging.debug(f"action is {action}")
    if action == "edit":
        delete_automation_rule(data, automation_rule_id,tenant_database)
    try:
        # Insert into automation_rule and retrieve the ID
        automation_rule_data = data["automation_data"]["automation_rule"]
        automation_rule_id = db.insert_data(automation_rule_data, "automation_rule")

        # Iterate over block_details
        for block in data["automation_data"]["block_details"]:
            automation_followup_id = None
            # Check if automation_followup exists and insert it
            if block.get("automation_rule_followup"):
                followup_data = block["automation_rule_followup"]
                automation_followup_id = db.insert_data(
                    followup_data, "automation_rule_followup"
                )

                # Insert into automation_followup_detail
                for followup_detail in block["automation_rule_followup_detail"]:
                    followup_detail["rule_followup_id"] = (
                        automation_followup_id  # Link to followup ID
                    )
                    db.insert_data(followup_detail, "automation_rule_followup_detail")

            # Insert into automation_rule_detail or step_details depending on the block
            if "automation_rule_detail" in block:
                rule_detail_data = block["automation_rule_detail"]
            else:
                rule_detail_data = block["step_details"]

            rule_detail_data["automation_rule_id"] = (
                automation_rule_id  # Link to automation_rule_id
            )
            rule_detail_data["rule_followup_id"] = (
                automation_followup_id  # Link to followup ID
            )
            db.insert_data(
                rule_detail_data, "automation_rule_detail"
            )  # Insert into rule detail
            # Prepare the response
        response = {
            "flag": True,
            "message": "Data Inserted Successfully",
        }
        try:
            # End time calculation
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {
                "service_name": "Automation rule",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response["flag"]),
                "time_consumed_secs": time_consumed,
                "tenant_name": Partner,
                "session_id":session_id,
                "comments": "fetching the optimization data",
                "module_name": "get_optimization_data",
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
            "service_name": "Automation rule",
            "created_date": start_time,
            "error_messag": message,
            "error_type": e,
            "session_id":session_id,
            "user": username,
            "tenant_name": Partner,
            "comments": message,
            "module_name": "get_optimization_data",
            "request_received_at": start_time,
        }
        common_utils_database.log_error_to_db(error_data, "error_table")
        response = {"flag": False, "error": str(e)}
        return response


def get_automation_rule_details_data(data):
    database = DB("altaworx_central", **db_config)
    automation_rule_name = data.get("automation_rule_name", "")
    service_provider_name = data.get("service_provider_name", "AT&T - Telegence")
    logging.info("Fetching automation rule ID for rule name: %s", automation_rule_name)
    automation_rule_id = database.get_data(
        "automation_rule", {"automation_rule_name": automation_rule_name}, ["id"]
    )["id"].to_list()[0]
    logging.info("Automation rule ID fetched: %s", automation_rule_id)
    automation_rule_detail_data = database.get_data(
        "automation_rule_detail",
        {"automation_rule_id": automation_rule_id},
        [
            "rule_followup_id",
            "automation_rule_id",
            "rule_condition_id",
            "rule_condition_value",
            "rule_action_id",
            "rule_action_value",
            "condition_step",
        ],
    ).to_dict(orient="records")
    # Get integration_id
    integration_id = database.get_data(
        "serviceprovider",
        {"service_provider_name": service_provider_name},
        ["integration_id"],
    )["integration_id"].to_list()[0]
    # Fetch data from 'device_status' table
    device_status_data = database.get_data(
        "device_status",
        {"integration_id": integration_id, "allows_api_update": True},
        ["id", "display_name"],
    )
    # Convert the result to a list of dictionaries and remove duplicates based on 'id'
    Has_Current_status_values = list(
        {d["id"]: d for d in device_status_data.to_dict(orient="records")}.values()
    )
    features_data = database.get_data(
        "mobility_feature", {"service_provider_id": 6}, ["soc_code", "friendly_name"]
    ).to_dict(orient="records")
    automation_rule_condition_dict = database.get_data(
        "automation_rule_condition",
        {"is_active": True},
        ["id", "automation_rule_condition_name"],
    ).to_dict(orient="records")
    automation_rule_action_dict = database.get_data(
        "automation_rule_action",
        {"is_active": True},
        ["id", "automation_rule_action_name"],
    ).to_dict(orient="records")
    automation_rule_followup_effective_date_type_dict = database.get_data(
        "automation_rule_followup_effective_date_type",
        {"is_active": True},
        ["id", "description"],
    ).to_dict(orient="records")
    message = f"Data Fetched Successfully"
    logging.info(message)
    # Prepare the response
    response = {
        "flag": True,
        "automation_rule_detail_data": automation_rule_detail_data,
        "automation_rule_condition_dict": automation_rule_condition_dict,
        "automation_rule_action_dict": automation_rule_action_dict,
        "automation_rule_followup_effective_date_type_dict":
        automation_rule_followup_effective_date_type_dict,
        "Has_Current_status_values": Has_Current_status_values,
        "features_data": features_data,
        "message": message,
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
