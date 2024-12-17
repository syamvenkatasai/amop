"""
@Author : Phaneendra.Y
Created Date: 21-06-24
"""

# Standard Library Imports
import base64
import concurrent.futures
import io
import json
import os
import re
import time
import zipfile
from datetime import datetime
from io import BytesIO

# Third-Party Imports
import boto3
import pandas as pd
import pytds
import requests
from pytz import timezone

# Local Application/Custom Imports
from common_utils.db_utils import DB
from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging

# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}
# AWS S3 client
s3_client = boto3.client("s3")
# Retrieve the S3 bucket name from an environment variable
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

logging = Logging(name="optimization")

def path_func(data,path):
    if path == "/optimization_dropdown_data":
        result = optimization_dropdown_data(data)
    elif path == "/get_optimization_data":
        result = get_optimization_data(data)
    elif path == "/export_optimization_data_zip":
        result = export_optimization_data_zip(data)
    elif path == "/start_optimization":
        result = start_optimization(data)
    elif path == "/get_optimization_pop_up_data":
        result = get_optimization_pop_up_data(data)
    elif path == "/push_charges_submit":
        result = push_charges_submit(data)
    elif path == "/get_optimization_row_details":
        result = get_optimization_row_details(data)
    elif path == "/get_optimization_details_reports_data":
        result = get_optimization_details_reports_data(data)
    elif path == "/get_optimization_push_charges_data":
        result = get_optimization_push_charges_data(data)
    elif path == "/get_assign_rate_plan_optimization_dropdown_data":
        result = get_assign_rate_plan_optimization_dropdown_data(data)
    elif path == "/update_optimization_actions_data":
        result = update_optimization_actions_data(data)
    elif path == "/update_push_charges_data_optimization":
        result = update_push_charges_data_optimization(data)
    elif path == "/get_export_status":
        result = get_export_status(data)
    elif path == "/get_optimization_progress_bar_data":
        result = get_optimization_progress_bar_data(data)
    elif path == "/get_optimization_error_details_data":
        result = get_optimization_error_details_data(data)
    elif path == "/optimize_button_status":
        result = optimize_button_status(data)

    else:
        result = {"error": "Invalid path or method"}
        logging.warning("Invalid path or method requested: %s", path)
    
    return result


##first point
def get_optimization_data(data):
    """
    This function retrieves optimization data by executing a query based
    on the provided module name and parameters,
    converting the results into a dictionary format.
    It logs the audit and error details to the database
    and returns the optimization data along with a success flag.
    """
    logging.info("Request Recieved")
    # Record the start time for performance measurement
    start_time = time.time()
    module_name = data.get("module_name", "")
    request_received_at = data.get("request_received_at", "")
    session_id = data.get("sessionID", "")
    username = data.get("username", None)
    Partner = data.get("Partner", "")
    mod_pages = data.get("mod_pages", {})
    role_name = data.get("role_name", "")
    # Set the limit for the number of records to retrieve per request
    limit = 100
    offset = mod_pages.get("start", 0)
    end = mod_pages.get("end", 0)
    table = data.get("table_name", "")
    tenant_name=data.get('tenant_name','')
    optimization_type = data.get("optimization_type", "")
    logging.debug("Optimization Type is: %s", optimization_type)
    # Check if optimization_type is empty or None and return a response
    if not optimization_type:
        logging.warning("Optimization type is missing from the data.")
        return {
            "flag": False,
            "message": "Optimization type is required but not provided.",
        }
    # Database connection
    tenant_database = data.get("db_name", "")
    try:
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    except Exception as db_exception:
        logging.error("Failed to connect to the database: %s", str(db_exception))
        return {"error": "Database connection failed.", "details": str(db_exception)}
    return_json_data = {}
    # Initialize pagination dictionary
    pages = {"start": offset, "end": end}

    tenant_timezone = None
    try:
        count_params = [table]
        if optimization_type == "Customer":
            count_query = f"SELECT COUNT(*) FROM vw_customer_optimization "
        else:
            count_query = f"SELECT COUNT(*) FROM vw_carrier_optimization"
        # count_start_time = time.time()
        count_result = database.execute_query(count_query, count_params).iloc[0, 0]
        # Set total pages count
        pages["total"] = int(count_result)
        # Retrieve the module query from the 'module_view_queries' table
        module_query_df = common_utils_database.get_data(
            "module_view_queries", {"module_name": optimization_type}
        )
        if module_query_df.empty:
            raise ValueError(f"No query found for module name: {optimization_type}")
        query = module_query_df.iloc[0]["module_query"]
        if not query:
            raise ValueError(f"Unknown module name: {optimization_type}")
        params = [offset, limit]
        # main_query_start_time = time.time()
        optimization_dataframe = database.execute_query(query, params=params)
        # Fetch the tenant's timezone
        tenant_time_zone = fetch_tenant_timezone(common_utils_database,data)
        optimization_dataframe = convert_timestamp_data(
            optimization_dataframe, tenant_time_zone
        )
        optimization_dict = optimization_dataframe.to_dict(orient="records")
        # Get headers mapping
        headers_map = get_headers_mapping(
            tenant_database,
            [optimization_type],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
            common_utils_database,
        )
        # Query for billing period information
        billing_period_query = """
                SELECT service_provider, billing_cycle_end_date
                    FROM billing_period
                    WHERE is_active = 'true'
                    ORDER BY
                        CASE
                            WHEN CURRENT_DATE BETWEEN billing_cycle_start_date
                            AND billing_cycle_end_date THEN 0
                            ELSE 1
                        END,
                        billing_cycle_end_date DESC
            """
        billing_period_dates = database.execute_query(
            billing_period_query, True
        ).to_dict(orient="records")
        # Extract only the service_provider values
        service_providers = [row["service_provider"] for row in billing_period_dates]

        # Create a dictionary where the key is service_provider and
        # the value is a list of billing_cycle_end_dates
        service_provider_dict = {}

        for row in billing_period_dates:
            if row["service_provider"] in service_provider_dict:
                service_provider_dict[row["service_provider"]].append(
                    row["billing_cycle_end_date"]
                )
            else:
                service_provider_dict[row["service_provider"]] = [
                    row["billing_cycle_end_date"]
                ]

        # Optionally, remove duplicates
        unique_service_providers = sorted(list(set(service_providers)))
        # Add "All" to the list of unique service providers
        unique_service_providers.insert(0, "All service providers")
        try:
            # Attempt to retrieve the value from the 'tenant' table
            result = common_utils_database.get_data('tenant', {"tenant_name": tenant_name
                                                    }, ["cross_provider_customer_optimization"])

            # Check if the result is not empty and extract the value safely
            cross_carrier_optimization = result["cross_provider_customer_optimization"].to_list()
            if cross_carrier_optimization:
                cross_carrier_optimization = cross_carrier_optimization[0]
            else:
                cross_carrier_optimization = False
        except Exception as e:
            logging.exception(f"Failed to get the cross_carrier_optimization{e}")
            cross_carrier_optimization=False
        # Preparing the response data
        return_json_data.update(
            {
                "message": "Successfully Fetched the optimization data",
                "flag": True,
                "service_providers": unique_service_providers,
                "billing_period_dates": serialize_data(service_provider_dict),
                "headers_map": headers_map,
                "data": optimization_dict,
                "pages": pages,"cross_carrier_optimization":cross_carrier_optimization
            }
        )
        # End time and audit logging
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        audit_data_user_actions = {
            "service_name": "Optimization",
            "created_date": request_received_at,
            "created_by": username,
            "status": str(return_json_data["flag"]),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": "optimization data",
            "module_name": optimization_type,
            "request_received_at": request_received_at,
        }
        common_utils_database.update_audit(
            audit_data_user_actions, "audit_user_actions"
        )
        return return_json_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        # Get headers mapping
        headers_map = get_headers_mapping(
            tenant_database,
            [module_name],
            role_name,
            "username",
            "main_tenant_id",
            "sub_parent_module",
            "parent_module",
            data,
            common_utils_database,
        )
        message = f"Unable to fetch the Optimization data{e}"
        return_json_data = {
            "flag": True,
            "message": message,
            "headers_map": headers_map,
            "data": [],"cross_carrier_optimization":False
        }
        error_type = str(type(e).__name__)
        # Error logging
        error_data = {
            "service_name": "Optimization",
            "created_date": request_received_at,
            "error_message": message,
            "error_type": error_type,
            "users": username,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": "Failed to get Optimization data",
            "module_name": optimization_type,
            "request_received_at": request_received_at,
        }
        common_utils_database.log_error_to_db(error_data, "error_log_table")

        return return_json_data

# Helper function to fetch the tenant's timezone
def fetch_tenant_timezone(common_utils_database,data):
    tenant_name = data.get("tenant_name", "")
    tenant_timezone_query = (
        """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
    )
    # tenant_timezone_start_time = time.time()
    tenant_timezone = common_utils_database.execute_query(
        tenant_timezone_query, params=[tenant_name]
    )
    # tenant_timezone_duration = time.time() - tenant_timezone_start_time
    if tenant_timezone.empty or tenant_timezone.iloc[0]["time_zone"] is None:
        raise ValueError("No valid timezone found for tenant.")
    tenant_time_zone = tenant_timezone.iloc[0]["time_zone"]
    match = re.search(
        r"\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)",
        tenant_time_zone,
    )
    if match:
        tenant_time_zone = match.group(1).replace(
            " ", ""
        )  # Ensure it's formatted correctly
    return tenant_time_zone

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


def convert_timestamp_data(df, tenant_time_zone):
    """Convert timestamp columns in the DataFrame to the tenant's timezone."""
    # List of timestamp columns to convert
    timestamp_columns = [
        "created_date",
        "modified_date",
        "deleted_date",
    ]  # Adjust as needed based on your data

    # Convert specified timestamp columns to the tenant's timezone
    for col in timestamp_columns:
        if col in df.columns:
            # Convert the column to datetime format if it isn't already
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # Localize the datetime column to UTC if it's naive
            df[col] = df[col].dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")
            # Convert to the target timezone
            df[col] = df[col].dt.tz_convert(tenant_time_zone)
            # Format as string if needed (optional)
            df[col] = df[col].dt.strftime("%m-%d-%Y %H:%M:%S")

    return df


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


##export button in list view

def export_optimization_data_zip(data):
    """
    The export_optimization_data_zip function generates a ZIP file containing
    Excel files grouped by session IDs based on query results retrieved from a
    database. It processes optimization data using provided filters, encodes the
    ZIP as a base64 string, and stores it in an S3 bucket, returning the URL for download.
    If no data is found, an empty ZIP with an empty Excel sheet is returned.
    """
    logging.info("Request Data Received")
    request_received_at = data.get("request_received_at", "")
    module_name = data.get("module_name", "Optimization")
    optimization_type = data.get("optimization_type", "")
    service_provider = data.get("service_provider", "")
    billing_period_start_date = data.get("billing_period_start_date", "")
    billing_period_end_date = data.get("billing_cycle_period", "")
    Partner = data.get("Partner", "")
    username = data.get("username", "")
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    common_utils_database.update_dict(
        "export_status",
        {"status_flag": "Waiting", "url": ""},
        {"module_name": "Export Optimization"},
    )

    try:
        # Fetch the query from the database based on the module name
        module_query_df = common_utils_database.get_data(
            "export_queries", {"module_name": module_name}
        )
        query = module_query_df.iloc[0]["module_query"]

        if module_name.lower() == "optimization":
            # Prepare the WHERE clause with necessary filters
            base_query = query.split("where")[0].strip()
            where_clause = "WHERE optimization_type = %s"
            params = [optimization_type]

            if service_provider:
                where_clause += " AND service_provider = %s"
                params.append(service_provider)

            if billing_period_end_date:
                where_clause += " AND billing_period_end_date = %s"
                params.append(billing_period_end_date)

            final_query = f"{base_query} {where_clause}"

        # Executing the query and fetching data
        data_frame = database.execute_query(final_query, params=params)

        # Create a new ZIP file buffer
        zip_buffer = io.BytesIO()
        zip_filename = "Optimization_session.zip"

        # If the data frame is empty, create an empty DataFrame with headers only
        if data_frame.empty:
            logging.info("No data found, creating an empty Excel file.")
            empty_df = pd.DataFrame(columns=["S.NO", "Session ID", "Other Columns"])
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                    empty_df.to_excel(writer, index=False)
                excel_buffer.seek(0)
                zipf.writestr("Empty_Optimization_Data.xlsx", excel_buffer.read())
        else:
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                grouped = data_frame.groupby("session_id")
                for session_id, group in grouped:
                    folder_name = f"{session_id}/"
                    zipf.writestr(folder_name, "")

                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                        group.to_excel(writer, index=False)

                    excel_buffer.seek(0)
                    zipf.writestr(f"{folder_name}{session_id}.xlsx", excel_buffer.read())

        # Upload the ZIP file to S3
        zip_buffer.seek(0)
        s3_client = boto3.client("s3")
        s3_key = f"exports/optimization/{zip_filename}"
        S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]  # Ensure this environment variable is set

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=zip_buffer.getvalue(),
            ContentType="application/zip",
        )

        # Generate download URL
        download_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{s3_key}"
        logging.info("File uploaded to S3 successfully.")

        # Log success and return response
        response = {
            "flag": True,
            "message": "Export successful",
            "download_url": download_url,
        }
        common_utils_database.update_dict(
            "export_status",
            {"status_flag": "Success", "url": download_url},
            {"module_name": "Export Optimization"},
        )

        return response

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        message = "Failed to process the data export"
        error_data = {
            "service_name": "optimization",
            "created_date": request_received_at,
            "error_message": message,
            "error_type": str(e),
            "user": username,
            "tenant_name": Partner,
            "comments": message,
            "module_name": module_name,
            "request_received_at": request_received_at,
        }
        common_utils_database.log_error_to_db(error_data, "error_table")
        common_utils_database.update_dict(
            "export_status",
            {"status_flag": "Failure", "url": ""},
            {"module_name": "Export Optimization"},
        )

        return {"flag": False, "error": str(e)}



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


def format_timestamp(ts):
    # Check if the timestamp is not None
    if ts is not None and pd.notna(ts):
        # Convert a Timestamp or datetime object to the desired string format
        return ts.strftime("%b %d, %Y, %I:%M %p")
    else:
        # Return a placeholder or empty string if the timestamp is None
        return "N/A"


##to get the pop up details in the optimize button
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
                    data, database, common_utils_database
                )
                rate_plan_count = int(
                    results[0][0]
                )  # Ensure conversion to standard int
            except Exception as e:
                logging.exception("rate_plan_count", e)
                rate_plan_count = 0
            logging.debug(f"rate_plan_count is {rate_plan_count}")

            """Sim cards to Optimize"""
            try:
                # Call function to get SIM cards to optimize
                sim_cards_to_optimize = sim_cards_to_optimize_count(
                    data, database, common_utils_database
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
                get_customer_total_sim_cards_count(rev_customer_id, database)
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
            rate_plan_count_query = """SELECT count(*) FROM public.carrier_rate_plan
            where service_provider_id=%s and is_active=True
            """
            rate_plan_count = int(
                database.execute_query(rate_plan_count_query, params=params).iloc[0, 0]
            )  # Ensure conversion to standard int
            logging.info(rate_plan_count, "rate_plan_count")

            """Sim cards to Optimize"""
            try:
                sim_cards_to_optimize = sim_cards_to_optimize_count(
                    data, database, common_utils_database
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
            total_sim_cards_count_query = """SELECT count(*) FROM public.sim_management_inventory
              where service_provider_id=%s and is_active=True
              """
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
        if body["siteId"]==0:
            body["siteId"]='null'
        # Take the first item in the serviceProviderId list if it exists
        if "serviceProviderId" in body and isinstance(body["serviceProviderId"], list) and body["serviceProviderId"]:
            body["serviceProviderId"] = body["serviceProviderId"][0]
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
        # Wait for 10 minutes (10 * 60 seconds)
        time.sleep(10 * 60)

        # After 15 minutes, call the second API
        api_url = "https://demo-api.amop.services/daily_migration_after_lambdas"
        api_payload = {
            "data": {
                "data": {
                    "path": "/lambda_sync_jobs",
                    "key_name": "optimization_sync"
                }
            }
        }

        try:
            response = requests.post(api_url, json=api_payload)
            if response.status_code == 200:
                logging.info("API call successful after 15 minutes.")
            else:
                logging.error(f"API call failed with status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Error occurred while calling the API after 15 minutes: {e}")
        # Return the status code and response JSON
        return response_data
    except Exception as e:
        logging.exception(f"Error fetching data: {e}")
        message = f"exception is {e}"
        response_data = {"flag": False, "message": message}
        # Return the status code and response JSON
        return response_data

def rate_plans_by_customer_count(data, database, common_utils_database):
    """
    The rate_plans_by_customer_count function retrieves customer rate plan
    counts by executing stored procedures based on the portal type (portal_id)
    and other input parameters like ServiceProviderId, BillingPeriodId, and tenant_name.
    It dynamically determines customer IDs (RevCustomerIds or AMOPCustomerIds), connects
    to a database, and processes data through specific stored procedures.
    The function handles different portal types and logs errors if exceptions occur.
    """
    logging.info(f"Request Data Recieved")
    ServiceProviderId = data.get("ServiceProviderId", "")
    BillingPeriodId = data.get("BillingPeriodId", "")
    tenant_name = data.get("tenant_name", "")
    TenantId = common_utils_database.get_data(
        "tenant", {"tenant_name": tenant_name}, ["id"]
    )["id"].to_list()[0]
    customer_name = data.get("customer_name", "")
    # Fetch integration_id and portal_id using database queries
    integration_id = database.get_data(
        "serviceprovider", {"id": ServiceProviderId}, ["integration_id"]
    )["integration_id"].to_list()[0]
    portal_id = database.get_data(
        "integration", {"id": integration_id}, ["portal_type_id"]
    )["portal_type_id"].to_list()[0]
    # If portal_id is 0, proceed with M2M connection and stored procedure execution
    if portal_id == 0:
        # Define database connection parameters
        server = os.getenv("ONEPOINTOSERVER", " ")
        database_name = os.getenv("ONEPOINTODATABASE", " ")
        username = os.getenv("ONEPOINTOUSERNAME", " ")
        password = os.getenv("ONEPOINTOPASSWORD", " ")
        rev_customer_data = database.get_data(
            "customers", {"customer_name": customer_name}, ["rev_customer_id"]
        )["rev_customer_id"].to_list()
        # Check if rev_customer_id data is available
        if rev_customer_data:
            RevCustomerIds = ",".join([str(id) for id in rev_customer_data])
            AMOPCustomerIds = ""
            SiteType = 1
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
            SiteType = 0
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

                    # Fetch and return the results
                    results = cursor.fetchall()
                    return results

        except Exception as e:
            logging.exception("Error in connection or execution:", e)
            return None
    elif portal_id == 2:
        # Define database connection parameters
        server = os.getenv("ONEPOINTOSERVER", " ")
        database_name = os.getenv("ONEPOINTODATABASE", " ")
        username = os.getenv("ONEPOINTOUSERNAME", " ")
        password = os.getenv("ONEPOINTOPASSWORD", " ")
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
                # Check if rev_customer_id data is available
                if rev_customer_data:
                    RevCustomerIds = ",".join([str(id) for id in rev_customer_data])
                    AMOPCustomerIds = ""
                    SiteType = 1
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
                    SiteType = 0
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


def sim_cards_to_optimize_count(data, database, common_utils_database):
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
        "tenant", {"tenant_name": tenant_name}, ["id"]
    )["id"].to_list()[0]
    optimization_type = data.get("optimization_type", "")
    # Define your connection parameters
    server = os.getenv("ONEPOINTOSERVER", " ")
    database= os.getenv("ONEPOINTODATABASE", " ")
    username = os.getenv("ONEPOINTOUSERNAME", " ")
    password = os.getenv("ONEPOINTOPASSWORD", " ")
    sim_cards_to_optimize = 0  # Ensure default initialization
    if optimization_type == "Customer":
        # customer_name='Easy Shop Supermarket (300007343)'
        # Create a connection to the database
        try:
            with pytds.connect(
                server=server, database=database, user=username, password=password
            ) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    stored_procedure_name = (
                        "AltaworxCentral_Test.dbo.[usp_OptimizationCustomersGet]"
                    )
                    # Optional: Define any parameters to pass to the stored procedure
                    # RevCustomerIds = '6B32A900-5E87-4BBD-BD53-28BD86FD6192,
                    # 94F8BE71-ACCC-47EB-A0DF-78D3AAF093FA
                    # ,95599FFC-6F67-40AC-8518-D180B929C430,45519C0F-BFC4-4CB4-8468-F4420E9AE0CC'
                    # ServiceProviderId = 1
                    # TenantId = 1
                    # # SiteType = 1
                    # # AMOPCustomerIds = ''
                    # BillingPeriodId = 412
                    # Execute the stored procedure
                    cursor.callproc(
                        stored_procedure_name,
                        (ServiceProviderId, TenantId, BillingPeriodId),
                    )

                    # Fetch results if the stored procedure returns any
                    results = cursor.fetchall()
                    for row in results:
                        if row[1] == customer_name:
                            sim_cards_to_optimize = row[-1]
                            break
                    return sim_cards_to_optimize

        except Exception as e:
            logging.exception("Error in connection or execution:", e)
    else:
        # Create a connection to the database
        try:
            with pytds.connect(
                server=server, database=database, user=username, password=password
            ) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    stored_procedure_name = (
                        "AltaworxCentral_Test.dbo.[usp_OptimizationCustomersGet]"
                    )
                    # Optional: Define any parameters to pass to the stored procedure
                    # RevCustomerIds = '6B32A900-5E87-4BBD-BD53-28BD86FD6192
                    # ,94F8BE71-ACCC-47EB-A0DF-78D3AAF093FA,95599FFC-6F67-40AC-8518-D180B929C430,
                    # 45519C0F-BFC4-4CB4-8468-F4420E9AE0CC'
                    # ServiceProviderId = 1
                    # TenantId = 1
                    # # SiteType = 1
                    # # AMOPCustomerIds = ''
                    # BillingPeriodId = 412
                    # Execute the stored procedure
                    cursor.callproc(
                        stored_procedure_name,
                        (ServiceProviderId, TenantId, BillingPeriodId),
                    )
                    # sim_cards_to_optimize = 0
                    # Fetch results if the stored procedure returns any
                    results = cursor.fetchall()
                    for row in results:
                        last_item = row[-1]
                        sim_cards_to_optimize += last_item

                    return sim_cards_to_optimize

        except Exception as e:
            logging.exception("Error in connection or execution:", e)


def get_customer_total_sim_cards_count(rev_customer_id, database):
    """
    The get_customer_total_sim_cards_count function calculates the total number
    of SIM cards associated with a specific rev_customer_id. It retrieves the
    related customer IDs from the customers table and then counts the
    corresponding SIM cards in the sim_management_inventory table.
    The function uses dynamic SQL queries and handles exceptions by
    logging errors if any issues arise during execution.
    """
    logging.info(f"Request Data Recieved")
    try:
        # database = DB('altaworx_central', **db_config)
        rev_customer_ids = database.get_data(
            "revcustomer", {"rev_customer_id": rev_customer_id}, ["id"]
        )["id"].to_list()
        customer_ids_query = f"""
                SELECT id FROM customers
                WHERE rev_customer_id IN ({', '.join(f"'{id}'" for id in rev_customer_ids)})
                """

        # Step 4: Execute the query to get the customer IDs
        customer_ids_dataframe = database.execute_query(customer_ids_query, True)
        # Step 5: Extract customer IDs from the DataFrame
        customer_ids = customer_ids_dataframe["id"].tolist()

        # Step 6: Query to count records in sim_management_inventory based on customer IDs
        if customer_ids:  # Check if there are any customer IDs
            customer_ids_tuple = ", ".join(
                map(str, customer_ids)
            )  # Convert list to tuple string for SQL
            count_query = f"""
            SELECT count(*) FROM public.sim_management_inventory
            WHERE customer_id IN ({customer_ids_tuple}) and is_active=True
            """

            # Step 7: Execute the count query
            final_count = database.execute_query(count_query, True)
            count_value = final_count["count"][
                0
            ]  # Assuming the result is returned as a DataFrame
        return count_value
    except Exception as e:
        logging.exception(f"Exception is {e}")


##second point
def get_optimization_row_details(data):
    """
    The get_optimization_row_details function retrieves and paginates
    optimization data for a given session, fetching high-usage customer
    details and/or error-related records based on the fetch_type.
    It dynamically constructs queries, supports pagination, and handles
    errors gracefully, returning structured responses with total counts and data.
    """
    session_id = data.get("session_id")
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    # Pagination parameters
    start = data.get("start", 0)
    end = data.get("end", 10)  # Default to 10 if no end parameter provided
    limit=10
    params = [start, limit]
    pages = {"start": start, "end": end}
    total_count = 0
    error_details_count = 0
    high_usage_customers_data = []
    error_details = []

    try:
        count_params = [session_id]

        # Check if we're fetching high usage customers, error details, or both
        fetch_type = data.get("fetch_type", "both")  # Default to 'both'

        if fetch_type == "both" or fetch_type == "high_usage":
            # Count query to get total count of high usage customers
            count_query = f"""
                SELECT COUNT(DISTINCT customer_name)
                FROM public.vw_optimization_instance_summary
                WHERE  session_id = '{session_id}'
            """

            total_count = database.execute_query(count_query, count_params).iloc[0, 0]

            # Query for high usage customers
            query = f"""
                SELECT DISTINCT customer_name,
                               device_count,
                               total_overage_charge_amt + total_rate_charge_amt AS total_charges,
                               total_charge_amount
                FROM public.vw_optimization_instance_summary
                WHERE  session_id = '{session_id}'
                ORDER BY total_charge_amount DESC, device_count DESC
                OFFSET %s LIMIT %s
            """

            high_usage_customers_data = database.execute_query(
                query, params=params
            ).to_dict(orient="records")

            # Add total count to the pages dictionary for pagination
            pages["high_usage_total"] = int(total_count)

        if fetch_type == "both" or fetch_type == "error_details":
            # Count query to get total count of error details
            error_details_count_query = f"""
                select count(*) from vw_optimization_error_details where session_id='{session_id}'

            """

            error_details_count = database.execute_query(
                error_details_count_query, True
            ).iloc[0, 0]

            # Error details query
            error_details_query = f"""
                select rev_customer_id,iccid,msisdn,customer_name,
                error_message from vw_optimization_error_details where session_id='{session_id}'
                offset %s limit %s
            """

            error_details = database.execute_query(
                error_details_query, params=params
            ).to_dict(orient="records")

            # Add total count to the pages dictionary for pagination
            pages["error_details_total"] = int(error_details_count)

        # Build response
        response = {"flag": True, "pages": pages}

        if fetch_type == "both" or fetch_type == "high_usage":
            response["high_usage_customers_data"] = high_usage_customers_data
        if fetch_type == "both" or fetch_type == "error_details":
            response["error_details"] = error_details

        return response

    except Exception as e:
        # Handle any exceptions and provide default empty data
        response = {
            "flag": False,
            "high_usage_customers_data": [],
            "error_details": [],
            "pages": pages,
            "error_message": str(e),
        }
        return response

# Helper function to generate CSV file for each query
def generate_report_to_csv(report_name, query,database):
    """Function to execute query and generate CSV data"""
    df = database.execute_query(query, True)
    df.columns = [col.replace("_", " ").capitalize() for col in df.columns]

    # Check if the dataframe is empty
    if df.empty:
        df = pd.DataFrame(columns=df.columns)

    # Convert dataframe to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return (report_name, csv_buffer.getvalue())


def add_report_to_zip(report_name, query, database, zip_buffer):
    """
    Helper function to generate a report and add it to a zip buffer.
    """
    report_name, csv_data = generate_report_to_csv(report_name, query, database)
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zip_file:
        formatted_report_name = " ".join(
            word.capitalize() for word in report_name.replace("_", " ").split()
        )
        zip_file.writestr(f"{formatted_report_name}.csv", csv_data)

# Optimization details and push charges reports
def get_optimization_details_reports_data(data):
    """
    The get_optimization_details_reports_data function generates various optimization-related
    reports (e.g., client list, device management, charge breakdown) based on a specified report
    type and session ID. It executes queries for each report type, generates corresponding CSV
    files, zips them, and uploads the zip file to an S3 bucket. Upon successful upload,
    a download URL is returned, or an error message is logged if any issues occur.
    """
    report_type = data.get("report_type")
    session_id = data.get("session_id")
    optimization_type = data.get("optimization_type")
    tenant_database = data.get("db_name", "")
    try:
        # Database Connection
        database = DB(tenant_database, **db_config)
        db = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    except Exception as db_exception:
        logging.error("Failed to connect to the database: %s", str(db_exception))
        return {"error": "Database connection failed.", "details": str(db_exception)}
    # Define the queries and their respective report types
    # Retrieve the queries dictionary from the database
    module_query_df = db.get_data("export_queries", {"module_name": optimization_type})
    if module_query_df.empty:
        raise ValueError(f"No query found for module name: {optimization_type}")

    # Extract and parse the JSON string back into a dictionary
    queries = json.loads(module_query_df.iloc[0]["module_query"])
    # Initialize a zip buffer to store all CSVs
    zip_buffer = io.BytesIO()
    # Process reports sequentially
    try:
        if report_type == "All Reports":
            for report_name, query in queries.items():
                add_report_to_zip(report_name, query, database, zip_buffer)
        elif report_type in queries:
            add_report_to_zip(report_name, query, database, zip_buffer)
        # Now upload the zip buffer to S3
        file_name = "optimization_module_uat/Optimization.zip"
        zip_buffer.seek(0)  # Reset the buffer to the beginning
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=zip_buffer.getvalue(),
            ContentType="application/zip",
        )

        # Generate the download URL
        download_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{file_name}"
        db.update_dict(
            "export_status",
            {"status_flag": "Success", "url": download_url},
            {"module_name": optimization_type},
        )
        # Return response with the download URL
        response = {
            "flag": True,
            "download_url": download_url,  # Return the URL where the file is stored in S3
        }

        logging.debug(f"File successfully uploaded to S3")
        return response

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": False,
            "download_url": "",  # Return empty download URL in case of error
        }
        db.update_dict(
            "export_status",
            {"status_flag": "Failure"},
            {"module_name": optimization_type},
        )
        return response


##third point
def get_optimization_push_charges_data(data):
    """
    The get_optimization_push_charges_data function retrieves data for push charges
    based on the provided session ID and pagination parameters (start, end, limit).
    It first counts the total number of rows that match the session ID and then fetches
    the corresponding push charges data. If data exists, it returns the data in a
    paginated format; otherwise, it returns a message indicating no data was found.
    The function also handles errors gracefully by logging exceptions and returning an
    error response with a message.
    """
    logging.info("Request Recieved")
    # Record the start time for performance measurement
    start_time = time.time()
    session_id = data.get("sessionID")
    tenant_database = data.get("db_name", "")
    username = data.get("username", "")
    module_name = data.get("module_name", "")
    partner = data.get("Partner", "")
    request_received_at = data.get("request_received_at", "")
    try:
        # database Connection
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    except Exception as db_exception:
        logging.error("Failed to connect to the database: %s", str(db_exception))
        return {"error": "Database connection failed.", "details": str(db_exception)}
    start = data.get("start", 0)
    end = data.get("end", 10)  # Default to 10 if no 'end' parameter provided
    limit = 10
    pages = {"start": start, "end": end}
    total_count_result = 0
    try:
        # Query to get the total count of rows
        error_details_count_query = """
           SELECT count(*) FROM public.vw_optimization_push_charges_data where session_id = %s
        """
        total_count_result = database.execute_query(
            error_details_count_query, params=[session_id]
        ).iloc[0, 0]
        pages["total_count"] = int(total_count_result)

        # Query to get the data for push charges
        push_charges_dataquery = """
            SELECT iccid,service_provider_id,service_provider,rev_customer_id,
            customer_name,msisdn,run_status,instance_id,total_charge_amount
            FROM public.vw_optimization_push_charges_data
            where session_id=%s
            OFFSET %s LIMIT %s
        """

        push_charges_data = database.execute_query(
            push_charges_dataquery, params=[session_id, start, limit]
        )
        # push_charges_data = push_charges_data.astype(str)
        # Check if data is empty
        if push_charges_data.empty:
            # If no data, create an empty list with column names as keys
            columns = [
                "rev_customer_id",
                "customer_name",
                "iccid",
                "msisdn",
                "run_status",
                "instance_id",
                "error_message",
                "total_charge_amount",
            ]
            # Return an empty list of data with the column names in the response
            push_charges_data = []
            response = {
                "flag": True,
                "message": "No data found for the given parameters",
                "pages": pages,
                "push_charges_data": push_charges_data,
                "columns": columns,
            }
        else:
            # If data exists, convert to list of dictionaries
            push_charges_data = push_charges_data.to_dict(orient="records")

            response = {
                "flag": True,
                "pages": pages,
                "push_charges_data": push_charges_data,
            }
            try:
                # End time and audit logging
                end_time = time.time()
                time_consumed = f"{end_time - start_time:.4f}"
                time_consumed = int(float(time_consumed))
                audit_data_user_actions = {
                    "service_name": "Charges history",
                    "created_date": request_received_at,
                    "created_by": username,
                    "status": str(response["flag"]),
                    "time_consumed_secs": time_consumed,
                    "session_id": session_id,
                    "tenant_name": partner,
                    "comments": "Charges history data",
                    "module_name": module_name,
                    "request_received_at": request_received_at,
                }
                common_utils_database.update_audit(
                    audit_data_user_actions, "audit_user_actions"
                )
            except Exception as e:
                logging.warning(f"Got Exception while updating the Audit")

        # Ensure everything is JSON serializable
        return response

    except Exception as e:
        # Log the exception and return a failure response
        logging.exception(f"Exception occurred: {e}")
        message = f"Unable to fetch the push charges data{e}"
        response = {"flag": True, "message": message}
        error_type = str(type(e).__name__)
        error_data = {
            "service_name": "Charges history",
            "created_date": request_received_at,
            "error_message": message,
            "error_type": error_type,
            "users": username,
            "session_id": session_id,
            "tenant_name": partner,
            "comments": "Charges history data",
            "module_name": module_name,
            "request_received_at": request_received_at,
        }
        common_utils_database.log_error_to_db(error_data, "error_log_table")
        return response


##update button for rate plans and other updates
def update_optimization_actions_data(data):
    """
    The update_optimization_actions_data function updates the rate plans and
    other relevant data for a specific ICCID in the sim_management_inventory table.
    It takes the changed_data (the data to be updated) and the iccid
    (identifier for the SIM) from the request.
    The function attempts to update the record and returns a success message
    if the update is successful. If an exception occurs during the update process,
    it logs the exception and continues.
    """
    logging.info("Request Recieved")
    # Database connection
    # Extract database name and validate
    tenant_database = data.get("db_name", "")
    if not tenant_database:
        logging.warning("Database name is missing in the request data")
    try:
        # Establishing the database connection
        database = DB(tenant_database, **db_config)
        logging.info(f"Connected to the database")
    except Exception as db_exception:
        logging.error(f"Failed to connect to the database")
        return {"flag": False, "message": "Database connection failed."}
    changed_data = data.get("changed_data", "")
    iccid = str(data.get("iccid"))
    logging.debug(f"Retrieved th")
    try:
        database.update_dict("sim_management_inventory", changed_data, {"iccid": iccid})
        return {"flag": True, "message": "Updated Successfully"}
    except Exception as e:
        logging.exception(f"Exception is {e}")
        return {"flag": False, "message": "Failed to Update!!!"}


# Function to fetch both service providers and their rate plans
def get_assign_rate_plan_optimization_dropdown_data(data):
    """
    The get_assign_rate_plan_optimization_dropdown_data function fetches
    the rate plans for a specified service provider, based on the
    optimization type (either "Customer" or "Carrier)
    """
    service_provider = data.get("service_provider")
    tenant_database = data.get("db_name", "")
    optimization_type = data.get("optimization_type", "Customer")
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        # based on the optimization type the rate plans will be fetched
        if optimization_type == "Customer":
            rate_plan_list_data = database.get_data(
                "customerrateplan",
                {"service_provider_name": service_provider, "is_active": True},
                ["rate_plan_name"],  # Rate plan column
            )["rate_plan_name"].to_list()
        else:
            rate_plan_list_data = database.get_data(
                "carrier_rate_plan",
                {"service_provider": service_provider, "is_active": True},
                ["rate_plan_code"],  # Rate plan column
            )["rate_plan_code"].to_list()

        response = {
            "flag": True,
            "message": "assign rate plan data fetched successfully",
            "rate_plan_list_data": rate_plan_list_data,
        }
        # Return the data as a JSON response,
        return response

    except Exception as e:
        logging.exception(f"Exception is {e}")
        response = {
            "flag": True,
            "message": "unable to fetch the assign rate plan data",
            "rate_plan_list_data": [],
        }
        # Return the data as a JSON response,
        return response


def charges_upload_status_mail(
    common_utils_database, username, template_name, database, session_id
):
    """
    The charges_upload_status_mail function is responsible for sending an email notification
    regarding the status of a charges upload process. It queries the database for charge
    details based on a specific session ID and attaches the results as an Excel file.
    Depending on the upload status, it sends either a success or failure email,
    while also updating the email audit with relevant details such as the email's subject,
    recipient, and status.
    """
    if template_name == "Charges Upload Completed":
        query = f"""
            SELECT
                customer_name,session_id,
                msisdn,
                charge_id,
                charge_amount,
                sms_charge_amount,
                billing_period_end_date,
                billing_period_start_date, error_message
            FROM vw_optimization_smi_result_customer_charge_queue
            where session_id='{session_id}'
        """
        result_df = database.execute_query(query, True)
        # Generate an Excel file from the result DataFrame
        blob_data = dataframe_to_blob(result_df)
        # Prepare the email content
        subject = f"Charges Uploaded Successfully"

        # Send the email with Excel file attached
        result = send_email(
            template_name=template_name,
            username=username,
            subject=subject,
            attachments=blob_data,  # Pass the blob_data as attachment
        )
    else:
        query = f"""
            SELECT
                customer_name,session_id,
                msisdn,
                charge_id,
                charge_amount,
                sms_charge_amount,
                billing_period_end_date,
                billing_period_start_date, error_message
            FROM vw_optimization_smi_result_customer_charge_queue
            where session_id='{session_id}'
        """
        result_df = database.execute_query(query, True)
        # Generate an Excel file from the result DataFrame
        blob_data = dataframe_to_blob(result_df)
        # Prepare the email content
        subject = f"Charges Uploaded Failed!!"
        # Send the email with Excel file attached
        result = send_email(
            template_name=template_name,
            username=username,
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
            logging.exception(f"Failed to send email: {email_audit_data}")
        else:
            to_emails, cc_emails, subject, from_email, body, partner_name = result

            query = """
                        SELECT parents_module_name, sub_module_name, child_module_name,
                          partner_name
                        FROM email_templates
                        WHERE template_name = %s
                    """

            params = [template_name]
            # Execute the query with template_name as the parameter
            email_template_data = common_utils_database.execute_query(
                query, params=params
            )
            parents_module_name, sub_module_name, child_module_name, partner_name = (
                email_template_data[0]
            )

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
    return {"flag": True}


##submit button push charges screen
def push_charges_submit(data):
    """
    The push_charges_submit function handles the submission of customer charges
    for a specific session. It builds a POST request payload with session details
    (SessionId, selectedInstances, selectedUsageInstances, and pushType) and sends
    it to the configured API endpoint for processing. The function dynamically retrieves
    the tenant_id and uses it in the headers. It returns the API response,
    including status code and message, while logging errors in case of failure.
    """
    logging.info("Request Recieved")
    # Record the start time for performance measurement
    start_time = time.time()
    username = data.get("username", "")
    partner = data.get("tenant_name", "")
    session_id = data.get("SessionId", "")
    request_received_at = data.get("request_received_at", "")
    module_name = data.get("module_name", "")
    selected_instances = data.get(
        "selectedInstances", ""
    )  # Placeholder, needs to be discussed
    selected_usage_instances = data.get(
        "selectedUsageInstances", ""
    )  # Placeholder, needs to be discussed
    push_type = data.get("pushType", "")
    # Connect to the database to get tenant_id
    tenant_database = data.get("db_name", "")
    try:
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    except Exception as db_exception:
        logging.error("Failed to connect to the database: %s", str(db_exception))
        return {"error": "Database connection failed.", "details": str(db_exception)}
    try:
        username = "amop-core"
        password = "uDLD4AK3vOnqAjpn2DVRhwlrcbTLB6EY"
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode(
            "utf-8"
        )
        authorization_header = f"Basic {encoded_credentials}"
        # database Connection
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": "Altaworx"}, ["id"]
        )["id"].to_list()[0]
        # Define the URL for customer charges upload
        url = os.getenv("OPTIMIZATIONAPI_CREATECOMFIRM_SESSION", "")
        # Define the headers
        # Define the headers
        headers = {
            "Authorization": authorization_header,
            "user-name": "lohitha.v@algonox.com",
            "x-tenant-id": tenant_id,
            "Content-Type": "application/json",  # Specify content type for JSON
        }

        # Create the request body dynamically
        body = {
            "SessionId": session_id,
            "selectedInstances": selected_instances,
            "selectedUsageInstances": selected_usage_instances,
            "pushType": push_type,
        }

        # Send the POST request
        response = requests.post(url, headers=headers, data=json.dumps(body))

        # Prepare the response data
        response_data = {
            "flag": True,
            "status code": response.status_code,
            "message": response.json() if response.content else "No Content",
        }
        try:
            # End time and audit logging
            end_time = time.time()
            time_consumed = f"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {
                "service_name": "Charges history",
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response_data["flag"]),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": partner,
                "comments": "Push charges submit button",
                "module_name": module_name,
                "request_received_at": request_received_at,
            }
            common_utils_database.update_audit(
                audit_data_user_actions, "audit_user_actions"
            )
        except Exception as e:
            logging.warning(f"Got Exception while updating the Audit")
        try:
            charges_upload_status_mail(
                common_utils_database,
                username,
                "Charges Upload Completed",
                database,
                session_id,
            )
        except Exception as e:
            logging.exception(f"Failed to send the mail :{e}")

        # Return the response
        return response_data

    except Exception as e:
        logging.exception(f"Error uploading customer charges data: {e}")
        try:
            charges_upload_status_mail(
                common_utils_database,
                username,
                "Charges Upload Failure",
                database,
                session_id,
            )
        except Exception as e:
            logging.exception(f"Failed to send the mail :{e}")
        response={"flag": False, "error": str(e)}
        message = f"Unable to upload the Charges"
        response = {"flag": True, "message": message}
        error_type = str(type(e).__name__)
        error_data = {
            "service_name": "Charges history",
            "created_date": request_received_at,
            "error_message": message,
            "error_type": error_type,
            "users": username,
            "session_id": session_id,
            "tenant_name": partner,
            "comments": "Charges Upload Failed",
            "module_name": module_name,
            "request_received_at": request_received_at,
        }
        common_utils_database.log_error_to_db(error_data, "error_log_table")
        return response

def update_push_charges_data_optimization(data):
    logging.info("Request Received: update_push_charges_data_optimization function called")
    tenant_name = data.get("tenant_name", "Altaworx")
    try:
        database = DB(tenant_name, **db_config)
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    except Exception as e:
        logging.exception(f"Exception while connecting to database")
        return {"flag":False,"message":"Failed to connnect to database"}
    try:
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": tenant_name}, ["id"]
        )["id"].to_list()[0]
        submit_data = data.get("changed_data", "")
        submit_data["tenant_id"] = tenant_id
        bulkchangeid = database.insert_data(submit_data, "sim_management_bulk_change")
        response = {"flag": True, "message": "Push charges data updated Successfully!!"}
        return response
    except Exception as e:
        logging.exception(f"Exception is {e}")
        response = {"flag": False, "message": "Push charges data updation failed!!"}
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


def get_export_status(data):
    try:
        common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
        module_name = data.get("module_name")
        export_status_query = (
            f"select * from export_status where module_name='{module_name}'"
        )
        export_status_data = common_utils_database.execute_query(
            export_status_query, True
        ).to_dict(orient="records")
        return {"flag": True, "export_status_data": export_status_data[0]}
    except Exception as e:
        logging.exception(f"Exception is {e}")
        return {"flag": False, "export_status_data": []}


def get_optimization_progress_bar_data(data):
    logging.info(f"optimization progress bar function started{data}")
    job_name = data.get("job_name", "")
    optimization_session_id = data.get("SessionId", "")
    progress = data.get("Progress", "")
    error_message = data.get("ErrorMessage", "")
    customer_id = data.get("CustomerId", "")
    session_id = data.get("OptimizationSessionGuid")

    # Database Connection
    tenant_database = data.get("db_name", "")
    database = DB('altaworx_test', **db_config)

    updated_data = {
        "optimization_session_id": optimization_session_id,
        "progress": progress,
        "error_message": error_message,
        "customer_id": customer_id,
        "session_id": session_id
    }

    try:
        if job_name == "ErrorMessage":
            insert_id = database.insert_data(updated_data, "optimization_details")
            response = {"flag": True, "message": "Call Successful"}
            return response
        else:

            # Check if details_id exists
            details_data = database.get_data("optimization_details",
                                             {"optimization_session_id": optimization_session_id},
                                             ["optimization_session_id"])
            if progress != 100:
                if not details_data.empty:
                    details_id = details_data["optimization_session_id"].to_list()[0]
                    updated_data.pop('optimization_session_id')  # Remove the key before updating
                    database.update_dict("optimization_details", updated_data, {"optimization_session_id": details_id})
                    optimization_session_update_data = {"progress": progress}
                    database.update_dict("optimization_session", optimization_session_update_data, {"session_id": session_id})
                else:
                    details_id = None

                    insert_id = database.insert_data(updated_data, "optimization_details")

                    # Check if 'AdditionalJson' exists and parse it safely
                    if 'AdditionalJson' in data and data['AdditionalJson'] is not None:
                        try:
                            additional_json_data = json.loads(data['AdditionalJson'])

                            if 'data' in additional_json_data and isinstance(additional_json_data['data'], dict):
                                service_provider_id = additional_json_data['data'].get('ServiceProviderId')
                                optimization_type = additional_json_data['data'].get('OptimizationType')

                                if service_provider_id is None or optimization_type is None:
                                    logging.warning("ServiceProviderId or OptimizationType is missing in AdditionalJson")
                                else:
                                    if optimization_type == 'Carrier':
                                        optimization_type_id = 0
                                    else:
                                        optimization_type_id = 1

                                    # Prepare data for optimization session
                                    optimization_session_data = {
                                        "session_id": session_id,
                                        "is_active": True,
                                        "optimization_type_id": optimization_type_id,
                                        "service_provider_id": service_provider_id,
                                        "is_deleted": False,
                                        "progress": progress,
                                        "optimization_run_start_date": datetime.now()  # Current timestamp
                                    }

                                    optimization_id = database.insert_data(optimization_session_data, "optimization_session")
                                    optimization_instance_data = {
                                        "run_status_id": 6,
                                        "service_provider_id": service_provider_id,
                                        "optimization_session_id": optimization_id,
                                        "is_deleted": False
                                    }
                                    optimization_instance_id = database.insert_data(optimization_instance_data, "optimization_instance")
                            else:
                                logging.warning("The 'data' subfield within 'AdditionalJson' is not found or is not a dictionary.")
                        except (json.JSONDecodeError, TypeError) as e:
                            logging.warning(f"Failed to parse 'AdditionalJson': {e}")
                    else:
                        logging.warning("The 'AdditionalJson' field is not found or is None.")
            # Check if progress is 100, if so, hit the API
            else:
                # Prepare the data to send in the API request to sync the 1.0 data
                api_url = "https://demo-api.amop.services/daily_migration_after_lambdas"
                api_payload = {
                        "data": {
                            "data": {
                                "path":"/lambda_sync_jobs",
                            "key_name": "optimization_sync",
                            "session_id":session_id
                            }
                        }
                        }

                # Send the POST request
                try:
                    response = requests.post(api_url, json=api_payload)

                    if response.status_code == 200:
                        logging.info("API call successful.Data ")
                    else:
                        logging.error(f"API call failed with status code: {response.status_code}")
                except Exception as e:
                    logging.error(f"Error occurred while calling the API: {e}")

            response = {"flag": True, "message": "Call Successful"}
            return response
    except Exception as e:
        logging.exception(f"Exception is {e}")
        response = {"flag": False, "message": f"Call Failed {e}"}
        return response





def get_optimization_error_details_data(data):
    try:
        response = {"flag": True, "message": "Call Successfull"}
        return response
    except Exception as e:
        logging.exception(f"Exception is {e}")
        response = {"flag": False, "message": "Call Failed"}
        return response




# Function to get optimization sessions by tenant ID
def get_optimization_sessions_by_tenant_id(tenant_id, optimization_type, filter_str):
    # Define your connection parameters
    server = os.getenv("ONEPOINTOSERVER", " ")
    database= os.getenv("ONEPOINTODATABASE", " ")
    username = os.getenv("ONEPOINTOUSERNAME", " ")
    password = os.getenv("ONEPOINTOPASSWORD", " ")
    query = ""
    if optimization_type == "All":
        query = f"""
            SELECT *
            FROM vwOptimizationSession
            WHERE TenantId = {tenant_id}
            AND IsActive = 1
            AND IsDeleted = 0
            ORDER BY CreatedDate DESC
        """
    else:
        query = f"""
            SELECT *
            FROM vwOptimizationSession
            WHERE TenantId = {tenant_id}
            AND OptimizationTypeId = {int(optimization_type)}
            AND IsActive = 1
            AND IsDeleted = 0
            ORDER BY CreatedDate DESC
        """

    # Connect to the database and execute the query
    with pytds.connect(server=server, database=database, user=username, password=password) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        # Apply filter if provided
        if filter_str:
            filter_str = filter_str.lower()
            rows = [row for row in rows if
                    (row.service_provider and filter_str in row.service_provider.lower()) or
                    (row.billing_period_end_date and filter_str in str(row.billing_period_end_date))]

        return rows

# Function to get running optimization sessions
def get_optimization_running():
    # Define your connection parameters
    server = os.getenv("ONEPOINTOSERVER", " ")
    database= os.getenv("ONEPOINTODATABASE", " ")
    username = os.getenv("ONEPOINTOUSERNAME", " ")
    password = os.getenv("ONEPOINTOPASSWORD", " ")
    query = """
        SELECT *
        FROM vwOptimizationSessionRunning
        ORDER BY OptimizationSessionId DESC
    """

    with pytds.connect(server=server, database=database, user=username, password=password) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

# Function to check if optimization is running
def check_optimization_is_running(tenant_id):
    # Get the list of optimization sessions
    optimization_session_list = get_optimization_sessions_by_tenant_id(tenant_id, "All", "")
    if not optimization_session_list:
        return False

    # Get the list of running optimization sessions
    optimization_session_running_list = get_optimization_running()
    if not optimization_session_running_list:
        return False

    # Assuming the first column in the tuple from optimization_session_list is the 'id'
    optimization_session_id = optimization_session_list[0][0]  # Access the 'id' (first element of the tuple)

    # Check if the session is running using dictionary keys for the optimization_session_running_list
    opt_running = [x for x in optimization_session_running_list if x['OptimizationSessionId'] == optimization_session_id]

    if opt_running:
        # Assuming '1' is 'COMPLETE_WITH_ERROR' for OptimizationQueueStatusId and OptimizationInstanceStatusId
        return not any(x['OptimizationQueueStatusId'] == 1 or x['OptimizationInstanceStatusId'] == 1 for x in opt_running)

    return False

# Main function to check optimize button status
def optimize_button_status(data):
    # Database connection parameters
    try:
        tenant_id = data.get('tenant_id', 1)

        # Get the status of the optimization button
        optimize_button = check_optimization_is_running(tenant_id)
        return {"flag":True,"optimize_button_status":optimize_button}
    except Exception as e:
        logging.exception(f"Exception occured while getting status for optimization button{e}")
        return {"flag":True,"optimize_button_status":False}